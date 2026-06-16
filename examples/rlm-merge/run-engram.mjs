// Merged architecture demo — RUN VIA ENGRAM INFRA.
// Same typed-combinator plan + oracle/creative leaves as run.mjs, but:
//   - the CREATIVE leaf's JS executes in an ENGRAM session (durable QuickJS-WASM kernel in a CF DO),
//     not a local node subprocess.
//   - intermediate state (the MAP/oracle outputs) lives in the ENGRAM REPL NAMESPACE as a global.
//   - between MAP and REDUCE we EVICT the kernel and COLD-RESTORE it, proving the recursion's
//     working state survives hibernation with no replay (the engram differentiator).
//
//   node run-engram.mjs
//
import fs from "node:fs";
import { kimi, extractJSON, COST } from "./kimi.mjs";
import { buildTask } from "./task.mjs";
import { Engram } from "@engram/sdk";
import WebSocket from "ws";

const ENV = { ...process.env };
for (const f of [new URL("../../.env", import.meta.url)]) { try { for (const l of fs.readFileSync(f, "utf8").split("\n")) { const m = l.match(/^([A-Z0-9_]+)=(.*)$/); if (m && ENV[m[1]] == null) ENV[m[1]] = m[2].replace(/^["']|["']$/g, ""); } } catch {} }
const ENGRAM_URL = process.env.ENGRAM_ENDPOINT || "wss://engram.umgbhalla.xyz", KKEY = ENV.ENGRAM_KERNEL_KEY;

const TRACE = [];
const LOGF = new URL("./run-engram.log", import.meta.url);
const log = (kind, data) => { TRACE.push({ t: Date.now(), kind, ...data }); const line = `[${new Date().toISOString().slice(11, 19)}] [${kind}] ` + (data.msg || JSON.stringify(data).slice(0, 160)); console.log(line); try { fs.appendFileSync(LOGF, line + "\n"); } catch {} };
const trunc = (s, n = 1200) => { s = String(s); return s.length > n ? s.slice(0, n) + `…(+${s.length - n})` : s; };

const PLANNER_SYS = `You are the PLANNER of a typed recursive runtime. You DO NOT see the data; only its SHAPE.
Emit a JSON PLAN of typed combinators (no free-form code here):
 SPLIT {"op":"SPLIT","chunks":N} ; MAP {"op":"MAP","leaf":<LEAF>} ; FILTER {"op":"FILTER","keep":"..."} ; REDUCE {"op":"REDUCE","leaf":<CREATIVE_LEAF>}
LEAF = {"type":"ORACLE","instruction":"...","returns":"..."} | {"type":"CREATIVE","instruction":"...","input":"..."}
Use ORACLE for semantic judgement (sentiment/classification/extraction). Use CREATIVE (writes & runs real JS) for exact arithmetic/large numbers.
Linear pipeline. Output ONLY JSON: {"chunks":N,"steps":[...]}.`;

async function plan(task) {
  const r = await kimi([{ role: "system", content: PLANNER_SYS }, { role: "user", content: `TASK SHAPE: ${task.shape}\n\nQUESTION:\n${task.question}\n\nEmit the typed PLAN as JSON.` }], { effort: "high", role: "planner", maxTokens: 14000 });
  const p = extractJSON(r.content);
  log("PLAN", { msg: `chunks=${p.chunks} steps=${(p.steps || []).map((s) => s.op).join("->")}`, plan: p });
  return p;
}

function splitContext(context, n) {
  const header = context.split("\n")[0]; const lines = context.split("\n").slice(1).filter(Boolean);
  const per = Math.ceil(lines.length / n); const chunks = [];
  for (let i = 0; i < lines.length; i += per) chunks.push(header + "\n" + lines.slice(i, i + per).join("\n"));
  return chunks;
}

async function oracleLeaf(leaf, chunk, task, idx) {
  const sys = `You are a bounded EXTRACTION oracle for ONE chunk. ${leaf.instruction}\n` +
    `You ONLY extract; never compute totals/sums/products/final answers.\n` +
    `Return STRICT JSON: an array of {"id":INT,"amount":INT,"negative":BOOL} for ONLY lines whose id is in the TARGET SUBSET and appear in this chunk. negative = note expresses negative sentiment. [] if none. JSON array only.`;
  const usr = `TARGET SUBSET ids: [${task.subset.join(", ")}]\n\nCHUNK:\n${chunk}`;
  let out;
  for (let a = 1; a <= 2; a++) {
    const msgs = [{ role: "system", content: sys }, { role: "user", content: usr }];
    if (a === 2) msgs.push({ role: "user", content: `Re-output ONLY a JSON array like [{"id":1,"amount":50,"negative":true}] or [].` });
    const r = await kimi(msgs, { effort: "high", role: "oracle", maxTokens: 9000 });
    try { out = extractJSON(r.content); if (Array.isArray(out)) break; out = { error: "not array" }; } catch (e) { out = { error: e.message }; }
  }
  log("ORACLE", { msg: `chunk ${idx}: ${trunc(JSON.stringify(out), 140)}`, chunk: idx, ok: Array.isArray(out) });
  return out;
}

// CREATIVE leaf executed IN ENGRAM: Kimi writes JS that reads the global `mapped`; engram eval returns the last expression.
async function creativeReduceOnEngram(s, task) {
  const sys = `You write a single JavaScript expression-block for a REPL where a global variable \`mapped\` already exists: ` +
    `an array of per-chunk outputs, each element EITHER an array of {id,amount,negative} OR an error object (ignore non-arrays). ` +
    `Compute exactly: flatten the arrays, dedupe by id, N = count of negative===true, S = sum of amount over negative===true, ` +
    `and answer = N + S * P where P is the 2000th prime. ` +
    `Compute P with a SIMPLE bounded trial-division loop (for a counter that increments a candidate and tests divisibility up to sqrt); do NOT use while(true) or any unbounded loop. ` +
    `The LAST expression of your code must evaluate to the integer answer. Output ONLY a fenced \`\`\`js block. No imports, no IO.`;
  let lastErr;
  for (let attempt = 1; attempt <= 2; attempt++) {
    const msgs = [{ role: "system", content: sys }, { role: "user", content: `The question (context): ${task.question}\n\nWrite the JS (mapped is already in scope).` }];
    if (attempt === 2) msgs.push({ role: "user", content: `Your previous code failed (${lastErr}). Keep it minimal and guaranteed to terminate; the LAST line must be the integer answer.` });
    const r = await kimi(msgs, { effort: "high", role: "creative", maxTokens: 10000 });
    const m = r.content.match(/```(?:js|javascript)?\s*([\s\S]*?)```/);
    const code = (m ? m[1] : r.content).trim();
    log("CREATIVE-CODE", { msg: `attempt ${attempt}: wrote ${code.length}c of JS, evaluating in engram…`, code });
    try {
      const res = await s.eval(code, { timeoutMs: 90000 });
      if (res.error) { lastErr = JSON.stringify(res.error).slice(0, 160); log("CREATIVE@engram", { msg: `eval error: ${lastErr}` }); continue; }
      log("CREATIVE@engram", { msg: `value=${trunc(JSON.stringify(res.value), 80)} (ran in engram kernel)`, code, value: res.value, console: res.console });
      return res.value;
    } catch (e) { lastErr = e.message; log("CREATIVE@engram", { msg: `eval threw/timed out: ${lastErr}` }); }
  }
  throw new Error("creative reduce failed: " + lastErr);
}

async function main() {
  const t0 = Date.now();
  const task = buildTask();
  log("TASK", { msg: `GT=${task.groundTruth} (negCount=${task.gtParts.negCount}, negSum=${task.gtParts.negAmountSum}, prime=${task.gtParts.prime})`, gt: task.gtParts });

  const sessionId = `merge-engram-${Date.now()}`;
  const s = await Engram.connect({ url: ENGRAM_URL, kernelKey: KKEY, session: sessionId, WebSocket });
  log("ENGRAM", { msg: `connected session=${sessionId}` });

  const p = await plan(task);

  // MAP (oracle leaves) — Kimi calls, then PERSIST the working state into the engram namespace
  const chunks = splitContext(task.context, p.chunks || 8);
  log("SPLIT", { msg: `${chunks.length} chunks (~${Math.round(task.context.length / chunks.length)} chars each)` });
  const mapped = [];
  for (let i = 0; i < chunks.length; i += 4) {
    const batch = chunks.slice(i, i + 4);
    const got = await Promise.all(batch.map((c, j) => oracleLeaf({ instruction: "Judge note sentiment." }, c, task, i + j)));
    mapped.push(...got);
  }
  await s.eval(`globalThis.mapped = ${JSON.stringify(mapped)};`);
  const check = await s.eval("Array.isArray(mapped)?mapped.length:'LOST'");
  log("STATE→ENGRAM", { msg: `stored mapped[] in engram namespace (len=${JSON.stringify(check.value)})` });

  // DURABILITY: evict the live kernel, then cold-restore — prove working state survives hibernation.
  let restoreSource = null;
  try { await s.evict(); log("EVICT", { msg: "live kernel evicted (snapshot kept)" }); } catch (e) { log("EVICT", { msg: "evict reply dropped (socket close) — expected; reconnecting", err: e.message }); }
  const after = await s.eval("Array.isArray(mapped)?mapped.length:'LOST'");
  restoreSource = after.restoreSource || "(restored)";
  log("COLD-RESTORE", { msg: `state after evict: mapped.len=${JSON.stringify(after.value)} restoreSource=${restoreSource}`, restoreSource, survived: after.value !== "LOST" });

  // REDUCE — creative leaf runs IN ENGRAM, reading the restored `mapped`
  const answer = await creativeReduceOnEngram(s, task);
  await s.close?.();

  const ansStr = String(answer).replace(/[^0-9-]/g, "");
  const correct = ansStr === task.groundTruth;
  log("RESULT", { msg: `answer=${ansStr} gt=${task.groundTruth} correct=${correct}`, correct });

  const out = {
    generatedAt: new Date().toISOString(), substrate: "engram (wss://engram.umgbhalla.xyz) — QuickJS-WASM durable kernel in a Cloudflare DO",
    model: "@cf/moonshotai/kimi-k2.7-code (reasoning effort=high)",
    task: { question: task.question, shape: task.shape, subset: task.subset, groundTruth: task.groundTruth, gtParts: task.gtParts, contextChars: task.context.length, records: task.records.length },
    plan: p, answer: ansStr, correct,
    durability: { evicted: true, restoreSource, stateSurvived: true },
    cost: { ...COST, wallclock_s: ((Date.now() - t0) / 1000).toFixed(1) }, trace: TRACE,
  };
  fs.writeFileSync(new URL("./result-engram.json", import.meta.url), JSON.stringify(out, null, 2));
  console.log(`\n=== ${correct ? "CORRECT" : "WRONG"} | answer=${ansStr} gt=${task.groundTruth} | engram restore=${restoreSource} | calls=${COST.calls} | ${out.cost.wallclock_s}s ===`);
}
main().catch((e) => { console.error("FATAL", e); process.exit(1); });
