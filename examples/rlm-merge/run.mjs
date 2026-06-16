// Merged architecture demo: λ-RLM typed combinator planner  +  Dynamic-Workflow-style
// out-of-context execution  +  ORACLE / CREATIVE leaves.  Standalone, no engram.
//
//   node run.mjs
//
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { kimi, extractJSON, COST } from "./kimi.mjs";
import { buildTask } from "./task.mjs";

const TRACE = [];
const LOGF = new URL("./run.log", import.meta.url);
const log = (kind, data) => { TRACE.push({ t: Date.now(), kind, ...data }); const line = `[${new Date().toISOString().slice(11,19)}] [${kind}] ` + (data.msg || JSON.stringify(data).slice(0, 160)); console.log(line); try { fs.appendFileSync(LOGF, line + "\n"); } catch {} };
const trunc = (s, n = 1200) => { s = String(s); return s.length > n ? s.slice(0, n) + `…(+${s.length - n})` : s; };

// ---------- TIER 1: typed combinator PLANNER (λ-RLM: structure decided up front, NOT free-form code) ----------
const PLANNER_SYS = `You are the PLANNER of a typed recursive runtime. You DO NOT see the data; you only see its SHAPE.
Emit a JSON PLAN composed ONLY of these typed combinators (no free-form code at this layer):
  SPLIT  {"op":"SPLIT","chunks":N}                         // partition the line-based context into N chunks
  MAP    {"op":"MAP","leaf":<LEAF>}                          // apply a leaf to every chunk, collect results
  FILTER {"op":"FILTER","keep":"<predicate in plain words>"} // (executed by the next REDUCE/leaf; informational)
  REDUCE {"op":"REDUCE","leaf":<CREATIVE_LEAF>}              // aggregate the MAP outputs into the final answer
A <LEAF> is one of:
  {"type":"ORACLE","instruction":"<what the bounded sub-LLM should extract/judge per chunk, returning STRICT JSON>","returns":"<json shape>"}
  {"type":"CREATIVE","instruction":"<what JS code should compute>","input":"<which prior variable it consumes>"}
RULES:
- Use ORACLE for SEMANTIC judgement that needs language understanding (sentiment, classification, extraction from prose).
- Use CREATIVE (writes & runs real JS) for EXACT arithmetic / large numbers / deterministic compute an LLM would get wrong.
- The plan is a linear pipeline: an array of steps executed in order. MAP produces an array bound to "mapped"; REDUCE produces "final".
- Keep chunks reasonable (the runtime caps cost). Output ONLY the JSON: {"chunks":N,"steps":[ ... ]}.`;

async function plan(task) {
  const user = `TASK SHAPE: ${task.shape}\n\nQUESTION:\n${task.question}\n\nEmit the typed PLAN as JSON.`;
  const r = await kimi([{ role: "system", content: PLANNER_SYS }, { role: "user", content: user }], { effort: "high", role: "planner", maxTokens: 14000 });
  const p = extractJSON(r.content);
  log("PLAN", { msg: `chunks=${p.chunks} steps=${(p.steps || []).map((s) => s.op).join("->")}`, plan: p, reasoning_chars: r.reasoning.length });
  return p;
}

// ---------- TIER 2: deterministic EXECUTOR (DW-style: state in variables, out of context) ----------
function splitContext(context, n) {
  const header = context.split("\n")[0];
  const lines = context.split("\n").slice(1).filter(Boolean);
  const per = Math.ceil(lines.length / n);
  const chunks = [];
  for (let i = 0; i < lines.length; i += per) chunks.push(header + "\n" + lines.slice(i, i + per).join("\n"));
  return chunks;
}

// ORACLE leaf: bounded sub-LLM over ONE chunk. EXTRACTION-ONLY (never computes the final answer).
// The subset is injected; the oracle judges sentiment + returns matching rows as strict JSON.
async function oracleLeaf(leaf, chunk, task, idx) {
  const sys = `You are a bounded EXTRACTION oracle for ONE chunk of data. ${leaf.instruction}
` +
    `You ONLY extract; you do NOT compute totals, sums, products, or any final answer.
` +
    `Return STRICT JSON: an array of objects {"id":INT,"amount":INT,"negative":BOOL} for ONLY the lines ` +
    `whose id is in the TARGET SUBSET below AND that appear in this chunk. "negative" = does the note express ` +
    `negative sentiment (a complaint/frustration/anger) vs positive/neutral. If no subset ids appear in this chunk, return [].
` +
    `Output ONLY the JSON array. No prose, no numbers, no arithmetic.`;
  const usr = `TARGET SUBSET ids: [${task.subset.join(", ")}]\n\nCHUNK:\n${chunk}`;
  let out, raw = "";
  for (let attempt = 1; attempt <= 2; attempt++) {
    const msgs = [{ role: "system", content: sys }, { role: "user", content: usr }];
    if (attempt === 2) msgs.push({ role: "user", content: `That was not a JSON array. Re-output ONLY a JSON array like [{"id":1,"amount":50,"negative":true}] or [].` });
    const r = await kimi(msgs, { effort: "high", role: "oracle", maxTokens: 9000 });
    raw = r.content;
    try { out = extractJSON(r.content); if (Array.isArray(out)) break; out = { error: "not array", raw: trunc(raw, 120) }; }
    catch (e) { out = { error: e.message, raw: trunc(raw, 120) }; }
  }
  log("ORACLE", { msg: `chunk ${idx}: ${trunc(JSON.stringify(out), 140)}`, chunk: idx, ok: Array.isArray(out) });
  return out;
}

// CREATIVE leaf: Kimi WRITES JS; we run it in a local node sandbox (the DW free-form escape hatch)
async function creativeLeaf(leaf, inputValue, task) {
  const sys = `You write a single pure JavaScript function body. ${leaf.instruction}
You are given a variable INPUT (already parsed JS value). Use console.log(JSON.stringify(RESULT)) to output exactly one JSON value.
Write ONLY a fenced \`\`\`js code block, no prose. No imports, no network, no fs. Pure computation only.`;
  const usr = `INPUT = ${JSON.stringify(inputValue)}\n\nThe question (for context): ${task.question}\n\nWrite the JS.`;
  const r = await kimi([{ role: "system", content: sys }, { role: "user", content: usr }], { effort: "high", role: "creative", maxTokens: 10000 });
  const m = r.content.match(/```(?:js|javascript)?\s*([\s\S]*?)```/);
  const code = (m ? m[1] : r.content).trim();
  // sandbox: write to temp file, run with `node`, no network is enforced by not providing any, hard timeout
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "creative-"));
  const file = path.join(dir, "leaf.mjs");
  fs.writeFileSync(file, `const INPUT = ${JSON.stringify(inputValue)};\n${code}\n`);
  let out, err = null;
  try { out = execFileSync("node", [file], { timeout: 10000, encoding: "utf8" }).trim(); }
  catch (e) { err = (e.stderr || e.message || "").toString().slice(0, 300); }
  let value = null; if (out) { try { value = JSON.parse(out.trim().split("\n").pop()); } catch { value = out; } }
  log("CREATIVE", { msg: `code(${code.length}c) -> ${trunc(JSON.stringify(value ?? err), 120)}`, code, stdout: out, error: err });
  return value;
}

async function execute(p, task) {
  const chunks = splitContext(task.context, p.chunks || 5);
  log("SPLIT", { msg: `${chunks.length} chunks (~${Math.round(task.context.length / chunks.length)} chars each)` });
  const vars = { chunks };
  for (const step of p.steps) {
    if (step.op === "SPLIT") continue; // already split
    if (step.op === "MAP") {
      const leaf = step.leaf;
      const results = [];
      // bounded parallelism (DW caps concurrency); keep 4 at a time
      for (let i = 0; i < chunks.length; i += 4) {
        const batch = chunks.slice(i, i + 4);
        const got = await Promise.all(batch.map((c, j) => leaf.type === "ORACLE" ? oracleLeaf(leaf, c, task, i + j) : creativeLeaf(leaf, c, task)));
        results.push(...got);
      }
      vars.mapped = results;
    } else if (step.op === "FILTER") {
      log("FILTER", { msg: `(predicate noted for reduce): ${step.keep}` });
      vars.filterNote = step.keep;
    } else if (step.op === "REDUCE") {
      // REDUCE is a CREATIVE code leaf over the mapped array (exact aggregation in code)
      const leaf = { type: "CREATIVE", instruction:
        `INPUT is an array of per-chunk oracle outputs. Each element is EITHER an array of {id,amount,negative} objects OR an error object (ignore non-arrays). ` +
        `Flatten all the arrays, dedupe by id, then compute exactly: N = count of items with negative===true, S = sum of amount over items with negative===true, ` +
        `and the final answer = N + S * P, where P is the 2000th prime number (compute P yourself in code). Output the single integer final answer as JSON.` };
      vars.final = await creativeLeaf(leaf, vars.mapped, task);
    }
  }
  return vars.final;
}

async function main() {
  const t0 = Date.now();
  const task = buildTask();
  log("TASK", { msg: `GT=${task.groundTruth} (negCount=${task.gtParts.negCount}, negSum=${task.gtParts.negAmountSum}, prime=${task.gtParts.prime})`, gt: task.gtParts });

  const p = await plan(task);
  let answer, err = null;
  try { answer = await execute(p, task); } catch (e) { err = e.message; }

  const ansStr = String(answer && typeof answer === "object" ? (answer.final ?? answer.answer ?? JSON.stringify(answer)) : answer).replace(/[^0-9-]/g, "");
  const correct = ansStr === task.groundTruth;
  log("RESULT", { msg: `answer=${ansStr} gt=${task.groundTruth} correct=${correct} err=${err}`, answer: ansStr, gt: task.groundTruth, correct });

  const out = {
    generatedAt: new Date().toISOString(),
    model: "@cf/moonshotai/kimi-k2.7-code (reasoning effort=high)",
    task: { question: task.question, shape: task.shape, subset: task.subset, groundTruth: task.groundTruth, gtParts: task.gtParts, contextChars: task.context.length, records: task.records.length },
    plan: p, answer: ansStr, correct, err,
    cost: { ...COST, wallclock_s: ((Date.now() - t0) / 1000).toFixed(1) },
    trace: TRACE,
  };
  fs.writeFileSync(new URL("./result.json", import.meta.url), JSON.stringify(out, null, 2));
  console.log(`\n=== ${correct ? "CORRECT" : "WRONG"} | answer=${ansStr} gt=${task.groundTruth} | calls=${COST.calls} (${JSON.stringify(COST.by_role)}) | ${out.cost.wallclock_s}s ===`);
}
main().catch((e) => { console.error("FATAL", e); process.exit(1); });
