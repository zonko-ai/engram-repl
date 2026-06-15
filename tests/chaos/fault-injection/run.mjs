#!/usr/bin/env node

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import WebSocket from "ws";

const ROOT = new URL("../../..", import.meta.url).pathname;
const PROD_BASELINE = "wss://engram.umgbhalla.xyz";
const args = parseArgs(process.argv.slice(2));
const expect = String(args.expect || "fixed");
const only = String(args.only || "all");
const outDir = path.join(ROOT, "scratch/beam/fault-injection-results");
mkdirSync(outDir, { recursive: true });

assertProdFaultGateOff();

const url = String(args.url || "");
const local = !url;
const port = Number(args.port || 8788);
const endpoint = local ? "ws://127.0.0.1:" + port : url.replace(/\/$/, "");
const wsPath = String(args.wsPath || "/ws");
if (endpoint === PROD_BASELINE || endpoint.startsWith(PROD_BASELINE + "/")) {
  throw new Error("refusing to run fault injection against prod baseline " + PROD_BASELINE);
}

async function caseGateOff(run) {
  const cfg = readFileSync(path.join(ROOT, "apps/kernel/wrangler.jsonc"), "utf8");
  const pass = !/ENGRAM_FAULT_TEST/.test(cfg);
  record(run, "prod_fault_flag_absent", pass, "apps/kernel/wrangler.jsonc has no ENGRAM_FAULT_TEST var");
}

async function caseTelemetryAndBug1Ceiling(run) {
  const s = "fault-bug1-" + Date.now().toString(36);
  const c = await Client.open(endpoint, s);
  await c.rpc({ t: "create", config: { faultTest: true, modules: false, rngSeed: 7 } });
  const normal = await c.rpc({ t: "eval", src: "globalThis.keep = 'ok'; keep" }, 30000);
  const ft = normal.checkpoint?.faultTelemetry || {};
  record(run, "bug1_telemetry_echo", normal.ok === true && ft.bufferBytes > 0 && ft.usedHeap > 0 && ft.scratchLen > 0, JSON.stringify(ft));

  const sizes = [8, 16, 24, 32, 48, 60, 61];
  const sweep = [];
  for (const mb of sizes) {
    const r = await c.rpc({
      t: "eval",
      src: "globalThis.__blob = 'x'.repeat(" + mb + " * 1024 * 1024); __blob.length",
      config: { modules: false },
    }, 90000).catch((e) => ({ ok: false, transportError: e.message }));
    sweep.push({
      mb,
      ok: r.ok,
      value: r.value,
      error: r.error?.name || r.error?.message || r.checkpoint?.error || r.transportError || "",
      checkpoint: r.checkpoint,
    });
    await c.rpc({ t: "reset" }, 30000).catch(() => null);
    await c.rpc({ t: "create", config: { faultTest: true, modules: false, rngSeed: 7 } }, 30000).catch(() => null);
  }
  const admitted16 = sweep.find((x) => x.mb === 16)?.checkpoint?.ok === true;
  const rejected61 = JSON.stringify(sweep.find((x) => x.mb === 61) || {}).includes("SizeAdmissionError");
  record(run, "bug1_measured_ceiling_gate", admitted16 && rejected61, JSON.stringify(sweep));
  c.close();
}

async function caseBug2R2Miss(run) {
  const s = "fault-bug2-" + Date.now().toString(36);
  const c = await Client.open(endpoint, s);
  await c.rpc({ t: "create", config: { faultTest: true, modules: false, rngSeed: 9 } });
  for (let i = 0; i < 19; i++) {
    const r = await c.rpc({ t: "eval", src: "globalThis.bug2_" + i + "=" + i + "; " + i }, 45000);
    if (r.ok !== true) throw new Error("bug2 setup failed at " + i + ": " + JSON.stringify(r));
  }
  // Arm the miss BEFORE the rollover cell. Fixed code consumes it during verify-before-truncate and
  // stores the new base in SQLite. Baseline code has no verification read, so the miss survives until
  // cold restore and forces replay from the newly truncated one-row oplog.
  await c.rpc({ t: "_fault", op: "r2-miss-once", key: "*" });
  await c.rpc({ t: "_fault", op: "force-r2-next-full" });
  const base = await c.rpc({ t: "eval", src: "globalThis.bug2_final = 21; bug2_final" }, 120000);
  const r2Key = base.checkpoint?.r2Key || "";
  await c.rpc({ t: "evict" });
  const restored = await c.rpc({ t: "eval", src: "({sum: bug2_0 + bug2_final, hasPayload: typeof bug2_payload})" }, 120000);
  const fixedOk = base.ok === true && base.checkpoint?.store === "sqlite" && restored.ok === true && restored.value?.sum === 21;
  const baselineRepro = base.ok === true && base.checkpoint?.store === "r2" && restored.ok === false && /bug2_0|not defined|ReferenceError/.test(JSON.stringify(restored));
  const ok = expect === "baseline" ? baselineRepro : fixedOk;
  record(run, expect === "baseline" ? "bug2_r2_miss_silent_loss_reproduced" : "bug2_r2_miss_closed", ok, JSON.stringify({ expect, baseCheckpoint: base.checkpoint, r2Key, restored }));
  c.close();
}

async function caseBug3ParkedVfsRace(run) {
  const s = "fault-bug3-" + Date.now().toString(36);
  const c = await Client.open(endpoint, s);
  const c2 = await Client.open(endpoint, s);
  await c.rpc({ t: "create", config: { faultTest: true, modules: false, fs: { provider: "r2" } } });
  const evalP = c.rpc({ t: "eval", src: "const fs = require('fs'); await fs.promises.writeFile('/workspace/race.txt', 'cell'); await host['pause']('race'); 'done'" }, 90000);
  const hc = await c.nextHostcall(30000).catch(async (e) => {
    const early = await Promise.race([evalP, sleep(1).then(() => null)]);
    throw new Error(e.message + "; early eval=" + JSON.stringify(early));
  });
  const wr = await c2.rpc({ t: "vfs-write", path: "/workspace/race.txt", dataB64: Buffer.from("frame").toString("base64"), truncate: true }, 30000);
  await c.rpc({ t: "hostcall-result", id: hc.id, ok: true, value: "resume" }, 30000, { noReply: true });
  const ev = await evalP;
  const read = await c2.rpc({ t: "vfs-read", path: "/workspace/race.txt" }, 30000);
  const body = read.dataB64 ? Buffer.from(read.dataB64, "base64").toString("utf8") : "";
  const fixedOk = wr.ok === true && ev.ok === true && read.ok === true && body === "frame";
  const baselineRepro = wr.ok === true && ev.ok === true && read.ok === true && body === "cell";
  const ok = expect === "baseline" ? baselineRepro : fixedOk;
  record(run, expect === "baseline" ? "bug3_parked_vfs_clobber_reproduced" : "bug3_parked_vfs_write_preserved", ok, JSON.stringify({ expect, hostcall: hc, wr, ev, read, body }));
  c2.close();
  c.close();
}

function assertProdFaultGateOff() {
  const cfg = readFileSync(path.join(ROOT, "apps/kernel/wrangler.jsonc"), "utf8");
  if (/ENGRAM_FAULT_TEST/.test(cfg)) throw new Error("prod wrangler.jsonc must not define ENGRAM_FAULT_TEST");
}

async function startWrangler(port) {
  const p = spawn("bunx", ["wrangler", "dev", "-c", "apps/kernel/wrangler.faulttest.jsonc", "--local", "--port", String(port)], {
    cwd: ROOT,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NO_COLOR: "1" },
  });
  let log = "";
  p.stdout.on("data", (d) => { log += d.toString(); });
  p.stderr.on("data", (d) => { log += d.toString(); });
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    if (/Ready on|localhost|127\.0\.0\.1|Total Upload/.test(log)) {
      await sleep(1000);
      return p;
    }
    if (p.exitCode != null) throw new Error("wrangler dev exited early:\n" + log);
    await sleep(250);
  }
  p.kill("SIGTERM");
  throw new Error("wrangler dev did not become ready:\n" + log);
}

class Client {
  static open(base, session) {
    const sep = wsPath.includes("?") ? "&" : "?";
    const wsUrl = base.replace(/^http/, "ws") + wsPath + sep + "id=" + encodeURIComponent(session);
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(wsUrl);
      const c = new Client(ws);
      ws.once("open", () => resolve(c));
      ws.once("error", reject);
    });
  }
  constructor(ws) {
    this.ws = ws;
    this.hostcalls = [];
    this.waitingHostcall = [];
    ws.on("message", (d) => {
      let m;
      try { m = JSON.parse(d.toString()); } catch { return; }
      if (m.t === "hostcall") {
        const waiter = this.waitingHostcall.shift();
        if (waiter) waiter.resolve(m); else this.hostcalls.push(m);
      }
    });
  }
  rpc(frame, timeout = 30000, opts = {}) {
    if (opts.noReply) {
      this.ws.send(JSON.stringify(frame));
      return Promise.resolve({ ok: true, noReply: true });
    }
    return new Promise((resolve, reject) => {
      const onMsg = (d) => {
        let m;
        try { m = JSON.parse(d.toString()); } catch { return; }
        if (m.t === "hostcall") return;
        cleanup();
        resolve(m);
      };
      const onClose = () => { cleanup(); reject(new Error("ws closed")); };
      const timer = setTimeout(() => { cleanup(); reject(new Error("timeout " + JSON.stringify(frame).slice(0, 120))); }, timeout);
      const cleanup = () => {
        clearTimeout(timer);
        this.ws.off("message", onMsg);
        this.ws.off("close", onClose);
      };
      this.ws.on("message", onMsg);
      this.ws.once("close", onClose);
      this.ws.send(JSON.stringify(frame));
    });
  }
  nextHostcall(timeout = 30000) {
    if (this.hostcalls.length) return Promise.resolve(this.hostcalls.shift());
    return new Promise((resolve, reject) => {
      const waiter = { resolve: (m) => { clearTimeout(timer); resolve(m); } };
      const timer = setTimeout(() => {
        const i = this.waitingHostcall.indexOf(waiter);
        if (i >= 0) this.waitingHostcall.splice(i, 1);
        reject(new Error("timeout waiting hostcall"));
      }, timeout);
      this.waitingHostcall.push(waiter);
    });
  }
  close() { try { this.ws.close(); } catch {} }
}

function record(run, name, pass, detail) {
  const row = { name, pass, detail, at: new Date().toISOString() };
  run.results.push(row);
  console.log((pass ? "PASS" : "FAIL") + " " + name + " :: " + detail);
  if (!pass && !process.env.FAULT_ALLOW_FAIL) throw new Error("case failed: " + name);
}

function parseArgs(argv) {
  const out = {};
  for (const a of argv) {
    if (!a.startsWith("--")) continue;
    const i = a.indexOf("=");
    out[a.slice(2, i < 0 ? undefined : i)] = i < 0 ? true : a.slice(i + 1);
  }
  return out;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

let dev;
try {
  if (local) dev = await startWrangler(port);
  const run = { startedAt: new Date().toISOString(), endpoint, expect, results: [] };
  await caseGateOff(run);
  if (only === "all" || only === "bug1") await caseTelemetryAndBug1Ceiling(run);
  if (only === "all" || only === "bug2") await caseBug2R2Miss(run);
  if (only === "all" || only === "bug3") await caseBug3ParkedVfsRace(run);
  run.finishedAt = new Date().toISOString();
  const out = path.join(outDir, "fault-run-" + Date.now() + ".json");
  writeFileSync(out, JSON.stringify(run, null, 2));
  console.log(JSON.stringify({ ok: true, out, results: run.results }, null, 2));
} finally {
  if (dev) dev.kill("SIGTERM");
}
