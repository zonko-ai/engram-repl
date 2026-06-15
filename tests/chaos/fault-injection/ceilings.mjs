#!/usr/bin/env node

import { writeFileSync, mkdirSync, readFileSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import WebSocket from "ws";

const ROOT = new URL("../../..", import.meta.url).pathname;
const args = parseArgs(process.argv.slice(2));
const outDir = path.join(ROOT, "scratch/beam/fault-injection-results");
mkdirSync(outDir, { recursive: true });

const port = Number(args.port || 8789);
const endpoint = args.url ? String(args.url).replace(/\/$/, "") : "ws://127.0.0.1:" + port;
if (endpoint === "wss://engram.umgbhalla.xyz") throw new Error("refusing prod baseline");

async function sweepJsCallDepth() {
  const depths = [64, 128, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192];
  const rows = [];
  for (const d of depths) {
    const c = await Client.open(endpoint, "ceil-stack-" + d + "-" + Date.now().toString(36));
    await c.rpc({ t: "create", config: { faultTest: true, modules: false, cellBudgetTicks: 2000 } });
    const src = "function f(n){ if(n<=0) return 0; return 1+f(n-1); } f(" + d + ")";
    const r = await c.rpc({ t: "eval", src }, 45000).catch((e) => ({ ok: false, transportError: e.message }));
    rows.push({ depth: d, ok: r.ok === true && r.value === d, error: r.error?.name || r.error?.message || r.transportError || "", closed: !!r.transportError });
    c.close();
    if (r.transportError) break;
  }
  return rows;
}

async function sweepSnapshotChain() {
  const depths = [0, 1, 2, 5, 10, 15, 19, 20, 21, 25];
  const c = await Client.open(endpoint, "ceil-chain-" + Date.now().toString(36));
  await c.rpc({ t: "create", config: { faultTest: true, modules: false, rngSeed: 11 } });
  const rows = [];
  let last = null;
  for (let i = 0; i <= Math.max(...depths); i++) {
    if (i > 0) last = await c.rpc({ t: "eval", src: "globalThis.chain=(globalThis.chain||0)+1; chain" }, 45000);
    if (depths.includes(i)) {
      await c.rpc({ t: "evict" }, 30000);
      const r = await c.rpc({ t: "eval", src: "chain || 0" }, 90000).catch((e) => ({ ok: false, transportError: e.message }));
      rows.push({ depth: i, ok: r.ok, value: r.value, restoreSource: r.restoreSource, restoreTimings: r.restoreTimings, checkpoint: r.checkpoint || last?.checkpoint, error: r.error?.name || r.transportError || "" });
    }
  }
  c.close();
  return rows;
}

async function sweepHeapRaw() {
  const sizes = [8, 16, 24, 32, 48, 56, 60, 61, 64];
  const rows = [];
  for (const mb of sizes) {
    const c = await Client.open(endpoint, "ceil-heap-" + mb + "-" + Date.now().toString(36));
    await c.rpc({ t: "create", config: { faultTest: true, modules: false, rngSeed: 12 } });
    const r = await c.rpc({ t: "eval", src: "globalThis.blob = 'x'.repeat(" + mb + " * 1024 * 1024); blob.length" }, 90000).catch((e) => ({ ok: false, transportError: e.message }));
    rows.push({ mb, ok: r.ok, value: r.value, error: r.error?.name || r.error?.message || r.checkpoint?.error || r.transportError || "", checkpoint: r.checkpoint });
    c.close();
  }
  return rows;
}

async function sweepHostCallFanout() {
  const calls = [1, 8, 32, 64, 65, 80];
  const rows = [];
  for (const n of calls) {
    const c = await Client.open(endpoint, "ceil-host-" + n + "-" + Date.now().toString(36));
    await c.rpc({ t: "create", config: { faultTest: true, modules: false } });
    const evalP = c.rpc({ t: "eval", src: "(async()=>{ let ok=0; for(let i=0;i<" + n + ";i++){ await host.pause('h'+i); ok++; } return ok; })()" }, 90000);
    let served = 0;
    while (served < n) {
      const hc = await c.nextHostcall(2000).catch(() => null);
      if (!hc) break;
      served++;
      await c.rpc({ t: "hostcall-result", id: hc.id, ok: true, value: "ok" }, 30000, { noReply: true });
    }
    const r = await evalP.catch((e) => ({ ok: false, transportError: e.message }));
    rows.push({ calls: n, served, ok: r.ok, value: r.value, error: r.error?.name || r.error?.message || r.transportError || "" });
    c.close();
  }
  return rows;
}

async function sweepSequentialSessions() {
  const counts = [1, 8, 32, 64, 128];
  const rows = [];
  for (const n of counts) {
    const started = Date.now();
    let ok = 0;
    let firstError = "";
    for (let i = 0; i < n; i++) {
      const c = await Client.open(endpoint, "ceil-seq-" + n + "-" + i + "-" + Date.now().toString(36));
      try {
        const created = await c.rpc({ t: "create", config: { faultTest: true, modules: false } }, 30000);
        const evaled = await c.rpc({ t: "eval", src: "globalThis.i=" + i + "; i" }, 30000);
        if (created.ok === true && evaled.ok === true && evaled.value === i) ok++;
        else if (!firstError) firstError = JSON.stringify({ created, evaled }).slice(0, 240);
      } catch (e) {
        if (!firstError) firstError = e.message;
      } finally {
        c.close();
      }
    }
    rows.push({ sessions: n, ok, elapsedMs: Date.now() - started, error: firstError });
    if (ok < n) break;
  }
  return rows;
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
    const wsUrl = base.replace(/^http/, "ws") + "/ws?id=" + encodeURIComponent(session);
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
  if (!args.url) dev = await startWrangler(port);
  const run = { startedAt: new Date().toISOString(), endpoint, curves: {} };
  run.curves.jsCallDepth = await sweepJsCallDepth();
  run.curves.snapshotChain = await sweepSnapshotChain();
  run.curves.heapRaw = await sweepHeapRaw();
  run.curves.hostCallFanout = await sweepHostCallFanout();
  run.curves.sequentialSessions = await sweepSequentialSessions();
  run.curves.staticOrchestration = {
    facetsPerShard: 128,
    shards: 64,
    theoreticalFacetSlots: 8192,
    cowAmplification: "current substrate has no fork primitive; child sessions copy full heap image, so amplification is O(children * rawImageBytes)",
    hostCallDefaultCap: 64,
  };
  run.finishedAt = new Date().toISOString();
  const out = path.join(outDir, "ceilings-run-" + Date.now() + ".json");
  writeFileSync(out, JSON.stringify(run, null, 2));
  console.log(JSON.stringify({ ok: true, out, curves: run.curves }, null, 2));
} finally {
  if (dev) dev.kill("SIGTERM");
}
