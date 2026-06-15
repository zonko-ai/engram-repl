// kernel-glue.ts — the THIN JS shim that the Rust DO (lib.rs) binds to.
//
// Authored in TS; esbuild bundles this to src/kernel-glue.mjs (the path wasm-bindgen's
// `#[wasm_bindgen(module = "/src/kernel-glue.mjs")]` reads + worker-build copies into its
// snippet dir) BEFORE worker-build runs. The REPL-persistence transform is imported from
// ./repl-transform.ts and INLINED into the single .mjs output by esbuild — so the runtime
// module remains a single file with no sibling import (which would not resolve in the
// worker-build snippet dir).
//
// IMPORTANT: this file contains NO kernel business logic. All eval / value-preview / guards /
// determinism / host-boundary logic lives in the Rust ENGINE wasm (src/engine.wasm). This shim
// only provides:
//   * the WASI host imports the engine needs (wasm32-wasip1),
//   * the literal memory.buffer BLIT (snapshot substrate) + gzip,
//   * the engine instantiate + the host-effect IMPLEMENTATION the engine can't do from inside
//     wasip1 (host.fetch -> DO-side fetch with allowlist).
//
// The engine ABI (C exports) is documented in engine/src/lib.rs.

import { transformCell, wrapAsyncCompletion } from "./repl-transform.js";
// sucrase: a fast, pure-JS TypeScript -> JavaScript transformer. esbuild (build-ts) INLINES this
// devDependency into kernel-glue.mjs (it is pure JS, runs inside workerd). We use it as the TS
// type eraser for REPL cells (replacing the hand-rolled regex eraser): it strips all TS type
// syntax (annotations / generics / interfaces / `as` / `satisfies`) AND transforms enum -> IIFE,
// while `disableESTransforms:true` leaves ES module / async / spread syntax untouched so the engine
// (which targets a modern ES2022 VM) still receives valid modern JS. The downstream transformCell
// re-tokenizes its input, so sucrase's non-length-preserving output is fine here.
import { transform as sucraseTransform } from "sucrase";
// Outbound TCP/TLS host provider (cloudflare:sockets). JS module; cloudflare:sockets is kept
// EXTERNAL by build-ts and resolved by workerd at runtime. Backs the VM-side net/tls shims.
// @ts-ignore — JS sibling module, no .d.ts; typed `any` here.
import { makeSocketHost } from "./host-sockets.mjs";

// stripTypes: erase TypeScript type syntax via sucrase (a fast, pure-JS TS->JS transformer that is
// inlined into kernel-glue.mjs by esbuild and runs inside workerd). Replaces the former hand-rolled
// regex eraser. `transforms:['typescript']` strips all type-only syntax (annotations, generics,
// interfaces, type aliases, `as`/`satisfies`, declare) AND lowers `enum` -> an IIFE (the hand-rolled
// eraser rejected enum); `disableESTransforms:true` leaves ES module / async / spread untouched so
// the modern-ES2022 VM still receives valid modern JS. On genuinely-broken TS, sucrase throws a parse
// error -> we re-tag it as a typed TypeScriptError so the cell rejects cleanly (socket alive, mutex
// released, next eval works) instead of crashing. Output is NOT length-preserving, but the downstream
// transformCell re-tokenizes its input, so offsets do not need to be stable. Disabled when config
// {typescript:false}.
function stripTypes(src: string): string {
  try {
    return sucraseTransform(src, { transforms: ["typescript"], disableESTransforms: true }).code;
  } catch (e) {
    const msg = (e as { message?: string })?.message || String(e);
    const err = new Error(`TypeScript transform failed: ${msg}`);
    (err as Error & { name: string }).name = "TypeScriptError";
    throw err;
  }
}

// ---- engine C-ABI export surface (the precompiled rquickjs engine.wasm) ----
interface EngineExports {
  memory: WebAssembly.Memory;
  _initialize?: () => void;
  create(clockSeed: bigint, rngSeed: bigint): void;
  reattach(): number;
  set_counters(clockCalls: bigint, rngCalls: bigint): void;
  // set the in-VM monotonic CLOCK directly (next __now = 1.7e12 + CLOCK). clock:real re-anchor only.
  set_clock(clock: bigint): void;
  clock_calls(): number | bigint;
  rng_calls(): number | bigint;
  buffer_bytes(): number | bigint;
  used_heap(): number | bigint;
  scrub_arena(budgetMb: number): number | bigint;
  scratch_ptr(): number;
  scratch_cap(): number;
  scratch_reserve(len: number): number; // grow dynamic scratch to fit; returns (possibly moved) ptr
  scratch_release(): void;               // shrink dynamic scratch back to the 1MB floor after a cell
  result_ptr(): number;
  result_len(): number;
  pending_host_call_ptr(): number;
  pending_host_call_len(): number;
  kv_export_ptr(): number;
  kv_export_len(): number;
  kv_import(ptr: number, len: number): void;
  result_artifact_chunk(idPtr: number, idLen: number, offset: number, len: number): number;
  eval_begin(ptr: number, len: number, budget: bigint, growCapPages: number): number;
  eval_resume(ptr: number, len: number): number;
  run_gc(): void;
}

interface WasiImports {
  [k: string]: (...args: number[]) => number | void;
}
interface WasiCtl {
  wasi: WasiImports;
  setMem: (m: WebAssembly.Memory) => void;
}

// A host call request emitted by the engine (e.g. {name:"fetch", args:[...]}).
interface HostCall {
  name: string;
  args?: unknown[];
}
// A host call result fed back to the engine.
type HostResult =
  | { ok: true; value: unknown }
  | { ok: false; error: string };
interface KernelConfig {
  rngSeed?: number;
  // clock mode. UNSET / anything but "real" ⇒ SEEDED (deterministic): the in-VM clock starts at the
  // 1.7e12 epoch (Nov 2023), +1ms/call, byte-identical across restore. "real" ⇒ seed the in-VM clock
  // from the DO's real host-side wall clock at create so Date/now() reflect the real year/time (still
  // frozen-in-turn + deterministic +1ms/call from a real base). Persisted across cold restore.
  clock?: "real" | "seeded" | string;
  typescript?: boolean;
  // egress allowlist. UNSET ⇒ ALLOW-ALL egress (default); false ⇒ block all; [hosts] ⇒ hostname
  // allowlist. A blocked host rejects with FetchBlockedError (socket stays alive).
  fetch?: boolean | string[];
  // stdlib preload. UNSET / "default" ⇒ the sensible default set (STDLIB_META.defaults); false ⇒
  // bare VM (no preload); true ⇒ all curated modules minus optIn; [names] ⇒ defaults + the named
  // extras (additive; optIn modules allowed when explicitly named). Anything else: await use('pkg').
  modules?: boolean | "default" | string[];
  cellBudgetTicks?: number;
  cellGrowCapPages?: number;
  maxHostCallsPerCell?: number;
  // TRUE STREAMING (#13): per-cell budget for stream chunk pulls (streamRead/streamWrite), counted
  // separately from maxHostCallsPerCell so a long stream is not capped by the 64 host-call guard.
  maxStreamOpsPerCell?: number;
  // WEBSOCKET-CLIENT-IN-VM (#12): per-cell budget for ws.* effects (recv/poll/send), counted
  // separately from maxHostCallsPerCell so a chatty WS pump is not capped by the 64 host-call guard.
  maxWsOpsPerCell?: number;
  // HOST-BACKED FS (#18): per-cell budget for host.__fs chunk calls (a body >64KB is chunked),
  // counted separately so a multi-MB host.fs read/write is not capped by the 64 host-call guard.
  maxFsOpsPerCell?: number;

  // fs provider: undefined / {provider:"vfs"} = in-heap VFS (sync, durable in snapshot);
  // {provider:"r2",binding?,prefix?} = host-backed R2 (async-only; serviced DO-side in lib.rs).
  fs?: { provider?: string; binding?: string; prefix?: string };

  // EXTENSIBILITY API — REDUCED Phase 1 (#32): builder-registered host tools routed to the CLIENT.
  // Validated + persisted in lib.rs create_critical (CLIENT backend only; http/worker rejected).
  // Each ext installs a host.<name>.<fn> namespace (glue-seeded snippet) delegating to the existing
  // flat-name host Proxy -> {t:hostcall} client bridge. SECRET-FREE; no credentials in this phase.
  extensions?: ExtManifest[];
}

interface ExtTool {
  fn: string;
  params?: unknown;
  description?: string;
  example?: unknown;
}
interface ExtManifest {
  name: string;
  version?: string;
  backend?: { kind?: string };
  tools?: ExtTool[];
  limits?: { callsPerCell?: number; maxResultBytes?: number };
}

interface RestoreTimings {
  gunzipMs?: number;
  instantiateMs?: number;
  growCount?: number;
  neededPages?: number;
  deltas?: number;
}

interface DumpCommon {
  sizeRaw: number;
  usedHeap: number;
  bufferBytes: number;
  scratchLen: number;
  scrubbed: boolean;
  stackPointer: number;
  clockCalls: number;
  rngCalls: number;
  kvJson: string;
}

interface DumpResult extends DumpCommon {
  gz: Uint8Array;
  sizeGz: number;
  // Codec used for `gz` (and, on a delta, `indicesGz`): "zstd" for new dumps, "gzip" historically.
  // The DO persists it in the manifest / delta row; restore dispatches on it (back-compat). Issue #9.
  snapCodec: SnapCodec;
  mode: "full" | "delta";
  grain: number;
  imageLen: number;
  nChanged: number;
  indicesGz: Uint8Array | null;
  // CRC32 of the FULL reconstructed image this dump represents (delta => base+chain incl. this row).
  // The DO carries it in the manifest; restoreW4 recomputes after reconstruction and throws a
  // RestoreSanityError on mismatch => oplog replay fallback. W4 defense-in-depth (task #15).
  imageCrc: number;
}

interface SerializeResult {
  raw: Uint8Array;
  usedHeap: number;
  bufferBytes: number;
  scrubbed: boolean;
}

// W4 delta descriptor consumed by restoreW4.
interface W4Delta {
  gz: Uint8Array;
  indicesGz: Uint8Array;
  grain?: number;
  // Per-delta codec (a chain can straddle a deploy boundary: gzip base + zstd deltas, or vice
  // versa). Absent => "gzip" (back-compat). Issue #9.
  codec?: string;
  // Recorded uncompressed byte-lengths of the payload/index blobs (= nChanged*grain / nChanged*4).
  // Used to size the zstd dst buffer; the gzip path ignores them. Optional for back-compat.
  payloadLen?: number;
  indicesLen?: number;
}

interface ReplayCell {
  src?: string;
  hostResults?: HostResult[];
}

// The precompiled engine WebAssembly.Module, exposed on globalThis by entry.ts
// (CompiledWasm import — workerd forbids runtime WebAssembly.compile of bytes).
const ENGINE_MODULE = (): WebAssembly.Module => globalThis.__ENGINE_MODULE as WebAssembly.Module;
const ENGINE_HASH = (): string => globalThis.__ENGINE_HASH || "rust-engine-unknown";

export function getEngineHash(): string {
  return ENGINE_HASH();
}

// ---- in-DO async mutex (JS promise-chain) — same shape lib.rs expects ----
export function newMutex(): Mutex {
  return new Mutex();
}
class Mutex {
  _tail: Promise<unknown>;
  constructor() {
    this._tail = Promise.resolve();
  }
  acquire(): Promise<() => void> {
    let release!: () => void;
    const next = new Promise<void>((res) => (release = res));
    const prev = this._tail;
    this._tail = next;
    return prev.then(() => release);
  }
}

// ---- minimal WASI preview1 shim sufficient for the engine ----
// The engine only needs: clock (seeded — we override via the Rust clock anyway), random_get
// (seeded), fd_write (stderr/stdout no-op-ish), proc_exit, args/env stubs, and the handful of
// fns rquickjs/std touch. Determinism: random_get is SEEDED so a seeded session is
// byte-reproducible across restore.
function makeWasi(rngSeed: number): WasiCtl {
  let mem: WebAssembly.Memory | null = null;
  const setMem = (m: WebAssembly.Memory): void => { mem = m; };
  // seeded mulberry32 for random_get (entropy externalized + deterministic).
  let s = (rngSeed >>> 0) || 0x9e3779b9;
  const rnd = (): number => {
    s |= 0;
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const view = (): DataView => new DataView((mem as WebAssembly.Memory).buffer);
  const u8 = (): Uint8Array => new Uint8Array((mem as WebAssembly.Memory).buffer);
  const wasi: WasiImports = {
    args_get: () => 0,
    args_sizes_get: (argc: number, argvBuf: number) => {
      view().setUint32(argc, 0, true);
      view().setUint32(argvBuf, 0, true);
      return 0;
    },
    environ_get: () => 0,
    environ_sizes_get: (c: number, b: number) => {
      view().setUint32(c, 0, true);
      view().setUint32(b, 0, true);
      return 0;
    },
    clock_res_get: (_id: number, out: number) => {
      view().setBigUint64(out, 1000n, true);
      return 0;
    },
    clock_time_get: (_id: number, _prec: number, out: number) => {
      // frozen deterministic clock; the engine uses its OWN seeded Date.now anyway.
      view().setBigUint64(out, 0n, true);
      return 0;
    },
    fd_write: (fd: number, iovs: number, iovsLen: number, nwritten: number) => {
      // count bytes; route stderr(2) to console for engine panics; drop stdout.
      let total = 0;
      const dv = view();
      const chunks: Uint8Array[] = [];
      for (let i = 0; i < iovsLen; i++) {
        const ptr = dv.getUint32(iovs + i * 8, true);
        const len = dv.getUint32(iovs + i * 8 + 4, true);
        total += len;
        if (fd === 2 && len > 0) chunks.push(u8().slice(ptr, ptr + len));
      }
      if (chunks.length) {
        try {
          const all = chunks.reduce((a, c) => a + new TextDecoder().decode(c), "");
          if (all.trim()) console.error("[engine-stderr]", all);
        } catch { /* ignore */ }
      }
      dv.setUint32(nwritten, total, true);
      return 0;
    },
    fd_read: (_fd: number, _iovs: number, _iovsLen: number, nread: number) => {
      view().setUint32(nread, 0, true);
      return 0;
    },
    fd_close: () => 0,
    fd_seek: () => 0,
    fd_fdstat_get: () => 0,
    fd_fdstat_set_flags: () => 0,
    fd_prestat_get: () => 8, // WASI_EBADF -> end preopen enumeration
    fd_prestat_dir_name: () => 8,
    path_open: () => 8,
    random_get: (ptr: number, len: number) => {
      const a = u8();
      for (let i = 0; i < len; i++) a[ptr + i] = (rnd() * 256) & 0xff;
      return 0;
    },
    poll_oneoff: () => 0,
    sched_yield: () => 0,
    proc_exit: (code: number) => {
      throw new Error("engine proc_exit(" + code + ")");
    },
  };
  return { wasi, setMem };
}

// ---- host-callback bridge (VM -> client over the kernel WS) ------------------
//
// A cell that calls `host.<name>(...args)` for any name OTHER than the built-in
// `fetch` is a CLIENT host-callback: the kernel parks the VM eval, asks the connected
// client (over the same WS) to compute the result, and resumes the VM with it. The
// engine already suspends/resumes (host.fetch path) — we just route the request to the
// client instead of a local fetch.
//
// DEADLOCK CONSTRAINT (the single most important design point): the eval critical
// section in lib.rs holds `self.mutex` for the WHOLE eval. The client's
// {t:hostcall-result} reply arrives as a NEW websocket_message. That handler MUST NOT
// re-acquire the mutex (it is held by the suspended eval) — it must only resolve the
// pending resolver. So the pending-resolver registry lives HERE in the glue module
// (one DO isolate == one module instance), and lib.rs resolves it from
// websocket_message WITHOUT touching the mutex.
//
// `host.fetch` is unchanged (serviced locally, DO-side). Determinism + crash-replay are
// unaffected: the E6 oplog still records the host RESULT, so engine-migration replay
// feeds the recorded value back WITHOUT re-calling the client (see evalCode replay path).

interface HostCallSender {
  // send a frame string to the client; returns false if no socket is available.
  send: (frameJson: string) => boolean;
  timeoutMs: number;
}

// One registry per glue module instance (== per DO isolate). callId -> resolver.
const __pendingHostCalls = new Map<string, { resolve: (r: HostResult) => void; timer: ReturnType<typeof setTimeout> }>();
let __hostCallSeq = 0;

// Called by lib.rs from websocket_message on a {t:hostcall-result} frame, OUTSIDE the
// eval mutex. Resolves the parked VM host call. No-throw; unknown ids are ignored
// (late/duplicate replies).
export function resolveHostCall(id: string, ok: boolean, valueJson: string, error: string): void {
  const pend = __pendingHostCalls.get(id);
  if (!pend) return;
  __pendingHostCalls.delete(id);
  clearTimeout(pend.timer);
  if (ok) {
    let value: unknown = null;
    try { value = valueJson === undefined || valueJson === "" ? null : JSON.parse(valueJson); } catch { value = null; }
    pend.resolve({ ok: true, value });
  } else {
    pend.resolve({ ok: false, error: error || "host callback failed" });
  }
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// WORKER REGISTRY — content-addressed Dynamic-Worker-Loader compute (lib.rs routes the worker-*
// frames here; the Rust DO never calls env.LOADER itself because LOADER is JS-shaped).
// ════════════════════════════════════════════════════════════════════════════════════════════════

// Harness/ABI version. BUMP whenever HARNESS_SRC or the env shape (VfsGateway props / module dict)
// changes — the Worker-Loader docs REQUIRE that the loader callback returns IDENTICAL content for a
// given codeId, and the warm cache keys on the codeId ONLY. Bumping the prefix invalidates stale
// warm isolates so an old harness can never serve a new env shape.
const REGISTRY_ABI = "wkr1";
// Match the kernel's wrangler compatibility_date so the loaded isolate's runtime semantics line up.
const REGISTRY_COMPAT_DATE = "2025-05-01";
// Per-invoke caps surfaced to the dynamic worker. cpuMs is the WorkerCode limit (exceeding it
// "immediately throws"); the DO also races a wall timeout in lib.rs.
const REGISTRY_DEFAULT_CPU_MS = 5000;

// The registry-owned adapter (mainModule). CONSTANT — versioned via REGISTRY_ABI, never perturbing
// user source hashes (the hash covers ONLY user.js). It imports the registered user source and
// adapts BOTH supported contracts:
//   A. export async function run(input, env) { ...; return jsonable }   (also export default {run})
//   B. export default { async fetch(req, env) { ... } }                 (raw/streaming escape hatch)
// Input crosses as the POST body (JSON); output crosses back as the {ok, output|error} envelope.
// env.VFS is the session-scoped, prefix-isolated VFS gateway; globalOutbound:null = no egress.
const HARNESS_SRC = `import * as user from "./user.js";
export default {
  async fetch(req, env) {
    // Read the inbound body EXACTLY ONCE. The input crosses the Worker-Loader
    // entrypoint boundary as the request body (a fresh ArrayBuffer constructed
    // DO-side, never a re-forwarded already-disturbed stream). req.text() drains
    // the stream a single time; we keep the raw text so the run() path JSON-parses
    // it AND the raw-fetch escape hatch can reconstruct a FRESH unconsumed Request
    // (the original req's body is spent after this single read). An empty body =>
    // null input.
    let rawBody = "";
    try { rawBody = await req.text(); } catch (_) { rawBody = ""; }
    let input = null;
    try { input = rawBody ? JSON.parse(rawBody) : null; } catch (_) { input = null; }
    const run = (typeof user.run === "function" && user.run)
      || (user.default && typeof user.default.run === "function" && user.default.run);
    if (run) {
      try {
        const output = await run(input, env);
        return Response.json({ ok: true, output: output === undefined ? null : output });
      } catch (e) {
        return Response.json({ ok: false, error: { name: (e && e.name) || "WorkerRuntimeError", message: String((e && e.message) || e) } });
      }
    }
    if (user.default && typeof user.default.fetch === "function") {
      // Hand the raw-fetch worker a FRESH Request with an UNCONSUMED body (req's
      // body was already drained above to extract input).
      const fresh = new Request(req.url, { method: req.method, headers: req.headers, body: rawBody || undefined });
      return user.default.fetch(fresh, env);
    }
    return Response.json({ ok: false, error: { name: "ContractError", message: "export run(input, env) or default {fetch}" } });
  }
};
`;

// Normalize a VFS path EXACTLY as lib.rs norm_fs_path does: strip leading '/', collapse empty/'.'
// segments, resolve '..' by popping, REJECT a NUL byte. Returns a leading-slash path. The VfsGateway
// (entry.ts) MUST use this same helper so its fs/<doId>/<normpath> key byte-for-byte matches the
// kernel + vfs-* key scheme — any drift is a sandbox-escape / coherence bug. Kept here so lib.rs's
// trusted do_id + this normalization are the only inputs to the dynamic worker's R2 reach.
export function normFsPath(p: string): string {
  if (p.indexOf(" ") >= 0) throw new Error("EINVAL: path contains NUL byte");
  const out: string[] = [];
  for (const seg of String(p).split("/")) {
    if (seg === "" || seg === ".") continue;
    if (seg === "..") {
      // A `..` that pops below the session root is an escape — throw (matches norm_fs_path /
      // @engram/fs resolve), never silently no-op back to root.
      if (out.length === 0) throw new Error("EACCES: path escapes the session root");
      out.pop();
      continue;
    }
    out.push(seg);
  }
  return "/" + out.join("/");
}

// Minimal shape of the bits of env/ctx the registry needs (env/ctx arrive as raw JsValue from
// lib.rs — typed loosely to avoid coupling to the worker-rs Env type).
interface LoaderEnv {
  LOADER?: {
    get(codeId: string, cb: () => unknown): { getEntrypoint(name: string | null, opts?: unknown): { fetch(req: Request): Promise<Response> } };
  };
}
interface CtxExports {
  exports?: { VfsGateway?: (opts: { props: { doId: string } }) => unknown };
}

// worker-invoke entry: load the registered source into a FRESH Worker-Loader isolate keyed on the
// content hash (warm-cache dedup), hand it the prefix-isolated VFS gateway, run it via fetch(POST
// input), return the harness envelope string verbatim. lib.rs wraps the string into the
// worker-invoke-result frame. THROWS a typed Error (RegistryUnavailableError) when the loader or
// ctx.exports.VfsGateway is unavailable — NEVER falls back to passing the raw bucket (the leak).
export async function registryInvoke(
  env: unknown,
  ctx: unknown,
  doId: string,
  hash: string,
  source: string,
  inputJson: string,
  optsJson: string,
): Promise<string> {
  const e = env as LoaderEnv;
  if (!e || !e.LOADER || typeof e.LOADER.get !== "function") {
    const err = new Error("env.LOADER unavailable (Worker Loader binding missing; needs Workers Paid + the LOADER binding)");
    err.name = "RegistryUnavailableError";
    throw err;
  }
  let opts: { timeoutMs?: number; cpuMs?: number } = {};
  try { opts = JSON.parse(optsJson) || {}; } catch (_) { /* defaults */ }
  const cpuMs = typeof opts.cpuMs === "number" && opts.cpuMs > 0 ? opts.cpuMs : REGISTRY_DEFAULT_CPU_MS;

  // VFS gateway: a loopback WorkerEntrypoint instance bound to the TRUSTED doId (props.doId is
  // injected here from lib.rs's state.id(), NEVER chosen by the dynamic worker). FAIL CLOSED if
  // ctx.exports.VfsGateway is unavailable — do not hand the worker any bucket.
  // ctx arrives NULL from lib.rs (worker-rs keeps the DO ctx JsValue private across the
  // wasm-bindgen boundary). entry.ts captures the DO ctx — which carries `.exports` under the
  // enable_ctx_exports flag — into globalThis.__ENGRAM_DO_CTX keyed by the TRUSTED lowercase-hex
  // doId. Resolve it here so ctx.exports.VfsGateway is available. The doId is still the trusted
  // value lib.rs passed (state.id()), so prefix isolation is unchanged.
  let c = ctx as CtxExports;
  if (!c || !c.exports || typeof c.exports.VfsGateway !== "function") {
    const map = (globalThis as { __ENGRAM_DO_CTX?: Map<string, unknown> }).__ENGRAM_DO_CTX;
    const captured = map && map.get(String(doId).toLowerCase());
    if (captured) c = captured as CtxExports;
  }
  const vfsExport = c && c.exports && c.exports.VfsGateway;
  if (typeof vfsExport !== "function") {
    const err = new Error(
      "ctx.exports.VfsGateway unavailable — registry compute requires the enable_ctx_exports compat flag + the VfsGateway top-level export (fail-closed: never hand the dynamic worker the raw bucket)",
    );
    err.name = "RegistryUnavailableError";
    throw err;
  }
  const vfsStub = vfsExport({ props: { doId: String(doId) } });

  // doIdShort: first 16 hex of the DO id. MANDATORY in the codeId so two sessions sharing a hash
  // get SEPARATE warm isolates with SEPARATE VFS env (the warm cache keys on the codeId only — a
  // hash-only codeId would let session B's invoke hit session A's warm isolate and write fs/<A>/).
  const doIdShort = String(doId).replace(/[^0-9a-f]/gi, "").slice(0, 16) || "0";
  const codeId = `${REGISTRY_ABI}:${doIdShort}:${hash}`;

  const stub = e.LOADER.get(codeId, () => ({
    compatibilityDate: REGISTRY_COMPAT_DATE,
    mainModule: "harness.js",
    modules: {
      "harness.js": HARNESS_SRC,   // registry-owned adapter (constant, versioned via REGISTRY_ABI)
      "user.js": source,            // the registered, hash-identified source (plain string = ESM)
    },
    env: { VFS: vfsStub },          // session-scoped, prefix-isolated VFS gateway — the ONLY I/O
    globalOutbound: null,           // deny ALL egress: fetch()/connect() throw inside the worker
    limits: { cpuMs },              // WorkerCode-level CPU cap (isolate startup counts)
  }));

  const ep = stub.getEntrypoint(null, { limits: { cpuMs } });
  // Pass the input as the request body. CRITICAL: encode it to a FRESH Uint8Array
  // (ArrayBuffer-backed) body, NOT a string. A string body is a lazily-realized
  // ReadableStream whose single read is consumed by the Worker-Loader entrypoint
  // dispatch (transfer across the loaded-isolate boundary), so the harness's own
  // body read then throws "Body has already been used. It can only be used once."
  // An ArrayBuffer body is materialized up-front and is delivered to the loaded
  // worker intact + unconsumed, so the harness reads it exactly once. (`inputJson`
  // is a plain JSON string here — produced DO-side in lib.rs::worker_invoke — never
  // an already-disturbed inbound stream.)
  const inputBytes = TEXT_ENC.encode(inputJson);
  const res = await ep.fetch(
    new Request("https://invoke/", {
      method: "POST",
      body: inputBytes,
      headers: { "content-type": "application/json" },
    }),
  );
  return await res.text();
}

// ---- the GlueKernel the Rust DO drives ----
export function newGlueKernel(): GlueKernel {
  return new GlueKernel();
}

const TEXT_ENC = new TextEncoder();
const TEXT_DEC = new TextDecoder();
const STATUS_HOST_CALL = 1;

// ---- ADR-0012 binary-safe host.fetch ----
// The host fetch effect carries RAW BYTES across the engine<->glue JSON boundary as base64
// (`bodyB64`), the SAME pattern the R2 fs op uses (ADR-0011). This is the structural unblocker
// for binary transfers (git packfiles), which the prior `await r.text()` path lossy-corrupted +
// truncated. We KEEP a utf8 `body` (capped) for back-compat with the existing string fetch shim.
// Default cap is deliberately set to the live-verified stable boundary. The engine/glue boundary is
// still env-overridable via FETCH_MAX_BODY_BYTES for experiments, but the public kernel should
// truncate instead of letting 3MB+ response bodies close the WebSocket during decode/checkpoint.
// A request body may also be binary (git-upload-pack POSTs a packfile): the engine sends
// init.bodyB64 (base64 bytes) which we decode to a Uint8Array before fetch.
const FETCH_MAX_BODY_BYTES = (() => {
  const v = (globalThis as { FETCH_MAX_BODY_BYTES?: unknown }).FETCH_MAX_BODY_BYTES;
  const n = typeof v === "number" ? v : (typeof v === "string" ? parseInt(v, 10) : NaN);
  return Number.isFinite(n) && n > 0 ? n : 2 * 1024 * 1024;
})();
// The back-compat utf8 `body` is a CAPPED PREVIEW only — the exact bytes always travel as `bodyB64`.
// Capping is REQUIRED for big binary payloads: a 6.6MB PDF would otherwise also ship a ~6.6MB utf8
// `body` string ALONGSIDE the ~8.9MB base64 in the SAME host-call result, and the engine would hold
// BOTH as UTF-16 strings (~2x) — ~30MB of redundant strings that blew the VM's linear memory past
// the per-cell grow cap. `.text()`/`.json()` on a body larger than this cap fall back to decoding
// the exact bytes from `bodyB64` (engine shim), so correctness is unaffected; only the convenience
// preview is truncated. 64KB is enough for the common text/JSON case while keeping binary cheap.
const FETCH_BODY_UTF8_PREVIEW_BYTES = 64 * 1024;
// ---- TRUE STREAMING (#13) ----
// Per-chunk cap for streamRead results: each upstream reader.read() is re-chunked to <= this so the
// base64 (~1.34x) rides the dynamic SCRATCH result buffer (1MB floor / 32MB ceil) comfortably, never
// the fixed 64KB HOSTCALL *request* buffer (only the streamId crosses that way). 256KB raw.
const STREAM_CHUNK_BYTES = 256 * 1024;
// Below this content-length the DO buffers the whole body inline at fetchStream-open time and returns
// it as bodyB64 (today's one-round-trip semantics for small JSON APIs). >= this, or unknown length
// (chunked-transfer), takes the truly-chunked streamRead path.
const STREAM_INLINE_THRESHOLD = 256 * 1024;
// DO-side stream registry entry. reader = download cursor; writer/fetchPromise = upload (request-body
// streaming) before it flips to the response reader. Lives in DO memory, never snapshotted.
interface StreamState {
  reader?: ReadableStreamDefaultReader<Uint8Array>;
  writer?: WritableStreamDefaultWriter<Uint8Array>;
  fetchPromise?: Promise<Response>;
  pending?: Uint8Array;            // re-chunk leftover (upstream chunk > STREAM_CHUNK_BYTES)
  done: boolean;
}
// ---- WEBSOCKET-CLIENT-IN-VM (#12) ----
// Per-cell budget for ws.* effects (a chatty pump cell makes many recv/poll calls). Counted
// separately from maxHostCallsPerCell (the 64 guard) like maxStreamOpsPerCell.
const WS_MAX_OPS_PER_CELL = 4096;
// EXTENSIBILITY API — REDUCED Phase 1 (#32): hard caps for client-backend ext tools (mirror the
// spec's limits ceiling). callsPerCell <= 64; maxResultBytes <= 65536 (also keeps the oplog row
// well under SQLITE_MAX_VALUE_BYTES). config.limits may only LOWER these.
const EXT_MAX_CALLS_PER_CELL = 64;
const EXT_MAX_RESULT_BYTES = 65536;
// Idle deadline for a parked recv(): if no frame arrives within this window the recv resolves
// {type:"idle"} so the cell ends and the eval mutex turns over (never hold it forever).
const WS_RECV_IDLE_MS = 25000;
// (#5) Polling cadence WITHIN a parked recv. A parked recv re-checks its inbound queue every
// WS_RECV_POLL_MS via setTimeout — each tick is a fresh workerd I/O turn that lets the outbound
// socket's `message` listener run (enqueueing the frame), guaranteeing same-cell echo delivery
// (the cross-cell-works/same-cell-fails bug). Small enough for low echo latency, large enough that
// the poll loop adds negligible overhead vs a single long park. The direct waiter still wins races.
const WS_RECV_POLL_MS = 15;
// Bounded inbound queue per handle (backpressure between pump cells). Over this, drop-oldest and
// surface a typed {type:"overflow"} marker so loss is visible + deterministic-on-record.
const WS_QUEUE_MAX_FRAMES = 1024;
// A single inbound frame above this raw size is dropped with a typed {type:"overflow"} marker
// (rides SCRATCH like fetch bodies; ~24MB raw practical ceiling after base64 inflation).
const WS_FRAME_MAX_BYTES = 24 * 1024 * 1024;
// An inbound frame the DO pushes into a handle's queue. data is a string (text) or base64 (binary).
interface WsFrame { type: string; seq: number; data?: string; binary?: boolean; code?: number; reason?: string; severed?: boolean }
// DO-side WebSocket registry entry. socket = the live outbound WS (DO memory, NEVER snapshotted).
// queue = inbound frames with monotonic seq; waiters = parked recv resolvers woken by the socket
// listeners. Dies with the isolate at eviction -> severed-on-wake semantic.
interface WsState {
  socket: WebSocket;
  url: string;
  queue: WsFrame[];
  seq: number;                       // monotonic, last assigned
  waiters: Array<(f: WsFrame) => void>;
  severed: boolean;
  closed: boolean;
  droppedThrough: number;            // highest seq dropped by overflow (for the marker)
}

// base64 encode/decode over Uint8Array, chunked so a large packfile does not blow the call stack
// of String.fromCharCode(...spread). Mirrors the fs-op base64 boundary.

// SSRF defense: is this URL hostname a private/internal/metadata address that must be blocked
// UNCONDITIONALLY (even under allow-all egress and with a valid auth key)? Covers metadata
// (169.254.0.0/16), loopback (127.0.0.0/8, ::1), RFC1918 (10/8, 172.16/12, 192.168/16),
// 0.0.0.0, link-local v6 (fe80::/10), unique-local v6 (fc00::/7), and *.internal/localhost names.
//
// IMPORTANT — this block + any hostname allowlist do NOT defend against DNS-REBIND: a public
// hostname that resolves to a private IP at connect time slips through, because the worker never
// resolves the name itself (workerd has no in-VM DNS resolver). The product-chosen egress default
// is allow-all; the real rebind fix is per-token egress policy + resolve-pinning (pin the A/AAAA
// at allow-time and reconnect to the pinned literal) — tracked as the Phase-2 hardening (#30).
// Until then this is best-effort: literal-IP (all encodings) + known-internal-suffix inspection.

// Parse a private/loopback/link-local IPv4 given as 4 octet numbers.
function isPrivateV4(a: number, b: number, _c: number, _d: number): boolean {
  if (a === 127) return true;                       // 127.0.0.0/8 loopback
  if (a === 10) return true;                         // 10.0.0.0/8
  if (a === 0) return true;                          // 0.0.0.0/8
  if (a === 169 && b === 254) return true;          // 169.254.0.0/16 link-local + metadata
  if (a === 172 && b >= 16 && b <= 31) return true; // 172.16.0.0/12
  if (a === 192 && b === 168) return true;          // 192.168.0.0/16
  if (a === 100 && b >= 64 && b <= 127) return true; // 100.64.0.0/10 CGNAT
  return false;
}

// Normalize an all-numeric IPv4 in any legal encoding (dotted-quad, dotted shorthands 127.1 /
// 10.0.1, single decimal 2130706433, hex 0x7f000001, octal 0177.0.0.1) into 4 octets.
// Returns null if `h` is not an all-numeric IPv4 form (i.e. it's a real hostname).
function normalizeV4(h: string): [number, number, number, number] | null {
  // Each part: decimal, 0x-hex, or 0-leading octal. Up to 4 parts (a / a.b / a.b.c / a.b.c.d).
  const partsRaw = h.split(".");
  if (partsRaw.length < 1 || partsRaw.length > 4) return null;
  const nums: number[] = [];
  for (const p of partsRaw) {
    if (p.length === 0) return null;
    let n: number;
    if (/^0x[0-9a-f]+$/i.test(p)) n = parseInt(p.slice(2), 16);
    else if (/^0[0-7]+$/.test(p)) n = parseInt(p, 8);
    else if (/^0$/.test(p)) n = 0;
    else if (/^[1-9][0-9]*$/.test(p)) n = parseInt(p, 10);
    else return null; // contains a non-numeric char -> a real hostname, not an IPv4 literal
    if (!Number.isFinite(n) || n < 0) return null;
    nums.push(n);
  }
  // Pack per the inet_aton shorthand rules: the LAST part fills the remaining low octets.
  const k = nums.length;
  let value: number;
  if (k === 1) {
    value = nums[0];
  } else {
    // first k-1 parts are single octets; the last part fills the remaining (4-(k-1)) bytes.
    for (let i = 0; i < k - 1; i++) if (nums[i] > 0xff) return null;
    const fillBytes = 4 - (k - 1);
    const maxLast = fillBytes >= 4 ? 0xffffffff : Math.pow(256, fillBytes) - 1;
    if (nums[k - 1] > maxLast) return null;
    value = 0;
    for (let i = 0; i < k - 1; i++) value = value * 256 + nums[i];
    value = value * Math.pow(256, fillBytes) + nums[k - 1];
  }
  if (value > 0xffffffff) return null;
  return [
    (value >>> 24) & 0xff,
    (value >>> 16) & 0xff,
    (value >>> 8) & 0xff,
    value & 0xff,
  ];
}

// Parse an IPv6 literal (zone-id stripped, :: expanded, embedded v4 handled) into 16 bytes.
// Returns null if `h` is not a valid IPv6 form.
function parseV6(h: string): Uint8Array | null {
  let s = h;
  const zone = s.indexOf("%");
  if (zone >= 0) s = s.slice(0, zone); // strip zone-id (fe80::1%eth0)
  if (s.indexOf(":") < 0) return null;
  // split off an embedded IPv4 tail (::ffff:127.0.0.1) -> two 16-bit groups
  let head = s;
  let tailGroups: number[] = [];
  const lastColon = s.lastIndexOf(":");
  const tail = s.slice(lastColon + 1);
  if (tail.indexOf(".") >= 0) {
    const v4 = normalizeV4(tail);
    if (!v4) return null;
    tailGroups = [(v4[0] << 8) | v4[1], (v4[2] << 8) | v4[3]];
    head = s.slice(0, lastColon); // includes trailing ":" of "...:1.2.3.4"
    // head now ends with ":" ; normalize by trimming the dangling colon so split is clean
    if (head.endsWith(":") && !head.endsWith("::")) head = head.slice(0, -1);
  }
  const dbl = head.indexOf("::");
  let groups: number[];
  if (dbl >= 0) {
    const left = head.slice(0, dbl).split(":").filter((x) => x.length > 0);
    const right = head.slice(dbl + 2).split(":").filter((x) => x.length > 0);
    const known = left.length + right.length + tailGroups.length;
    if (known > 8) return null;
    const fill = 8 - known;
    const mid = new Array(fill).fill(0);
    const parse16 = (g: string[]) => {
      const out: number[] = [];
      for (const x of g) {
        if (!/^[0-9a-f]{1,4}$/i.test(x)) return null;
        out.push(parseInt(x, 16));
      }
      return out;
    };
    const l = parse16(left), r = parse16(right);
    if (!l || !r) return null;
    groups = [...l, ...mid, ...r, ...tailGroups];
  } else {
    const g = head.split(":").filter((x) => x.length > 0);
    const out: number[] = [];
    for (const x of g) {
      if (!/^[0-9a-f]{1,4}$/i.test(x)) return null;
      out.push(parseInt(x, 16));
    }
    groups = [...out, ...tailGroups];
  }
  if (groups.length !== 8) return null;
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 8; i++) {
    bytes[i * 2] = (groups[i] >>> 8) & 0xff;
    bytes[i * 2 + 1] = groups[i] & 0xff;
  }
  return bytes;
}

function isBlockedV6(bytes: Uint8Array): boolean {
  // ::  (unspecified) and ::1 (loopback)
  let allZeroButLast = true;
  for (let i = 0; i < 15; i++) if (bytes[i] !== 0) { allZeroButLast = false; break; }
  if (allZeroButLast && (bytes[15] === 0 || bytes[15] === 1)) return true;
  // fe80::/10 link-local
  if (bytes[0] === 0xfe && (bytes[1] & 0xc0) === 0x80) return true;
  // fc00::/7 unique-local
  if ((bytes[0] & 0xfe) === 0xfc) return true;
  // ::ffff:0:0/96 (IPv4-mapped) -> re-check the embedded v4 against the v4 private ranges
  let mapped = true;
  for (let i = 0; i < 10; i++) if (bytes[i] !== 0) { mapped = false; break; }
  if (mapped && bytes[10] === 0xff && bytes[11] === 0xff) {
    if (isPrivateV4(bytes[12], bytes[13], bytes[14], bytes[15])) return true;
  }
  return false;
}

function isBlockedSsrfHost(hostname: string): boolean {
  if (!hostname) return true;
  let h = hostname.toLowerCase();
  // strip IPv6 brackets if any slipped through
  if (h.startsWith("[") && h.endsWith("]")) h = h.slice(1, h.length - 1);
  // internal hostname suffixes / loopback names
  if (h === "localhost" || h.endsWith(".localhost")) return true;
  if (h.endsWith(".internal") || h.endsWith(".local")) return true;
  if (h === "metadata.google.internal") return true;

  // IPv6: real parse (zone-id strip, :: expansion, embedded v4) then numeric-range test.
  if (h.indexOf(":") >= 0) {
    const v6 = parseV6(h);
    if (v6) return isBlockedV6(v6);
    return true; // looks like IPv6 but failed to parse -> reject (fail-closed)
  }

  // IPv4: normalize ANY all-numeric encoding (decimal/hex/octal/shorthand) to dotted-quad first.
  const v4 = normalizeV4(h);
  if (v4) return isPrivateV4(v4[0], v4[1], v4[2], v4[3]);

  // An all-numeric host that did NOT normalize is a malformed literal, not a real hostname -> reject.
  if (/^[0-9.]+$/.test(h)) return true;

  return false;
}

// SSRF-SAFE redirect following. A default `redirect:"follow"` fetch only SSRF-checks the INITIAL
// host — a public URL that 302s to http://169.254.169.254/ would bypass the block. So we force
// `redirect:"manual"` and walk each hop ourselves: on a 3xx with a Location, re-parse it, re-run
// isBlockedSsrfHost on the new host, and re-fetch — capped at MAX_REDIRECT_HOPS. A blocked hop
// rejects. The returned Response is the first non-redirect (or the last hop at the cap).
const MAX_REDIRECT_HOPS = 5;
async function ssrfSafeFetch(url: string, init?: RequestInit): Promise<Response> {
  let curUrl = url;
  // strip any caller-supplied redirect mode; we always drive redirects manually.
  const baseInit: RequestInit = { ...(init || {}), redirect: "manual" };
  for (let hop = 0; hop <= MAX_REDIRECT_HOPS; hop++) {
    const r = await fetch(curUrl, baseInit);
    const isRedirect = r.status >= 300 && r.status < 400 && r.headers.has("location");
    if (!isRedirect) return r;
    if (hop === MAX_REDIRECT_HOPS) {
      throw new Error("FetchBlockedError: too many redirects (>" + MAX_REDIRECT_HOPS + ")");
    }
    const loc = r.headers.get("location") || "";
    let next: URL;
    try {
      next = new URL(loc, curUrl); // resolve relative Location against the current URL
    } catch {
      throw new Error("FetchBlockedError: invalid redirect Location");
    }
    if (isBlockedSsrfHost(next.hostname)) {
      throw new Error("FetchBlockedError: redirect to " + next.hostname + " is a blocked private/internal address");
    }
    // a 303 (or 302/301 on POST per browser behavior) becomes GET; keep it simple & safe: after the
    // first hop, drop the body and force GET so we never replay a body to a new (re-checked) origin.
    if (hop === 0 && baseInit.method && baseInit.method.toUpperCase() !== "GET" && baseInit.method.toUpperCase() !== "HEAD") {
      baseInit.method = "GET";
      delete (baseInit as { body?: unknown }).body;
    }
    curUrl = next.toString();
  }
  // unreachable (loop returns or throws), but satisfy the type checker.
  return fetch(curUrl, baseInit);
}

function bytesToB64(u8: Uint8Array): string {
  let s = "";
  const CH = 0x8000;
  for (let i = 0; i < u8.length; i += CH) {
    s += String.fromCharCode.apply(null, Array.prototype.slice.call(u8.subarray(i, i + CH)) as number[]);
  }
  return btoa(s);
}
function b64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// ---- W5 compaction thresholds (docs/W5-COMPACTION-PLAN.md, REALCF-VALIDATION sized) ----
// The engine's static SCRATCH buffer was raised 1MB -> 32MB (lib.rs, to let big fetch bodies / fs
// writes cross the host boundary). That 32MB lives in the SAME linear memory that the W4 snapshot
// blits, so EVERY session's buffer_bytes() is now ~32MB higher (the zero-filled scratch gzips to
// ~nothing, so the STORED image barely grows — only the RAW image size does). The raw-image (buffer)
// caps below were therefore raised by the +31MB scratch delta so a session that also pulled in a big
// payload can still snapshot/restore. The USED-HEAP caps measure the rquickjs allocator tally
// (memory_used_size) only — the static scratch is NOT counted there — so they are unchanged (heap
// cap untouched, per the task). The W4 delta path is preserved for all buffer sizes (resetting it to
// full would also reset the engine-migration oplog tail — see lib.rs checkpoint full path).
const SCRATCH_DELTA_BYTES = 31 * 1024 * 1024;       // 32MB scratch - the old 1MB it replaced
// In-VM clock epoch base: the engine's __now adds this (1.7e12 = Nov 2023) to clock_seed+tick.
// MUST stay in lockstep with engine/src/lib.rs (1_700_000_000_000.0 in inject_host_fns __now).
// config.clock==='real' seeds clock_seed = realEpochMs - CLOCK_EPOCH_BASE_MS so the VM clock reads real.
const CLOCK_EPOCH_BASE_MS = 1_700_000_000_000;
const MAX_USED_BYTES = 50 * 1024 * 1024;            // refuse snapshot above ~50MB live used heap
const MAX_RESTORE_USED_BYTES = 50 * 1024 * 1024;    // refuse restore above ~50MB recorded used heap
// CHAOS-FAULT-INJECTION 2026-06-15: restore retains one reconstructed image plus the live WASM
// instance (2x raw, after the BUG-1 triple-buffer mitigation). Local workerd sweep found the cliff
// between 60-64MiB raw; gate both dump and restore at 60MiB raw. This deliberately admits normal
// sessions whose raw image is ~29MiB but gzip is tiny, while rejecting the large raw images that
// cannot be restored without crossing the DO memory cliff.
const MEASURED_RESTORE_RAW_CEILING_BYTES = 60 * 1024 * 1024;
const SAFE_SERIALIZE_BUFFER_BYTES = MEASURED_RESTORE_RAW_CEILING_BYTES;
const MAX_RESTORE_RAW_BYTES = MEASURED_RESTORE_RAW_CEILING_BYTES;
// Above this raw-image size, dumpW4 takes a 2-copy NO-RETAIN full path (it does NOT retain
// _lastImage and does NOT diff against prev) so peak DO memory during the dump is ~raw + gz, not
// the ~3x raw the delta/full-with-retain paths cost — which OOMs the DO once the buffer carries a
// big payload on top of the 32MB scratch (measured: a 2.2MB-PDF cell pushes the buffer to ~67MB;
// the 3-copy full retain at 67MB hung the DO, while a normal session tops out at ~59MB and is safe).
// The threshold sits ABOVE the normal-session ceiling (~59MB) so ordinary cells keep the delta path
// (which preserves the engine-migration oplog tail); only a genuinely large payload trips it. A
// large-payload cell that trips it forces a full base (resetting the oplog tail), so engine-migration
// REPLAY of that specific cell would no-op — an accepted edge: the byte-blit image stays the primary
// restore path, and 32MB-class payload cells are the rare exception, not the steady state.
const LARGE_BUFFER_NO_RETAIN_BYTES = 63 * 1024 * 1024;
// CHECKPOINT-PEAK fix (docs/CHAOS-CASE1-VERDICT.md): on a FULL base whose raw image is at least
// this big, do NOT retain a host-side _lastImage copy (the next dump is forced full — chain-safe,
// since a full base resets the delta chain anyway). Caps a large full-base checkpoint peak at
// ~raw+gz instead of ~raw+gz+retain. Applies ONLY to full bases (delta MUST retain to keep the
// retained image == committed chain tail, else the W4 module-drop desync at commitDump()).
const FULL_BASE_NO_RETAIN_BYTES = 16 * 1024 * 1024;
const SCRUB_SLACK_BYTES = 4 * 1024 * 1024;          // scrub when freed slack exceeds this
const SCRUB_MAX_BUFFER_BYTES = 44 * 1024 * 1024 + SCRATCH_DELTA_BYTES;      // only scrub below this (stay under the absolute cap)
const COMPACT_TRIGGER_BYTES = 12 * 1024 * 1024;     // cell-boundary scrub trigger (bloated buffer)
const COMPACT_USED_RATIO = 0.4;                     // ...AND used/buffer < 0.4 (>=60% slack = freed spike)
// TIER-1 ADDITIVE INCOMPRESSIBLE-CONTENT CEILING (below the ~28MB transient-OOM cliff).
// buffer_bytes() includes up to SCRATCH_DELTA_BYTES of zero-filled scratch high-water that gzips
// away; the genuine INCOMPRESSIBLE content is ~max(usedHeap, bufferBytes - SCRATCH_DELTA_BYTES).
// A flat raw buffer_bytes() gate would false-trip every benign session that ever pulled in a big
// fetch (resident scratch high-water), which is exactly why the absolute cap is 76MB not 24MB.
// Gating the INCOMPRESSIBLE extent catches an incompressible-heap session BELOW the uncatchable
// WS-1006 cliff (the dump's transient ~2-3x gz/copy expansion OOMs there) WITHOUT false-tripping.
const INCOMPRESSIBLE_BUFFER_CEILING_BYTES = 24 * 1024 * 1024; // 24MB, below the ~28MB incompressible WS-1006 cliff

// ---- W4 byte-delta thresholds (docs/W4-BYTEDELTA-PLAN.md, W4-proven) ----
const DELTA_GRAIN_BYTES = 256;   // W4-proven sweet spot (295KB gz delta vs ~1MB full)
const DELTA_FALLBACK_PCT = 0.5;  // delta >= 50% of full => store full base instead (dense-mutation fallback)
// A delta row is stored as a SINGLE SQLite blob per column (payload / indices). DO SQLite caps a
// single value at ~2MB, so a delta whose payload or indices gz exceeds this must NOT be emitted as
// a delta (it'd hit SQLITE_TOOBIG on the delta_chunks INSERT). Fall back to a full base, which is
// chunked (sqlite) or pushed to R2 — both side-step the single-value cap. Conservative ceiling.
const DELTA_MAX_BLOB_BYTES = 1_400_000;

const MAX_STDLIB_SOURCE_BYTES = 500 * 1024; // V0.7 GUARD 1: combined injected stdlib source cap

// ---- v0.6 configurable in-VM stdlib (esbuilt IIFE bundle, evaled at create) ----
let __stdlibBundleCache: Record<string, string> | null = null;
function stdlibBundle(): Record<string, string> {
  if (__stdlibBundleCache) return __stdlibBundleCache;
  const raw = globalThis.__STDLIB_BUNDLE;
  try { __stdlibBundleCache = raw ? JSON.parse(raw) : {}; } catch { __stdlibBundleCache = {}; }
  return __stdlibBundleCache as Record<string, string>;
}
// Catalog accessors read globalThis.__STDLIB_META LAZILY (not at module-eval time): the entry
// module stamps __STDLIB_META AFTER the worker shim (and this glue) module is already evaluated
// (ES import hoisting), so a top-level const would capture an empty catalog. Reading per-call fixes
// the no-config default-preload (which is why `modules:undefined -> defaults` was silently empty).
function stdlibNames(): string[] { const m = globalThis.__STDLIB_META; return (m && Array.isArray(m.modules)) ? m.modules : []; }
function stdlibOptIn(): string[] { const m = globalThis.__STDLIB_META; return (m && Array.isArray(m.optIn)) ? m.optIn : []; }
// The sensible default preload set (heavy libs excluded; see STDLIB_META.defaults). Loaded when
// config.modules is UNSET so a no-config session gets IDs/dates/schema-validation/utility for free.
function stdlibDefaults(): string[] { const m = globalThis.__STDLIB_META; return (m && Array.isArray((m as { defaults?: string[] }).defaults)) ? (m as { defaults: string[] }).defaults : []; }
// Normalize config.modules -> concrete name list.
//   false                -> [] (bare VM, explicit opt-out)
//   undefined | "default" -> the sensible default set (NEW: a no-config session is no longer bare)
//   true                 -> all curated modules minus optIn
//   [names]              -> defaults + the named extras (additive; optIn allowed when named)
function resolveStdlibModules(cfgModules: boolean | "default" | string[] | undefined): string[] {
  const bundle = stdlibBundle();
  const available = new Set(Object.keys(bundle));
  const optIn = new Set(stdlibOptIn());
  const defaults = stdlibDefaults().filter((n) => available.has(n));
  if (cfgModules === false) return [];
  if (cfgModules === undefined || cfgModules === "default") return defaults;
  if (cfgModules === true) return stdlibNames().filter((n) => available.has(n) && !optIn.has(n));
  if (Array.isArray(cfgModules)) return Array.from(new Set([...defaults, ...cfgModules.filter((n) => available.has(n))]));
  return defaults;
}

class GlueKernel {
  inst: WebAssembly.Instance | null;
  wasiCtl: WasiCtl | null;
  config: KernelConfig;
  clockSeed: number;
  rngSeed: number;
  tsEnabled: boolean;
  lastTimings: RestoreTimings;
  _fetchAllow: boolean | string[];
  _lastImage: Uint8Array | null;
  // W4 COMMIT-ORDERING: the just-dumped candidate image. dumpW4/dump compute it but DO NOT promote
  // it to _lastImage — the DO calls commitDump() AFTER its SQL txn commits, only then does it become
  // the new delta base. This closes the desync where a dump whose row never became the committed
  // chain tail (re-route to full / failed txn) left _lastImage ahead of the stored chain, so the
  // NEXT delta diffed against a base that no longer matched the store (the W4 module-drop bug).
  _pendingImage: Uint8Array | null;
  _stdlibLoaded?: string[];
  _cellHostResults?: HostResult[];
  _replayHostResults?: HostResult[] | null;
  // host-callback bridge: a per-eval sender the DO installs (via setHostSender) so a
  // non-fetch host.<name> call can round-trip to the connected client over the WS.
  _hostSender?: HostCallSender | null;
  // fs provider handler: the DO (lib.rs) installs a JS async closure (R2/S3 servicer) per eval
  // when config.fs.provider != vfs; the engine's `host.__fs` effect routes here. null = in-heap VFS.
  _fsHandler?: ((payload: unknown) => Promise<unknown>) | null;
  // fetch-fence: deterministic (doId, cell) the DO installs per-eval so _doFetch derives a stable
  // Idempotency-Key per (session, cell, in-cell fetch ordinal). _curFetchSeq = ordinal within cell.
  _fenceDoId?: string;
  _fenceCell?: number;
  _curFetchSeq?: number;
  // TRUE STREAMING (#13): DO-side registry of open upstream stream readers/writers, keyed by a
  // deterministic streamId (`${doId}:${cell}:${ordinal}`). Lives in DO memory (GlueKernel
  // instance), NEVER in the heap blit — so it dies with the isolate at eviction, which IS the
  // StreamExpired-on-wake semantic. streamRead pulls one upstream chunk per call (backpressure).
  _streams?: Map<string, StreamState>;
  // OUTBOUND TCP/TLS (cloudflare:sockets): the live socket provider holds its Socket Map in
  // DO-instance memory (never snapshotted) — dies on eviction; the VM heap keeps only the integer
  // handleId token, which reads as ECONNRESET after a cold restore. Lazily constructed.
  _sockets?: ReturnType<typeof makeSocketHost>;
  // WEBSOCKET-CLIENT-IN-VM (#12): DO-side registry of live outbound WebSockets, keyed by a
  // deterministic handleId (`ws:${doId}:${cell}:${ordinal}`). Lives in DO memory (GlueKernel
  // instance), NEVER in the heap blit — so it dies with the isolate at eviction, which IS the
  // severed-on-wake semantic. The VM holds only a small JSON handle token (in globalThis.__wsHandles)
  // that snapshot-persists; recv() PULLS frames (immediate if queued, else parks until the socket
  // listener enqueues the next frame or the idle deadline fires).
  _ws?: Map<string, WsState>;

  // EXTENSIBILITY API — REDUCED Phase 1 (#32): client-backend tool registry resolved from
  // config.extensions in _applyConfig. Map "<extName>.<fn>" -> { ext, callsPerCell, maxResultBytes }.
  // A registered tool routes through the existing _clientHostCall bridge with a per-cell call cap +
  // result clamp. SECRET-FREE; no credentials in this phase (http/worker backends are rejected at
  // create in lib.rs). _extOps = per-(ext)-cell call counter (mirrors streamOps/wsOps), reset per cell.
  _extTools?: Map<string, { ext: string; callsPerCell: number; maxResultBytes: number }>;
  _extOps?: Map<string, number>;
  _aeExt?: (op: string) => void;

  // SANDBOX BRIDGE (additive host effect): config the DO installs per-eval (setSandboxConfig) so the
  // in-cell `host.sandbox.*` effect can DO-side fetch the engram-sandbox container worker. The URL +
  // Bearer key are read from the kernel ENV by lib.rs and NEVER cross into the VM heap — the cell only
  // ever sees the effect NAME (host.sandbox.exec); the key lives in DO-instance memory. _sandboxDoId is
  // the trusted KERNEL DO id (the R2 prefix `fs/<doId>/` the sandbox mounts as /session). Capability-
  // gated: _sandboxEnabled mirrors config.sandbox — without it every sandbox.* call returns
  // SandboxUnavailable.
  _sandboxUrl?: string | null;
  _sandboxKey?: string | null;
  _sandboxDoId?: string;
  _sandboxEnabled?: boolean;

  constructor() {
    this.inst = null;
    this.wasiCtl = null;
    this.config = {};
    this.clockSeed = 0;
    this.rngSeed = 42;
    this.tsEnabled = true; // default ON; resolved from config at create/restore
    this.lastTimings = {};
    this._fetchAllow = true; // resolved from config at create/restore
    // W4: the previous committed image, retained host-side (NOT in the VM heap), so the next
    // checkpoint can byte-diff against it. Dropped on evict; re-derived on cold wake (base+deltas).
    this._lastImage = null;
    this._pendingImage = null;
    this._hostSender = null;
  }

  // setHostSender: the DO installs (per eval) a sender that delivers a {t:hostcall} frame
  // to the connected client and a timeoutMs. Pass `null` to clear (e.g. facet/HTTP path
  // with no held client socket) so non-fetch host calls reject cleanly. `send` returns
  // false when no live socket is available.
  setHostSender(send: ((frameJson: string) => boolean) | null, timeoutMs: number): void {
    this._hostSender = send ? { send, timeoutMs: timeoutMs > 0 ? timeoutMs : 60000 } : null;
  }

  // setFsHandler: the DO installs (per eval) the R2/S3 fs servicer closure when
  // config.fs.provider != vfs. `null` clears it (in-heap VFS path; the engine never calls host.__fs).
  setFsHandler(handler: ((payload: unknown) => Promise<unknown>) | null): void {
    this._fsHandler = typeof handler === "function" ? handler : null;
  }

  // setFenceContext: the DO installs (per eval) the deterministic identity (doId, cell) used to
  // derive the host.fetch idempotency key. Replayed / re-run cells reproduce the same key.
  setFenceContext(doId: string, cell: number): void {
    this._fenceDoId = doId;
    this._fenceCell = cell;
  }

  // setExtAeSink: the DO installs (per eval) a sink that writes ONE AE datapoint per ext call
  // (op="ext:<name>"). The glue has no AE binding; lib.rs owns the AE path, so it passes a closure.
  // `null` clears it (facet/HTTP path or no AE binding).
  setExtAeSink(sink: ((op: string) => void) | null): void {
    this._aeExt = typeof sink === "function" ? sink : undefined;
  }

  // setSandboxConfig: the DO (lib.rs) installs (per eval) the engram-sandbox endpoint + Bearer key
  // (read from kernel ENV: ENGRAM_SANDBOX_URL / ENGRAM_SANDBOX_KEY) + the trusted KERNEL do_id + the
  // capability flag (config.sandbox). The VM NEVER sees the key — only the host.sandbox.* effect names
  // cross the engine boundary; the key is added DO-side in _doSandbox. Pass enabled=false (or url/key
  // null) to gate the route off (SandboxUnavailable). Mirrors setFsHandler / setFenceContext.
  setSandboxConfig(url: string | null, key: string | null, doId: string, enabled: boolean): void {
    this._sandboxUrl = url || null;
    this._sandboxKey = key || null;
    this._sandboxDoId = doId || "";
    this._sandboxEnabled = !!enabled;
  }

  // _seedExtMeta: EXTENSIBILITY API — REDUCED Phase 1 (#32) discoverability WITHOUT an engine edit.
  // Evals a glue-side snippet into the live VM (same eval-snippet mechanism as _seedStdlibMeta /
  // _applyFsProvider) that (a) installs globalThis.__extMeta = the SECRET-FREE manifest so an agent
  // can read the registered tools/params/examples; (b) for each ext, installs a namespace object
  // host.<name> = { <fn>: (...a)=>host["<name>.<fn>"](...a) } by REPLACING globalThis.host with a
  // wrapper Proxy that returns the namespace for an ext name and delegates EVERYTHING ELSE to the
  // original host Proxy (so the flat-name "<name>.<fn>" still reaches the BOOTSTRAP-captured
  // __HOSTCALL). Idempotent; re-applied on every create AND restore (a cold-woken VM re-runs
  // BOOTSTRAP but NOT this glue seed). Pure deterministic data write (zero entropy).
  _seedExtMeta(): void {
    try {
      const exts = Array.isArray(this.config.extensions) ? this.config.extensions : [];
      // SECRET-FREE manifest: names/versions/tools (fn/description/params/example). No backend/creds.
      const meta = exts.map((e) => ({
        name: e.name,
        version: e.version || "0.0.0",
        tools: (Array.isArray(e.tools) ? e.tools : []).map((t) => ({
          fn: t.fn,
          description: t.description || "",
          params: t.params ?? null,
          example: t.example ?? null,
        })),
      }));
      const names = exts.map((e) => e.name).filter((n) => typeof n === "string" && n);
      const ex = this._ex();
      const src =
        "(function(){var M=" + JSON.stringify(meta) + ";var NS=" + JSON.stringify(names) + ";" +
        "globalThis.__extMeta=M;" +
        "var nameSet={};for(var i=0;i<NS.length;i++)nameSet[NS[i]]=true;" +
        // capture the original host Proxy ONCE (re-seed must not re-wrap an already-wrapped host).
        "if(!globalThis.__hostBase)globalThis.__hostBase=globalThis.host;" +
        "var base=globalThis.__hostBase;" +
        "function mkNs(name){var o={};var tools=null;for(var k=0;k<M.length;k++)if(M[k].name===name)tools=M[k].tools;" +
        "if(tools)for(var j=0;j<tools.length;j++){(function(fn){o[fn]=function(){var a=Array.prototype.slice.call(arguments);return base[name+'.'+fn].apply(base,a);};})(tools[j].fn);}return o;}" +
        "var nsCache={};" +
        "globalThis.host=new Proxy(base,{get:function(t,prop){" +
        "if(typeof prop==='string'&&nameSet[prop]){if(!nsCache[prop])nsCache[prop]=mkNs(prop);return nsCache[prop];}" +
        "return base[prop];}});" +
        "globalThis.extensions=function(){return globalThis.__extMeta;};" +
        "})();0";
      const b = TEXT_ENC.encode(src);
      this._writeScratch(b);
      ex.eval_begin(ex.scratch_ptr(), b.length, BigInt(300000), 0);
    } catch { /* non-fatal: flat host["<name>.<fn>"] still works without the namespace sugar */ }
  }

  // _sanityProbe: post-restore integrity check before a hot-blitted session goes live. Runs a full
  // GC sweep (whole-heap structural walk; faults on a corrupt object header) then evals the snapshot
  // canary (exercises shape lookup, string, closure env, funcref dispatch). Returns false on any
  // fault/mismatch → the DO discards this instance and falls back to E6 oplog replay (always correct).
  // A canary-absent snapshot (pre-canary build) returns ok=true for back-compat.
  _sanityProbe(): boolean {
    try {
      const ex = this._ex();
      try { ex.run_gc(); } catch { return false; }
      const src = '(function(){try{var c=globalThis.__engram_canary; if(!c) return 1; return (c.s==="engram-canary"&&typeof c.f==="function"&&c.f()===43)?1:0;}catch(e){return 0;}})()';
      const bb = TEXT_ENC.encode(src);
      this._writeScratch(bb);
      const st = ex.eval_begin(ex.scratch_ptr(), bb.length, BigInt(1_000_000), 0);
      if (st !== 0) return false; // not DONE (fault / host-call / budget) => unsafe
      const res = JSON.parse(this._readResult());
      return !!(res && res.ok && res.value === 1);
    } catch {
      return false;
    }
  }

  // _applyFsProvider: tell the in-VM fs router which provider is active (it reads
  // globalThis.__fsProvider). Called after create/restore. 'vfs' = in-heap; else host-backed.
  _applyFsProvider(): void {
    const prov = (this.config.fs && this.config.fs.provider) || "vfs";
    try {
      const ex = this._ex();
      const src = "globalThis.__fsProvider=" + JSON.stringify(String(prov)) + ";0";
      const b = TEXT_ENC.encode(src);
      this._writeScratch(b);
      ex.eval_begin(ex.scratch_ptr(), b.length, BigInt(100000), 0);
    } catch { /* non-fatal: defaults to 'vfs' in the bootstrap */ }
  }

  // _seedStdlibMeta: stamp the baked STDLIB_META catalog (default set + opt-in + versions) into the
  // VM as globalThis.__stdlibMeta so the engine's __nodeCompat.modules getter + help() can report
  // which modules exist, their versions, and whether each is preloaded. Pure deterministic data
  // write (no entropy); idempotent; re-applied on every create AND restore so a cold-woken VM (which
  // re-runs BOOTSTRAP but NOT this glue-side seed) reports the catalog too.
  _seedStdlibMeta(): void {
    try {
      const m = globalThis.__STDLIB_META;
      if (!m) return;
      const payload = {
        all: Array.isArray(m.modules) ? m.modules : [],
        default: Array.isArray((m as { defaults?: string[] }).defaults) ? (m as { defaults: string[] }).defaults : [],
        optIn: Array.isArray(m.optIn) ? m.optIn : [],
        versions: m.versions || {},
      };
      const ex = this._ex();
      const src = "globalThis.__stdlibMeta=(" + JSON.stringify(payload) + ");0";
      const b = TEXT_ENC.encode(src);
      this._writeScratch(b);
      ex.eval_begin(ex.scratch_ptr(), b.length, BigInt(200000), 0);
    } catch { /* non-fatal: __nodeCompat.modules falls back to a static list */ }
  }

  _newInstance(rngSeed: number): WebAssembly.Instance {
    const { wasi, setMem } = makeWasi(rngSeed);
    const inst = new WebAssembly.Instance(ENGINE_MODULE(), {
      wasi_snapshot_preview1: wasi,
    });
    const exports = inst.exports as unknown as EngineExports;
    setMem(exports.memory);
    // WASI reactor init: call _initialize if present (no _start for reactors).
    if (typeof exports._initialize === "function") {
      try {
        exports._initialize();
      } catch { /* ignore */ }
    }
    this.wasiCtl = { wasi, setMem };
    return inst;
  }

  _ex(): EngineExports {
    return (this.inst as WebAssembly.Instance).exports as unknown as EngineExports;
  }

  // seed scalars from config (clock mode + rngSeed) — parity with JS kernel.
  _applyConfig(configJson: string): KernelConfig {
    let cfg: KernelConfig = {};
    try {
      cfg = JSON.parse(configJson || "{}");
    } catch { /* keep empty */ }
    this.config = cfg;
    this.rngSeed = ((cfg.rngSeed ?? 0) >>> 0) || 42;
    // clock seed: the engine's in-VM millisecond clock returns (CLOCK_EPOCH_BASE_MS + clockSeed +
    // tick). Seeded sessions (the default) use clockSeed=0 → epoch starts at 1.7e12 (Nov 2023),
    // +1ms/call, byte-identical across restore. config.clock==='real' seeds clockSeed from the DO's
    // REAL host-side wall clock at create so the in-VM Date/now() reflects the real year/time. It is
    // STILL frozen-in-turn (workerd freezes wall time within a turn) and ticks +1ms/call determinist-
    // ically — acceptable: the clock advances monotonically from a real base, not from 2023. Persisted
    // via configJson across cold restore (same path as rngSeed); the live clock value is also blitted
    // in the heap so a restore continues from where it was, and a re-create (replayJournal) re-seeds
    // from the then-current real epoch.
    if (cfg.clock === "real") {
      const realOffset = Date.now() - CLOCK_EPOCH_BASE_MS;
      // clamp to non-negative (engine clock_seed is u64); real epoch is always > base, but guard.
      this.clockSeed = realOffset > 0 ? realOffset : 0;
    } else {
      this.clockSeed = 0;
    }
    // TypeScript strip: default TRUE (stripping valid JS is a safe near no-op); {typescript:false}
    // disables. Persisted via configJson across cold restore (same path as clock/rngSeed).
    this.tsEnabled = cfg.typescript !== false;
    // fetch egress allowlist (EXPLICIT CONTRACT): config.fetch UNSET ⇒ ALLOW-ALL egress (the
    // default — host.fetch reaches any public URL, which is what use()/CDN loads rely on);
    // true ⇒ allow all; false ⇒ block all; [hosts] ⇒ hostname allowlist. A blocked host rejects
    // with FetchBlockedError (see _doFetch; socket stays alive). config can still RESTRICT egress.
    this._fetchAllow = cfg.fetch === undefined ? true : cfg.fetch;
    // EXTENSIBILITY API (#32): resolve the CLIENT-backend tool registry from config.extensions.
    // lib.rs create_critical already validated (client-only, name/shadow/size rules) + persisted in
    // meta.config, so this is re-resolved on EVERY create/restore (configJson round-trips through the
    // snapshot). Defensive: only register backend.kind==="client" tools; cap callsPerCell/maxResultBytes.
    this._resolveExtensions(cfg);
    return cfg;
  }

  _resolveExtensions(cfg: KernelConfig): void {
    const tools = new Map<string, { ext: string; callsPerCell: number; maxResultBytes: number }>();
    const exts = Array.isArray(cfg.extensions) ? cfg.extensions : [];
    for (const e of exts) {
      if (!e || typeof e !== "object") continue;
      const name = typeof e.name === "string" ? e.name : "";
      if (!name) continue;
      const kind = (e.backend && e.backend.kind) || "client";
      if (kind !== "client") continue; // defense-in-depth: lib.rs already rejects non-client at create
      const lim = e.limits || {};
      const cpc = (lim.callsPerCell ?? 0) | 0;
      const mrb = (lim.maxResultBytes ?? 0) | 0;
      const callsPerCell = Math.max(1, Math.min(EXT_MAX_CALLS_PER_CELL, cpc || EXT_MAX_CALLS_PER_CELL));
      const maxResultBytes = Math.max(1, Math.min(EXT_MAX_RESULT_BYTES, mrb || EXT_MAX_RESULT_BYTES));
      const list = Array.isArray(e.tools) ? e.tools : [];
      for (const t of list) {
        if (!t || typeof t.fn !== "string" || !t.fn) continue;
        tools.set(name + "." + t.fn, { ext: name, callsPerCell, maxResultBytes });
      }
    }
    this._extTools = tools;
  }

  async createFresh(configJson: string): Promise<string> {
    const cfg = this._applyConfig(configJson);
    this.inst = this._newInstance(this.rngSeed);
    this._ex().create(BigInt(this.clockSeed), BigInt(this.rngSeed));
    // stdlib injection: eval the selected esbuilt IIFEs into the live VM so they snapshot-
    // persist (survive hibernation, no re-inject). Subset chosen by config.modules.
    this._injectStdlib(cfg.modules);
    this._applyFsProvider();
    this._seedStdlibMeta();
    this._seedExtMeta();
    this.lastTimings = { instantiateMs: 0, growCount: 0 };
    return "fresh";
  }

  _injectStdlib(cfgModules: boolean | "default" | string[] | undefined): void {
    this._stdlibLoaded = [];
    const modules = resolveStdlibModules(cfgModules);
    if (!modules.length) return;
    const bundle = stdlibBundle();
    // V0.7 GUARD 1: combined source cap (avoid the snapshot OOM cliff).
    let total = 0;
    for (const n of modules) { const s = bundle[n]; if (typeof s === "string") total += s.length; }
    if (total > MAX_STDLIB_SOURCE_BYTES) {
      throw new Error("SizeAdmissionError: selected stdlib source " + total + " > MAX_STDLIB_SOURCE_BYTES " +
        MAX_STDLIB_SOURCE_BYTES + " (modules: " + modules.join(",") + ")");
    }
    const ex = this._ex();
    for (const name of modules) {
      const iife = bundle[name];
      if (typeof iife !== "string") continue;
      try {
        // eval the IIFE in GLOBAL scope; it self-installs globalThis.<name>. Then REGISTER it into
        // globalThis.__stdmods[name] (the require-ONLY preloaded-stdlib map the engine require()
        // checks after __builtins+__mods) so require('<name>') resolves the curated bundle. We do
        // NOT touch __mods — that is use()'s ESM cache, and registering a bare-function global
        // (nanoid) there would make use('nanoid') return the function instead of the {nanoid}
        // namespace. __stdmods keeps require() working while leaving use()'s real ESM path intact.
        // Map common name->global-var mismatches (js-yaml->jsyaml etc.). preloaded flags in
        // __nodeCompat.modules read both __mods and __stdmods (engine getter updated).
        const nameJson = JSON.stringify(name);
        const wrapped =
          "(0,eval)(" + JSON.stringify(iife) + ");" +
          "(function(){var __n=" + nameJson + ";globalThis.__stdmods=globalThis.__stdmods||{};" +
          "var g=globalThis[__n];if(g===undefined){var alt=__n.replace(/-/g,'');g=globalThis[alt];}" +
          "if(g!==undefined&&globalThis.__stdmods[__n]===undefined)globalThis.__stdmods[__n]=g;})();0";
        const srcBytes = TEXT_ENC.encode(wrapped);
        this._writeScratch(srcBytes);
        const st = ex.eval_begin(ex.scratch_ptr(), srcBytes.length, BigInt(5_000_000), 0);
        // stdlib injection makes no host calls; if it parked, abandon (shouldn't happen).
        if (st === STATUS_HOST_CALL) continue;
        const res = JSON.parse(this._readResult());
        if (res.ok !== false) (this._stdlibLoaded as string[]).push(name);
      } catch { /* skip a failed module, never fatal */ }
    }
  }

  // restore: blit the gz image into a fresh instance, then re-seed counters + kv.
  async restore(
    gz: Uint8Array,
    engineHash: string,
    clockCalls: number,
    rngCalls: number,
    configJson: string,
    label: string,
    kvJson: string,
    usedHeap: number,
    baseCodec?: string
  ): Promise<string> {
    this._applyConfig(configJson);
    const t0 = Date.now();
    // decompress the snapshot image by its recorded codec (zstd | gzip — back-compat). Issue #9.
    const raw = await codecDecompress(gz, baseCodec);
    const gunzipMs = Date.now() - t0;
    // W5 RESTORE admission: a spiked-then-freed image gunzips back to the full (zeroed) raw
    // extent. Admit on the RECORDED used heap (the genuine fence); the raw ceiling only fails
    // images too big to safely instantiate. docs/W5-COMPACTION-PLAN.md.
    const recordedUsed = Number.isFinite(usedHeap) && usedHeap > 0 ? usedHeap | 0 : 0;
    if (recordedUsed > MAX_RESTORE_USED_BYTES) {
      throw new Error(
        "SizeAdmissionError: recorded used heap " + recordedUsed +
          "B > MAX_RESTORE_USED_BYTES " + MAX_RESTORE_USED_BYTES + "; refusing restore"
      );
    }
    if (raw.length > MAX_RESTORE_RAW_BYTES) {
      throw new Error(
        "SizeAdmissionError: restore raw image " + raw.length +
          "B > MAX_RESTORE_RAW_BYTES " + MAX_RESTORE_RAW_BYTES + "; refusing restore"
      );
    }
    // fresh instance, grow to fit, blit.
    this.inst = this._newInstance(this.rngSeed);
    const mem = this._ex().memory;
    const needPages = (raw.length + 0xffff) >> 16;
    const havePages = mem.buffer.byteLength >> 16;
    let growCount = 0;
    if (needPages > havePages) {
      mem.grow(needPages - havePages);
      growCount = 1;
    }
    new Uint8Array(this._ex().memory.buffer).set(raw);
    // re-validate the blitted runtime + restore entropy counters + kv state.
    const ok = this._ex().reattach();
    if (!ok) throw new Error("RestoreError: reattach failed after blit");
    this._ex().set_counters(BigInt(clockCalls | 0), BigInt(rngCalls | 0));
    if (kvJson && kvJson !== "{}") this._importKv(kvJson);
    // W4: retain the reconstructed image so the next warm checkpoint diffs against it.
    // BUG-1 FIX (restore triple-buffer): `raw` is a standalone decompressed array we own and which
    // nothing mutates after the blit above, so retain it DIRECTLY instead of `raw.slice()`. The old
    // slice was a 3rd full-size transient copy (instance + raw + raw.slice) that crossed the OOM
    // cliff on large images; dropping it halves the extra transient peak.
    this._lastImage = raw;
    this._applyFsProvider();
    this._seedStdlibMeta();
    this._seedExtMeta();
    this.lastTimings = { gunzipMs, instantiateMs: 0, growCount, neededPages: needPages };
    return label || "sqlite-restore";
  }

  // W4 restore: reconstruct the raw image from a gz'd full base + an ordered chain of gz'd
  // deltas, then blit (same path as restore()). deltaList = [{gz, indicesGz, grain}] applied
  // in order. docs/W4-BYTEDELTA-PLAN.md.
  async restoreW4(
    baseGz: Uint8Array,
    deltaList: W4Delta[],
    engineHash: string,
    clockCalls: number,
    rngCalls: number,
    configJson: string,
    label: string,
    kvJson: string,
    usedHeap: number,
    expectCrc: number,
    expectLen: number,
    baseCodec?: string
  ): Promise<string> {
    this._applyConfig(configJson);
    const t0 = Date.now();
    // Decompress the base by its recorded codec (zstd | gzip). expectLen (the full reconstructed
    // image length) is NOT the base length here (deltas extend it), so let zstd read its own frame
    // content size. Issue #9.
    const base = await codecDecompress(baseGz, baseCodec);
    let image = base; // grown + mutated by deltas
    const deltas = Array.isArray(deltaList) ? deltaList : [];
    // Pre-decode each delta so we can size the reconstructed image to the LARGEST grain target
    // across the chain (the buffer grows monotonically, so a later delta may extend past the base).
    const decoded: Array<{ payload: Uint8Array; dv: DataView; grain: number; nIdx: number }> = [];
    let maxLen = image.byteLength;
    for (const d of deltas) {
      // each delta blob carries its OWN codec (a chain can straddle a deploy boundary). Issue #9.
      const payload = await codecDecompress(d.gz, d.codec, d.payloadLen);
      const idxBytes = await codecDecompress(d.indicesGz, d.codec, d.indicesLen);
      const grain = d.grain || DELTA_GRAIN_BYTES;
      const nIdx = (idxBytes.byteLength / 4) | 0;
      const dv = new DataView(idxBytes.buffer, idxBytes.byteOffset, idxBytes.byteLength);
      let dMax = 0;
      for (let k = 0; k < nIdx; k++) { const gi = dv.getUint32(k * 4, true); if (gi > dMax) dMax = gi; }
      if (nIdx > 0) maxLen = Math.max(maxLen, (dMax + 1) * grain);
      decoded.push({ payload, dv, grain, nIdx });
    }
    if (maxLen > image.byteLength) {
      const grown = new Uint8Array(maxLen);
      grown.set(image);
      image = grown;
    }
    for (const d of decoded) {
      const { payload, dv, grain, nIdx } = d;
      for (let k = 0; k < nIdx; k++) {
        const gi = dv.getUint32(k * 4, true);
        const dst = gi * grain;
        const srcStart = k * grain;
        const n = Math.min(grain, image.byteLength - dst);
        if (n > 0) image.set(payload.subarray(srcStart, srcStart + n), dst);
      }
    }
    const gunzipMs = Date.now() - t0;
    // W4 RECONSTRUCTION CHECKSUM (defense-in-depth, task #15): the DO carries the dump's
    // reconstructed-image CRC32 in the manifest. Recompute over the bytes the chain produced and
    // reject a mismatch BEFORE blitting => the DO's catch falls back to oplog replay (always
    // correct), converting any silent W4 desync into a caught, recoverable fallback. expectLen<=0
    // (older snapshots) or expectCrc===0 skips the check (backward-compatible).
    const wantLen = Number.isFinite(expectLen) && expectLen > 0 ? expectLen | 0 : 0;
    const wantCrc = (expectCrc >>> 0);
    if (wantLen > 0 && wantCrc !== 0) {
      if (image.byteLength < wantLen) {
        throw new Error("RestoreSanityError: reconstructed image " + image.byteLength +
          "B shorter than recorded " + wantLen + "B (W4 chain incomplete)");
      }
      const got = crc32(image.subarray(0, wantLen));
      if (got !== wantCrc) {
        throw new Error("RestoreSanityError: W4 reconstruction CRC mismatch (got " + got +
          " want " + wantCrc + ", len " + wantLen + ") — chain desync, falling back to oplog replay");
      }
    }
    // W5 RESTORE admission on the reconstructed image.
    const recordedUsed = Number.isFinite(usedHeap) && usedHeap > 0 ? usedHeap | 0 : 0;
    if (recordedUsed > MAX_RESTORE_USED_BYTES)
      throw new Error("SizeAdmissionError: recorded used heap " + recordedUsed + " > " + MAX_RESTORE_USED_BYTES);
    if (image.length > MAX_RESTORE_RAW_BYTES)
      throw new Error("SizeAdmissionError: restore raw image " + image.length + " > " + MAX_RESTORE_RAW_BYTES);

    this.inst = this._newInstance(this.rngSeed);
    const mem = this._ex().memory;
    const needPages = (image.length + 0xffff) >> 16;
    const havePages = mem.buffer.byteLength >> 16;
    let growCount = 0;
    if (needPages > havePages) { mem.grow(needPages - havePages); growCount = 1; }
    new Uint8Array(this._ex().memory.buffer).set(image);
    const ok = this._ex().reattach();
    if (!ok) throw new Error("RestoreError: reattach failed after W4 blit");
    this._ex().set_counters(BigInt(clockCalls | 0), BigInt(rngCalls | 0));
    if (kvJson && kvJson !== "{}") this._importKv(kvJson);
    // BUG-1 FIX (restore triple-buffer): `image` is a standalone reconstructed array we own and
    // which nothing mutates after the blit; retain it DIRECTLY instead of `image.slice()` to drop
    // the redundant 3rd full-size transient copy.
    this._lastImage = image;
    this._applyFsProvider();
    this._seedStdlibMeta();
    this._seedExtMeta();
    if (!this._sanityProbe()) {
      throw new Error("RestoreSanityError: post-blit canary/GC probe failed (possible image corruption)");
    }
    this.lastTimings = { gunzipMs, instantiateMs: 0, growCount, neededPages: needPages, deltas: deltas.length };
    return label || "sqlite-restore";
  }

  lastRestoreTimings(): string {
    return JSON.stringify(this.lastTimings || {});
  }

  // E6: the host requests+results serviced during the last eval (for the crash-tail oplog).
  lastCellHostResults(): string {
    try { return JSON.stringify(this._cellHostResults || []); } catch { return "[]"; }
  }

  resultArtifactChunk(id: string, offset: number, len: number): string {
    try {
      const ex = this._ex();
      const idBytes = TEXT_ENC.encode(id || "text");
      this._writeScratch(idBytes);
      ex.result_artifact_chunk(ex.scratch_ptr(), idBytes.length, Math.max(0, offset | 0), Math.max(0, len | 0));
      return this._readResult();
    } catch (e) {
      const err = e as { name?: string; message?: string };
      return JSON.stringify({
        ok: false,
        t: "artifact",
        error: {
          name: (err && err.name) || "ArtifactError",
          message: String((err && err.message) || e),
        },
      });
    }
  }

  // evalCode: ASYNC. Drives the engine eval_begin/eval_resume loop, servicing host effects
  // (host.fetch -> DO-side fetch). Returns the rich JSON result STRING and NEVER throws across
  // the boundary (so the eval mutex is always released).
  async evalCode(src: string): Promise<string> {
    // TS-STRIP (runs BEFORE transformCell): erase TS type syntax to whitespace. Length-preserving,
    // so the depth-0 declaration tokenizer in transformCell sees identical offsets. On an
    // un-erasable construct (enum / namespace / parameter-properties) reject the cell with a typed
    // TypeScriptError mirroring the eval error envelope — socket alive, mutex released, next eval
    // works — do NOT crash. Disabled when config {typescript:false}.
    let tsOut: string;
    try {
      tsOut = this.tsEnabled ? stripTypes(src) : src;
    } catch (e) {
      const err = e as { name?: string; message?: string };
      return JSON.stringify({
        ok: false,
        valueType: "error",
        logs: [],
        error: { name: err.name || "TypeScriptError", message: String(err.message || e), stack: "" },
      });
    }
    try {
      const ex = this._ex();
      // CLOCK:REAL RE-ANCHOR (#7) — at the START of each eval, in clock:real mode ONLY, re-seed the
      // in-VM monotonic CLOCK to the DO's REAL wall-clock so Date.now() reflects real inter-cell
      // elapsed time (not just +1ms per call). The engine's __now returns 1.7e12 + CLOCK, so set
      // CLOCK = realNow - 1.7e12. workerd freezes the clock IN-turn, so within a cell the clock keeps
      // its +1ms-per-call monotonic step (unavoidable). We NEVER move the clock backwards: if real
      // wall time somehow reads below the current VM clock (clock skew / replay), keep the higher VM
      // value (monotonic guarantee preserved). SEEDED (default) mode is untouched -> still byte-
      // identical/deterministic across restore. DETERMINISM BREAK is intentional + documented for
      // clock:real: a real wall-clock base is, by definition, non-reproducible across runs.
      if (this.config.clock === "real") {
        try {
          const realNow = Date.now();
          const target = realNow - CLOCK_EPOCH_BASE_MS;             // desired CLOCK so next __now == realNow
          // current VM clock value = base offset + ticks-so-far; clock_calls() == number of +1ms steps.
          const curClock = this.clockSeed + Number(ex.clock_calls());
          // monotonic: only advance. If real time is behind the VM clock, hold the VM clock.
          const next = target > curClock ? target : curClock;
          if (next >= 0) {
            ex.set_clock(BigInt(next));
            // keep this.clockSeed coherent so a subsequent within-process read (and the snapshot's
            // recorded seed) reflects the re-anchored base: seed = CLOCK - ticks-so-far.
            this.clockSeed = next - Number(ex.clock_calls());
          }
        } catch { /* engine lacks set_clock (older build) -> leave clock as-is */ }
      }
      // REPL-persistence transform: rewrite top-level let/const/function/class declarations into
      // global assignments so they persist across cells (Node-REPL semantics), preserving the
      // completion value. Pure deterministic pre-eval source rewrite; falls back to the ORIGINAL
      // source on any ambiguity (never corrupts the cell). See src/repl-transform.ts.
      let transformed = tsOut;
      try { transformed = transformCell(tsOut); } catch { transformed = tsOut; }
      // REPL completion value for top-level-await cells: the engine runs an await-using
      // multi-statement cell as an async function BODY (no completion value), so a trailing
      // expression after a loop/block is otherwise lost. Rewrite it into an explicit `return`.
      // Bails to the input on any ambiguity (no regression for non-await / single-expr cells).
      try { transformed = wrapAsyncCompletion(transformed); } catch { /* keep transformed */ }
      const srcBytes = TEXT_ENC.encode(transformed);
      // (a) PROTOCOL/SOURCE size guard: reject an oversized cell source BEFORE writing it into the
      // scratch buffer. Reserve the DYNAMIC scratch to fit first (grows to the 32MB ceiling), then
      // read the cap — if the source still exceeds it (i.e. > ceiling) reject with a typed
      // ProtocolSizeError (socket alive, mutex released, next eval works).
      ex.scratch_reserve(srcBytes.length);
      const cap = ex.scratch_cap();
      if (srcBytes.length > cap) {
        return JSON.stringify({
          ok: false,
          valueType: "error",
          logs: [],
          error: {
            name: "ProtocolSizeError",
            message: "cell source " + srcBytes.length + "B exceeds max " + cap + "B",
            stack: "",
          },
        });
      }
      this._writeScratch(srcBytes);
      // per-cell guards: interrupt-tick budget + per-cell buffer-growth cap (pages). The budget
      // counts INTERRUPT-HANDLER invocations (QuickJS fires the handler every ~Nk bytecodes), so
      // the certified value is ~1200 (parity with the JS kernel's default 1200 / cap 2000).
      // Per-cell instruction budget (interrupt-handler ticks; the timeout fence). Default raised
      // 1200 -> 500000: decoding a large fetch body (base64 -> bytes) inside the VM burns far more
      // bytecode than a normal cell, and the old 1200 tripped a TimeoutError before a big-payload
      // cell (e.g. fetching + writing a 2.2MB PDF) could finish (~150k ticks observed for 2.2MB).
      // Memory bombs are still caught by the grow-cap tripwire + rquickjs heap limit; the platform
      // also bounds wall-clock CPU. Overridable per-session via config.cellBudgetTicks.
      // ENFORCEABILITY NOTE (config.cellBudgetTicks, the TIMEOUT fence): the budget counts
      // INTERRUPT-HANDLER invocations, which QuickJS fires every ~Nk bytecodes. workerd THROTTLES the
      // host interrupt callback after ~1.6k invocations PER TURN, so the tick budget can only reliably
      // fence a cell that keeps STEPPING BYTECODE past that floor — i.e. the budget reliably trips
      // bytecode-heavy work (a big base64 decode of a fetch body burns ~150k ticks for 2.2MB, which is
      // exactly why the default is 500000, NOT ~1500). A truly TIGHT CPU loop (e.g. `for(;;)x+=i`)
      // outruns the throttled callback and is NOT killed by this budget — it falls to the platform 30s
      // CPU limit and ends with a WS-1006 (recover via reconnect or .reset). We do NOT lower the default
      // (that would false-trip legitimate big-payload cells, reopening the big-payload regression that
      // motivated 1200->500000). We DO clamp the configured value to a sane bounded range so a
      // negative/NaN/absurd config can't disable the fence or overflow. A deeper fix (a real CPU-time
      // deadline) is moot here: workerd freezes the wall clock in-turn, so there is no in-turn clock to
      // deadline against. See docs/results/v0.2.md (infinite-loop hole) + the v0.8 mid-cell tripwire.
      const CELL_BUDGET_TICKS_MIN = 1;
      const CELL_BUDGET_TICKS_DEFAULT = 500000;
      const CELL_BUDGET_TICKS_MAX = 5_000_000; // documented ceiling: above this the budget adds no real
      // fence (the workerd interrupt throttle dominates) and only delays a clean reject; bound it.
      const rawBudget = this.config.cellBudgetTicks;
      const budgetN = Number.isFinite(rawBudget) && (rawBudget as number) > 0
        ? Math.min(Math.max(Math.floor(rawBudget as number), CELL_BUDGET_TICKS_MIN), CELL_BUDGET_TICKS_MAX)
        : CELL_BUDGET_TICKS_DEFAULT;
      const budget = BigInt(budgetN);
      // Per-cell linear-memory growth cap (pages, 64KB each). Default raised 128p/8MB -> 1024p/64MB:
      // a single cell may legitimately pull in a fetch body up to FETCH_MAX_BODY_BYTES (32MB) and
      // then hold a working copy or two (decoded bytes, an fs write) while it runs — the old 8MB cap
      // tripped a MemoryLimitError before such a cell could finish (e.g. a 6.6MB PDF). The rquickjs
      // heap limit (set_memory_limit, 64MB in the engine) and the snapshot size-admission guard
      // remain the hard OOM fences; this tripwire just catches runaway per-cell growth.
      const growCapPages = (this.config.cellGrowCapPages ?? 0) || 1024;
      let status = ex.eval_begin(ex.scratch_ptr(), srcBytes.length, budget, growCapPages);
      let guard = 0;
      // host-call loop cap: bound the host calls a single cell may make (re-entrant cells
      // can chain many). Configurable via config.maxHostCallsPerCell; default 64.
      const maxHostCalls = (this.config.maxHostCallsPerCell ?? 0) || 64;
      // TRUE STREAMING (#13): stream chunk pulls (streamRead/streamWrite) have their OWN large budget
      // — a 100MB body at 256KB chunks is ~400 pulls, far past the 64 non-stream guard. Both counters
      // still bound the loop so the pump can never spin forever. Default 4096 (~1GB at 256KB/chunk).
      const maxStreamOps = (this.config.maxStreamOpsPerCell ?? 0) || 4096;
      let streamOps = 0;
      // WEBSOCKET-CLIENT-IN-VM (#12): ws.* effects (a pump cell makes many recv/poll calls) get their
      // OWN large budget, like stream ops — so a chatty WS pump is not capped by the 64 host-call
      // guard. Each parked recv still has the idle deadline so the cell can never hang forever.
      const maxWsOps = (this.config.maxWsOpsPerCell ?? 0) || WS_MAX_OPS_PER_CELL;
      let wsOps = 0;
      // HOST-BACKED FS (#18): a single host.fs read/write of a body > 64KB is CHUNKED into many
      // host.__fs calls (each chunk crosses the fixed 64KB HOSTCALL request buffer). Like stream/ws
      // ops, these get their OWN large budget so a multi-MB file is not capped by the 64 host-call
      // guard (a 5MB file at 32KB/chunk is ~160 calls). Bounded so the pump can never spin forever.
      const maxFsOps = (this.config.maxFsOpsPerCell ?? 0) || 8192;
      let fsOps = 0;
      // E6 oplog: capture host requests+results for the crash-tail journal (host-side).
      this._cellHostResults = [];
      // EXTENSIBILITY API (#32): reset the per-cell ext-call counter (mirrors streamOps/wsOps).
      this._extOps = new Map<string, number>();
      while (status === STATUS_HOST_CALL && guard <= maxHostCalls && streamOps <= maxStreamOps && wsOps <= maxWsOps && fsOps <= maxFsOps) {
        const req = this._readHostCall();
        // socket.read/write are the per-chunk TCP pump ops; budget them with streams so a long
        // socket transfer is not capped by the 64 host-call guard (open/startTls/close stay guarded).
        const isStreamOp = req.name === "streamRead" || req.name === "streamWrite" || req.name === "socket.read" || req.name === "socket.write";
        const isWsOp = req.name === "ws.recv" || req.name === "ws.poll" || req.name === "ws.send";
        const isFsOp = req.name === "__fs";
        if (isStreamOp) streamOps++; else if (isWsOp) wsOps++; else if (isFsOp) fsOps++; else guard++;

        let res: HostResult;
        if (this._replayHostResults && this._replayHostResults.length) {
          // engine-migration replay: feed the recorded result, do NOT re-fire the effect.
          res = this._replayHostResults.shift() as HostResult;
        } else {
          this._curFetchSeq = this._cellHostResults.length;
          res = await this._serviceHostCall(req);
        }
        this._cellHostResults.push(res);
        const resBytes = TEXT_ENC.encode(JSON.stringify(res));
        this._writeScratch(resBytes);
        status = ex.eval_resume(ex.scratch_ptr(), resBytes.length);
      }
      return this._readResult();
    } catch (e) {
      const err = e as { name?: string; message?: string };
      return JSON.stringify({
        ok: false,
        valueType: "error",
        logs: [],
        error: { name: (err && err.name) || "GlueError", message: String((err && err.message) || e), stack: "" },
      });
    } finally {
      // DYNAMIC scratch: release the (possibly grown) buffer back to the 1MB floor after EVERY cell
      // (success or error), so a big-payload cell does not leave this kernel resident-large for the
      // rest of its life — the key to keeping idle/parked kernels small in the conjoined isolate.
      try { this._ex().scratch_release(); } catch { /* engine not ready / torn down */ }
      // TRUE STREAMING (#13): sweep any stream readers NOT held past cell end (no `global` flag) so
      // an abandoned/un-cancelled reader does not leak the upstream connection. A stream referenced
      // by persistent global state keeps its entry (warm) and only dies at eviction (StreamExpired).
      this._sweepStreams();
    }
  }

  // _sweepStreams: drop FINISHED stream entries (reader hit done, or cancelled). OPEN streams are
  // kept warm across cells (DO-instance lifetime) so a `for await` that spans cells works while the
  // DO stays resident; they die naturally with the isolate at eviction -> StreamExpired on wake.
  // Called in the eval finally so a fully-consumed stream does not linger.
  _sweepStreams(): void {
    const m = this._streams;
    if (!m || m.size === 0) return;
    for (const [id, st] of Array.from(m.entries())) {
      if (st.done) m.delete(id);
    }
  }

  // host effects the ENGINE cannot do from wasip1: fetch (allowlisted). The RLM surface
  // (host.subLM / host.ctx.* / host.final / host.finalVar) and host.kv were removed.
  // Lazily construct the outbound TCP/TLS provider (one per DO instance; holds live Sockets in
  // memory, never snapshotted).
  _sock(): ReturnType<typeof makeSocketHost> {
    return (this._sockets ||= makeSocketHost());
  }
  // Soft-wrap a socket provider call: the provider returns {ok:true,value}/{ok:false,error,code};
  // the engine rejects the VM promise on ok:false, but the VM net.js shim expects a RESOLVED value
  // carrying `.error`. So always resolve ok:true, surfacing failures as inner {error,code}.
  async _socketCall(p: Promise<{ ok: boolean; value?: unknown; error?: string; code?: string }>): Promise<HostResult> {
    const r = await p;
    if (r && r.ok) return { ok: true, value: r.value };
    return { ok: true, value: { error: (r && r.error) || "socket error", code: r && r.code } };
  }
  // Extract the destination hostname from the many shapes net/tls pass: "host:port",
  // "tcp://host:port", "[ipv6]:port", or { host|hostname, port }. Empty string if undeterminable.
  _socketHostOf(addr: unknown, opts: unknown): string {
    const o = opts as { host?: string; hostname?: string } | undefined;
    if (o && (o.host || o.hostname)) return String(o.host || o.hostname);
    if (addr && typeof addr === "object") {
      const a = addr as { host?: string; hostname?: string };
      if (a.host || a.hostname) return String(a.host || a.hostname);
    }
    if (typeof addr === "string") {
      const s = addr;
      if (s.includes("://")) { try { return new URL(s).hostname; } catch { return ""; } }
      if (s.startsWith("[")) { const e = s.indexOf("]"); return e > 0 ? s.slice(1, e) : ""; }
      return s.split(":")[0];
    }
    return "";
  }
  // socket.open with the SAME egress posture as fetch/ws: SSRF block (private/internal/metadata)
  // + config.fetch allowlist. A blocked host resolves as a soft {error} (net.js emits ECONNREFUSED-
  // shaped 'error', socket stays alive) — never a silent raw-TCP escape to internal addresses.
  async _socketOpen(addr: unknown, opts: unknown): Promise<HostResult> {
    const host = this._socketHostOf(addr, opts);
    if (!host) return { ok: true, value: { error: "SocketError: could not determine destination host", code: "EINVAL" } };
    if (isBlockedSsrfHost(host)) return { ok: true, value: { error: "FetchBlockedError: " + host + " is a blocked private/internal address", code: "EACCES" } };
    const allow = this._fetchAllow;
    const permitted = allow === true ? true : allow === false ? false : Array.isArray(allow) ? allow.includes(host) : false;
    if (!permitted) return { ok: true, value: { error: "FetchBlockedError: " + host + " not allowed", code: "EACCES" } };
    return this._socketCall(this._sock().open(addr, opts));
  }
  async _serviceHostCall(req: HostCall): Promise<HostResult> {
    const name = req.name;
    const args = req.args || [];
    // `fetch` is the built-in DO-side host effect (allowlisted). Everything else is a
    // CLIENT host-callback: round-trip to the connected client over the WS.
    if (name === "fetch") return this._doFetch(args[0] as string, args[1] as RequestInit | undefined);
    if (name === "fetchStream") return this._doFetchStream(args[0] as string, args[1] as RequestInit | undefined);
    if (name === "streamRead") return this._doStreamRead(args[0] as string);
    if (name === "streamCancel") return this._doStreamCancel(args[0] as string);
    if (name === "streamWrite") return this._doStreamWrite(args[0] as string, args[1] as string | null, !!args[2]);
    // WEBSOCKET-CLIENT-IN-VM (#12): the VM PULLS frames over these five effects; the live socket
    // is DO-memory only (never snapshotted). recv() may PARK (resolved by the socket listener).
    if (name === "ws.open") return this._wsOpen(args[0] as string, args[1] as string[] | undefined);
    if (name === "ws.send") return this._wsSend(args[0] as string, args[1] as string | null, !!args[2]);
    if (name === "ws.recv") return this._wsRecv(args[0] as string, (args[1] as number) | 0, args[2] as number | undefined);
    if (name === "ws.poll") return this._wsPoll(args[0] as string, (args[1] as number) | 0);
    if (name === "ws.close") return this._wsClose(args[0] as string, args[1] as number | undefined, args[2] as string | undefined);
    // OUTBOUND TCP/TLS (cloudflare:sockets): the VM-side net/tls shims PULL bytes over these five
    // effects; the live Socket is DO-memory only (never snapshotted). The host provider returns its
    // own {ok,value}/{ok:false,error} envelope; the engine resolves p.value on ok and REJECTS the VM
    // promise on !ok — but the net.js shim checks `r.error` on a RESOLVED value, so we soft-wrap:
    // every socket.* failure resolves as inner {error,code} (engine sees ok:true, VM sees r.error).
    if (name === "socket.open") return this._socketOpen(args[0], args[1]);
    if (name === "socket.write") return this._socketCall(this._sock().write(args[0], args[1] as string, !!args[2]));
    if (name === "socket.read") return this._socketCall(this._sock().read(args[0]));
    if (name === "socket.startTls") return this._socketCall(this._sock().startTls(args[0]));
    if (name === "socket.close") return this._socketCall(this._sock().close(args[0]));
    // SANDBOX BRIDGE (additive): `host.sandbox.*` is a DO-side host effect (like fetch) that proxies
    // to the engram-sandbox container worker over the SHARED R2 VFS (the sandbox mounts `fs/<doId>/`
    // as /session). Capability-gated by config.sandbox; the Bearer key is added DO-side, never in-VM.
    if (name === "sandbox.exec" || name.startsWith("sandbox.")) return this._doSandbox(name, args);
    // host-backed fs (config.fs.provider != vfs): serviced DO-side via the installed _fsHandler.

    if (name === "__fs") return this._serviceFs(args[0] as Record<string, unknown>);
    // EXTENSIBILITY API — REDUCED Phase 1 (#32): a registered "<extName>.<fn>" client-backend tool
    // routes through the SAME client bridge (_clientHostCall) as any non-fetch host effect, but with a
    // per-cell call cap (limits.callsPerCell) + a result-byte clamp (limits.maxResultBytes) + one AE
    // datapoint (op="ext:<name>"). Deny-by-default: an UNREGISTERED "<x>.<y>" falls through to the raw
    // _clientHostCall path (which rejects when no client handler answers) — same as before.
    const ext = this._extTools && this._extTools.get(name);
    if (ext) {
      const ops = this._extOps || (this._extOps = new Map<string, number>());
      const used = (ops.get(ext.ext) || 0) + 1;
      ops.set(ext.ext, used);
      if (used > ext.callsPerCell) {
        return { ok: false, error: "ExtCallLimitError: ext '" + ext.ext + "' exceeded callsPerCell " + ext.callsPerCell };
      }
      try { if (this._aeExt) this._aeExt("ext:" + ext.ext); } catch { /* AE best-effort */ }
      const res = await this._clientHostCall(name, args);
      // RESULT CLAMP: keep the value within maxResultBytes so the oplog row never blows past
      // SQLITE_MAX_VALUE_BYTES. Over-budget -> replace with a typed marker (recorded deterministically).
      if (res && res.ok) {
        let vbytes = 0;
        try { vbytes = TEXT_ENC.encode(JSON.stringify(res.value ?? null)).length; } catch { vbytes = ext.maxResultBytes + 1; }
        if (vbytes > ext.maxResultBytes) {
          return { ok: true, value: { __extResultClamped: true, ext: ext.ext, tool: name, bytes: vbytes, maxResultBytes: ext.maxResultBytes } };
        }
      }
      return res;
    }
    return this._clientHostCall(name, args);
  }

  // _doSandbox: DO-side host effect for the in-cell `host.sandbox.*` surface. Proxies to the
  // engram-sandbox container worker (FIXED trusted URL -> no SSRF allowlist needed) carrying the
  // trusted KERNEL do_id (x-engram-session => the R2 prefix `fs/<doId>/` the sandbox mounts as
  // /session) + the Bearer key (added HERE, never visible to the VM). Capability-gated by
  // config.sandbox: without it (or without a configured url/key) -> SandboxUnavailable. Returns
  // {ok:true,value:json} on a 2xx, {ok:false,error} otherwise. Mirrors _doFetch's posture; the key
  // and url live in DO-instance memory only (never in the heap blit, never in any client frame).
  async _doSandbox(name: string, args: unknown[]): Promise<HostResult> {
    if (!this._sandboxEnabled) {
      return { ok: false, error: "SandboxUnavailable: config.sandbox not enabled" };
    }
    const url = this._sandboxUrl;
    const key = this._sandboxKey;
    if (!url || !key) {
      return { ok: false, error: "SandboxUnavailable: sandbox endpoint/key not configured" };
    }
    const op = name.slice("sandbox.".length); // exec | git | writeFile | readFile | list | expose
    const a0 = (args && args[0]) as Record<string, unknown> | string | number | undefined;
    const a1 = (args && args[1]) as Record<string, unknown> | string | undefined;
    // Route + method + body per op. Reads are GET with query params; mutations are POST JSON.
    let route = "";
    let method = "POST";
    let body: Record<string, unknown> | undefined;
    const qp = new URLSearchParams();
    switch (op) {
      case "exec": {
        // host.sandbox.exec(cmd, {cwd?})
        const cmd = typeof a0 === "string" ? a0 : (a0 as { cmd?: string } | undefined)?.cmd;
        const cwd = (a1 as { cwd?: string } | undefined)?.cwd ?? (a0 as { cwd?: string } | undefined)?.cwd;
        route = "/exec";
        body = { cmd, ...(cwd != null ? { cwd } : {}) };
        break;
      }
      case "git": {
        // host.sandbox.git({op:"checkout", repo, branch?, dir?}) | host.sandbox.git("checkout", {...})
        const g = (typeof a0 === "object" && a0 ? a0 : (a1 as Record<string, unknown>)) as Record<string, unknown>;
        route = "/git";
        body = { op: (g && g.op) || (typeof a0 === "string" ? a0 : "checkout"), repo: g && g.repo, branch: g && g.branch, dir: g && g.dir };
        break;
      }
      case "writeFile": {
        // host.sandbox.writeFile(path, content)
        const path = typeof a0 === "string" ? a0 : (a0 as { path?: string } | undefined)?.path;
        const content = typeof a1 === "string" ? a1 : (a0 as { content?: string } | undefined)?.content;
        route = "/files";
        body = { op: "write", path, content: content ?? "" };
        break;
      }
      case "readFile": {
        // host.sandbox.readFile(path) -> GET /files?path=&op=read
        const path = typeof a0 === "string" ? a0 : (a0 as { path?: string } | undefined)?.path;
        method = "GET";
        route = "/files";
        qp.set("op", "read");
        if (path != null) qp.set("path", String(path));
        break;
      }
      case "list": {
        const path = typeof a0 === "string" ? a0 : (a0 as { path?: string } | undefined)?.path;
        method = "GET";
        route = "/files";
        qp.set("op", "list");
        if (path != null) qp.set("path", String(path));
        break;
      }
      case "expose": {
        // host.sandbox.expose(port) -> {url}
        const port = typeof a0 === "number" ? a0 : Number((a0 as { port?: unknown } | undefined)?.port);
        route = "/expose";
        body = { port };
        break;
      }
      case "mount": {
        route = "/mount";
        break;
      }
      case "unmount": {
        route = "/unmount";
        break;
      }
      default:
        return { ok: false, error: "SandboxError: unknown op '" + op + "'" };
    }
    try {
      const headers: Record<string, string> = {
        authorization: "Bearer " + key,
        "x-engram-session": this._sandboxDoId || "",
      };
      const init: RequestInit = { method, headers };
      if (method !== "GET" && body !== undefined) {
        headers["content-type"] = "application/json";
        init.body = JSON.stringify(body);
      }
      const q = qp.toString();
      const full = url.replace(/\/$/, "") + route + (q ? "?" + q : "");
      const r = await fetch(full, init);
      // Cap the response body like _doFetch (the container can return large dir listings / stdout).
      const ab = await r.arrayBuffer();
      let bytes = new Uint8Array(ab);
      if (bytes.length > FETCH_MAX_BODY_BYTES) bytes = bytes.subarray(0, FETCH_MAX_BODY_BYTES);
      const text = TEXT_DEC.decode(bytes);
      let value: unknown;
      try { value = JSON.parse(text); } catch { value = { raw: text }; }
      if (!r.ok) {
        const errMsg = (value && typeof value === "object" && (value as { error?: unknown }).error)
          ? String((value as { error: unknown }).error)
          : "HTTP " + r.status;
        return { ok: false, error: "SandboxError: " + errMsg };
      }
      return { ok: true, value };
    } catch (e) {
      const err = e as { message?: string };
      return { ok: false, error: "SandboxError: " + String((err && err.message) || e) };
    }
  }

  // _serviceFs: bridge the engine's `host.__fs({op,path,data?})` to the DO-side R2/S3 handler.
  // Binary crosses the engine boundary as base64 (`data`); here we decode it to a Uint8Array for
  // the handler, and re-encode returned bytes to base64 for the engine. Errors are passed back as
  // a value `{error}` so the in-VM fs throws them (e.g. ENOENT), never crossing as a reject.
  async _serviceFs(req: Record<string, unknown>): Promise<HostResult> {
    const h = this._fsHandler;
    if (!h) {
      return { ok: true, value: { error: "FsError: no fs provider active (set config.fs.provider)" } };
    }
    const op = String(req && req.op);
    // Forward the per-op fields the DO handler understands: write chunk index/count (a body >64KB is
    // sent in slices), and read range window (off/len) for chunked reassembly.
    const payload: Record<string, unknown> = { op, path: req && req.path };
    if (typeof req.chunk === "number") payload.chunk = req.chunk;
    if (typeof req.chunks === "number") payload.chunks = req.chunks;
    if (typeof req.off === "number") payload.off = req.off;
    if (typeof req.len === "number") payload.len = req.len;
    if (op === "write" && typeof req.data === "string") {
      try {
        payload.bytes = Uint8Array.from(atob(req.data as string), (c) => c.charCodeAt(0));
      } catch {
        return { ok: true, value: { error: "FsError: bad base64 write payload" } };
      }
    }
    let res: Record<string, unknown>;
    try {
      res = (await h(payload)) as Record<string, unknown>;
    } catch (e) {
      const err = e as { message?: string };
      return { ok: true, value: { error: "FsError: " + String((err && err.message) || e) } };
    }
    if (res && res.error) return { ok: true, value: { error: res.error } };
    const value: Record<string, unknown> = { ok: true };
    if (res && res.bytes instanceof Uint8Array) {
      let s = "";
      const u8 = res.bytes as Uint8Array;
      for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]);
      value.data = btoa(s);
    }
    if (res && Array.isArray(res.names)) value.names = res.names;
    // size: read returns the TOTAL file size (for chunked reassembly); stat returns size + type.
    if (res && typeof res.size === "number") value.size = res.size;
    if (op === "stat") {
      value.isFile = !!(res && res.isFile);
      value.isDirectory = !!(res && res.isDirectory);
    }
    return { ok: true, value };
  }

  // _clientHostCall: park the VM eval, send {t:hostcall,id,name,args} to the client, and
  // await the correlated {t:hostcall-result,id,...} reply (delivered out-of-band by lib.rs
  // -> resolveHostCall). Rejects cleanly (never throws across the boundary) when there is
  // no live socket, the payload is oversized, or the client never answers in time — the
  // engine turns {ok:false} into a rejected VM promise, the cell fails, and the eval mutex
  // is released.
  async _clientHostCall(name: string, args: unknown[]): Promise<HostResult> {
    const sender = this._hostSender;
    if (!sender) {
      return { ok: false, error: "HostCallUnavailable: no client socket for host." + name };
    }
    const id = "hc-" + (++__hostCallSeq).toString(36) + "-" + Date.now().toString(36);
    let argsJson: string;
    try {
      argsJson = JSON.stringify(args ?? []);
    } catch {
      return { ok: false, error: "HostCallArgsError: host." + name + " args not JSON-serialisable" };
    }
    const frame = JSON.stringify({ t: "hostcall", id, name, args: JSON.parse(argsJson) });
    // SIZE GUARD: mirror the engine HOSTCALL buffer (64KB) — refuse an oversized request.
    if (TEXT_ENC.encode(frame).length > (1 << 16)) {
      return { ok: false, error: "HostCallSizeError: host." + name + " request exceeds 64KB" };
    }
    return new Promise<HostResult>((resolve) => {
      const timer = setTimeout(() => {
        __pendingHostCalls.delete(id);
        resolve({ ok: false, error: "HostCallTimeout: client did not answer host." + name + " in time" });
      }, sender.timeoutMs);
      __pendingHostCalls.set(id, { resolve, timer });
      const sent = sender.send(frame);
      if (!sent) {
        __pendingHostCalls.delete(id);
        clearTimeout(timer);
        resolve({ ok: false, error: "HostCallUnavailable: client socket send failed for host." + name });
      }
    });
  }

  // _fetchPrep: shared allowlist + SSRF + body-decode + deterministic Idempotency-Key fence used by
  // both _doFetch (buffered) and _doFetchStream (chunked). Returns the prepared RequestInit, or an
  // {error} envelope when the URL is invalid / blocked. Zero entropy (the key is deterministic).
  async _fetchPrep(url: string, init?: RequestInit): Promise<{ fetchInit?: RequestInit; error?: string }> {
    const allow = this._fetchAllow;
    let host = "";
    let hostname = "";
    try {
      const u = new URL(url);
      host = u.host;
      hostname = u.hostname;
    } catch {
      return { error: "FetchError: invalid url" };
    }
    // SSRF HARD-BLOCK (UNCONDITIONAL — DNS-rebind defense). Block link-local/metadata + RFC1918 +
    // loopback literal IPs and known-internal hostname suffixes.
    if (isBlockedSsrfHost(hostname)) {
      return { error: "FetchBlockedError: " + hostname + " is a blocked private/internal address" };
    }
    let permitted = false;
    if (allow === true) permitted = true;
    else if (allow === false) permitted = false;
    else if (Array.isArray(allow)) permitted = allow.includes(host);
    if (!permitted) return { error: "FetchBlockedError: " + host + " not allowed" };

    // BINARY-SAFE: a request body may be binary; the engine sends it as base64 in init.bodyB64.
    let fetchInit: RequestInit | undefined = init;
    if (init && typeof (init as { bodyB64?: unknown }).bodyB64 === "string") {
      const ri = { ...(init as Record<string, unknown>) } as RequestInit & { bodyB64?: string };
      const reqBytes = b64ToBytes((init as { bodyB64: string }).bodyB64);
      delete ri.bodyB64;
      ri.body = reqBytes as unknown as BodyInit;
      fetchInit = ri;
    }

    // EXACTLY-ONCE FENCE: deterministic Idempotency-Key for non-idempotent methods.
    {
      const method = String(
        (fetchInit && (fetchInit as { method?: string }).method) ||
          (init && (init as { method?: string }).method) || "GET",
      ).toUpperCase();
      if (method === "POST" || method === "PUT" || method === "DELETE" || method === "PATCH") {
        const hdrs = new Headers((fetchInit && fetchInit.headers) || undefined);
        if (!hdrs.has("Idempotency-Key") && this._fenceDoId != null && this._fenceCell != null) {
          const seq = this._curFetchSeq ?? 0;
          const mat = this._fenceDoId + ":" + this._fenceCell + ":" + seq;
          const digest = await crypto.subtle.digest("SHA-256", TEXT_ENC.encode(mat));
          const keyHex = Array.from(new Uint8Array(digest))
            .map((b) => b.toString(16).padStart(2, "0")).join("");
          hdrs.set("Idempotency-Key", "engram-" + keyHex.slice(0, 32));
          const ri2 = { ...((fetchInit || {}) as Record<string, unknown>) } as RequestInit;
          ri2.headers = hdrs;
          fetchInit = ri2;
        }
      }
    }
    return { fetchInit: fetchInit || undefined };
  }

  async _doFetch(url: string, init?: RequestInit): Promise<HostResult> {
    try {
      const prep = await this._fetchPrep(url, init);
      if (prep.error) return { ok: false, error: prep.error };
      const fetchInit = prep.fetchInit;
      // SSRF-safe: manual redirect following with per-hop host re-check.
      const r = await ssrfSafeFetch(url, fetchInit || undefined);
      // BINARY-SAFE: read RAW BYTES (arrayBuffer), never lossy `.text()`. Cross the boundary as
      // base64 (`bodyB64`) — exact bytes both ways. Cap at FETCH_MAX_BODY_BYTES (was 1MB).
      const ab = await r.arrayBuffer();
      let bytes = new Uint8Array(ab);
      let truncated = false;
      if (bytes.length > FETCH_MAX_BODY_BYTES) {
        bytes = bytes.subarray(0, FETCH_MAX_BODY_BYTES);
        truncated = true;
      }
      const bodyB64 = bytesToB64(bytes);
      // back-compat utf8 view — CAPPED to a small preview (the exact bytes are in bodyB64). Decoding
      // the FULL body here for a large payload would double the host-call result size and OOM the VM
      // (see FETCH_BODY_UTF8_PREVIEW_BYTES). The engine's .text()/.json() fall back to bodyB64 when
      // the body is truncated, so this only shrinks the convenience preview, never correctness.
      const bodyTruncated = bytes.length > FETCH_BODY_UTF8_PREVIEW_BYTES;
      const body = TEXT_DEC.decode(bodyTruncated ? bytes.subarray(0, FETCH_BODY_UTF8_PREVIEW_BYTES) : bytes);
      const headers: Record<string, string> = {};
      r.headers.forEach((v, k) => (headers[k] = v));
      return {
        ok: true,
        value: {
          status: r.status,
          ok: r.ok,
          // statusText/url/redirected complete the in-VM WHATWG Response (Wave 4). Older engines
          // ignore the extra fields, so this is backward-compatible with a vendored engine.
          statusText: r.statusText || "",
          url: r.url || url,
          redirected: !!r.redirected,
          headers,
          body,
          bodyTruncated,
          bodyB64,
          byteLength: bytes.length,
          truncated,
        },
      };
    } catch (e) {
      const err = e as { message?: string };
      return { ok: false, error: "FetchError: " + String((err && err.message) || e) };
    }
  }

  // ---- TRUE STREAMING (#13) host effects ----
  // _doFetchStream: open the upstream and register a DO-side reader; return metadata + streamId (and,
  // for small/known-length bodies, the whole body inline as bodyB64 = today's one-round-trip path).
  // For request-body streaming (init.requestStream), build a TransformStream feeding fetch({body,
  // duplex:"half"}) and return {uploadOpen}. Allowlist/SSRF/Idempotency reuse _fetchPrep.
  async _doFetchStream(url: string, init?: RequestInit): Promise<HostResult> {
    try {
      const prep = await this._fetchPrep(url, init);
      if (prep.error) return { ok: false, error: prep.error };
      const fetchInit = (prep.fetchInit || {}) as RequestInit & { requestStream?: boolean };
      const streamId = (this._fenceDoId ?? "do") + ":" + (this._fenceCell ?? 0) + ":" + (this._curFetchSeq ?? 0);
      if (!this._streams) this._streams = new Map();

      // ---- request-body streaming (upload): VM will push chunks via streamWrite ----
      if ((init as { requestStream?: boolean } | undefined)?.requestStream) {
        const ts = new TransformStream<Uint8Array, Uint8Array>();
        const upInit: RequestInit & { duplex?: string } = { ...fetchInit, duplex: "half", redirect: "manual" };
        delete (upInit as { requestStream?: boolean }).requestStream;
        upInit.body = ts.readable as unknown as BodyInit;
        // kick off the fetch WITHOUT awaiting; the VM drains its source via streamWrite.
        // redirect:"manual" — a streamed (duplex) upload body cannot be safely replayed to a
        // re-checked redirect target, so a 3xx surfaces as a non-2xx response instead of following
        // (no SSRF bypass via Location). The initial host was already SSRF-checked in _fetchPrep.
        const fetchPromise = fetch(url, upInit as RequestInit);
        this._streams.set(streamId, { writer: ts.writable.getWriter(), fetchPromise, done: false });
        return { ok: true, value: { streamId, uploadOpen: true, stream: true } };
      }

      // SSRF-safe: manual redirect following with per-hop host re-check.
      const r = await ssrfSafeFetch(url, fetchInit as RequestInit);
      const headers: Record<string, string> = {};
      r.headers.forEach((v, k) => (headers[k] = v));
      const meta = {
        streamId,
        stream: true,
        status: r.status,
        ok: r.ok,
        statusText: r.statusText || "",
        url: r.url || url,
        redirected: !!r.redirected,
        headers,
      };
      // small-body fast path: known content-length below the inline threshold buffers in one round
      // trip (today's semantics) — returns bodyB64, no reader registered.
      const contentLength = r.headers.get("content-length");
      const clen = contentLength == null ? NaN : Number(contentLength);
      if (Number.isFinite(clen) && clen >= 0 && clen <= STREAM_INLINE_THRESHOLD) {
        const ab = await r.arrayBuffer();
        const bytes = new Uint8Array(ab);
        return { ok: true, value: { ...meta, bodyB64: bytesToB64(bytes), byteLength: bytes.length, inline: true } };
      }
      // truly chunked: register the reader; VM pulls chunks via streamRead.
      if (!r.body) {
        // no body (e.g. 204): inline-empty.
        return { ok: true, value: { ...meta, bodyB64: "", byteLength: 0, inline: true } };
      }
      this._streams.set(streamId, { reader: r.body.getReader(), done: false });
      return { ok: true, value: meta };
    } catch (e) {
      const err = e as { message?: string };
      return { ok: false, error: "FetchError: " + String((err && err.message) || e) };
    }
  }

  // _doStreamRead: pull exactly ONE chunk (<= STREAM_CHUNK_BYTES raw) from the registered upstream
  // reader. This single read IS the backpressure (only called when the VM ReadableStream wants more).
  // Unknown streamId (fresh isolate after hibernation) -> typed StreamExpiredError, socket alive.
  async _doStreamRead(streamId: string): Promise<HostResult> {
    const m = this._streams;
    const st = m && m.get(streamId);
    if (!st || !st.reader) {
      return { ok: true, value: { error: "StreamExpiredError: stream " + streamId + " did not survive hibernation (consume a stream within the cell that opened it)" } };
    }
    try {
      // drain any re-chunk leftover first.
      let chunk: Uint8Array;
      if (st.pending && st.pending.length) {
        chunk = st.pending;
        st.pending = undefined;
      } else {
        const { value, done } = await st.reader.read();
        if (done) {
          st.done = true;
          m!.delete(streamId);
          return { ok: true, value: { done: true } };
        }
        chunk = value as Uint8Array;
      }
      // re-chunk to <= STREAM_CHUNK_BYTES so the b64 result rides SCRATCH; stash the remainder.
      if (chunk.length > STREAM_CHUNK_BYTES) {
        st.pending = chunk.subarray(STREAM_CHUNK_BYTES);
        chunk = chunk.subarray(0, STREAM_CHUNK_BYTES);
      }
      return { ok: true, value: { chunk: bytesToB64(chunk), done: false } };
    } catch (e) {
      const err = e as { message?: string };
      st.done = true;
      m!.delete(streamId);
      return { ok: true, value: { error: "StreamReadError: " + String((err && err.message) || e) } };
    }
  }

  // _doStreamCancel: release the upstream reader (reader.cancel / GC / for-await early break).
  async _doStreamCancel(streamId: string): Promise<HostResult> {
    const m = this._streams;
    const st = m && m.get(streamId);
    if (st) {
      try { st.reader?.cancel(); } catch { /* already gone */ }
      try { st.writer?.abort(); } catch { /* */ }
      st.done = true;
      m!.delete(streamId);
    }
    return { ok: true, value: { ok: true } };
  }

  // _doStreamWrite: push one request-body chunk into the DO-side TransformStream writer. done=true
  // closes the writer (the in-flight upload fetch settles) and flips the same streamId into the
  // response-download reader so subsequent streamRead pulls the response body.
  async _doStreamWrite(streamId: string, chunkB64: string | null, done: boolean): Promise<HostResult> {
    const m = this._streams;
    const st = m && m.get(streamId);
    if (!st || !st.writer) {
      return { ok: true, value: { error: "StreamExpiredError: upload stream " + streamId + " is gone" } };
    }
    try {
      if (chunkB64) await st.writer.write(b64ToBytes(chunkB64));
      if (done) {
        await st.writer.close();
        st.writer = undefined;
        // await the response and flip to download.
        const r = await (st.fetchPromise as Promise<Response>);
        st.fetchPromise = undefined;
        const headers: Record<string, string> = {};
        r.headers.forEach((v, k) => (headers[k] = v));
        if (r.body) st.reader = r.body.getReader();
        else st.done = true;
        return { ok: true, value: { ok: true, uploaded: true, status: r.status, statusOk: r.ok, statusText: r.statusText || "", headers } };
      }
      return { ok: true, value: { ok: true } };
    } catch (e) {
      const err = e as { message?: string };
      return { ok: true, value: { error: "StreamWriteError: " + String((err && err.message) || e) } };
    }
  }

  // ---- WEBSOCKET-CLIENT-IN-VM (#12) host effects ----
  // _wsEnqueue: push an inbound frame onto a handle queue with a monotonic seq, wake one parked
  // recv waiter. Bounded queue (drop-oldest + {type:"overflow"} marker). Called by the socket
  // listeners on the DO event loop (which run while a cell is parked in await inside _wsRecv).
  _wsEnqueue(st: WsState, frame: Omit<WsFrame, "seq">): void {
    st.seq += 1;
    const f: WsFrame = { ...frame, seq: st.seq };
    st.queue.push(f);
    // backpressure: bound the queue; drop oldest non-overflow frames, record droppedThrough.
    while (st.queue.length > WS_QUEUE_MAX_FRAMES) {
      const dropped = st.queue.shift();
      if (dropped) st.droppedThrough = Math.max(st.droppedThrough, dropped.seq);
    }
    const w = st.waiters.shift();
    if (w) {
      // serve the EARLIEST queued frame at/after the implicit cursor — the waiter recorded its
      // sinceSeq via the closure; we just hand it the next available frame.
      w(f);
    }
  }

  // _wsOpen: allowlist-check (reuse _fetchPrep's SSRF + config.fetch allow) the ws/wss URL, open a
  // real outbound WebSocket, register it in _ws. handleId is DETERMINISTIC (doId,cell,ordinal) so
  // replay reproduces the same heap token. Returns {handleId, readyState}.
  async _wsOpen(url: string, protocols?: string[]): Promise<HostResult> {
    let host = "";
    let hostname = "";
    let proto = "";
    try {
      const u = new URL(url);
      host = u.host;
      hostname = u.hostname;
      proto = u.protocol;
    } catch {
      return { ok: false, error: "WsError: invalid url" };
    }
    if (proto !== "ws:" && proto !== "wss:") {
      return { ok: false, error: "WsError: only ws:/wss: URLs are allowed (got " + proto + ")" };
    }
    // SSRF HARD-BLOCK (unconditional, same defense as host.fetch): no ws:// to metadata/loopback/RFC1918.
    if (isBlockedSsrfHost(hostname)) {
      return { ok: false, error: "WsBlockedError: " + hostname + " is a blocked private/internal address" };
    }
    // allowlist: reuse config.fetch (_fetchAllow) — false=block all, true=all, [hosts]=hostnames.
    const allow = this._fetchAllow;
    let permitted = false;
    if (allow === true) permitted = true;
    else if (allow === false) permitted = false;
    else if (Array.isArray(allow)) permitted = allow.includes(host);
    if (!permitted) return { ok: false, error: "WsBlockedError: " + host + " not allowed" };

    const handleId = "ws:" + (this._fenceDoId ?? "do") + ":" + (this._fenceCell ?? 0) + ":" + (this._curFetchSeq ?? 0);
    if (!this._ws) this._ws = new Map();
    // workerd does NOT establish an outbound connection via `new WebSocket(url)` (that constructor
    // immediately errors). The supported path for an OUTBOUND client socket from a Worker/DO is the
    // fetch() + Upgrade handshake: fetch the ws/wss URL (mapped to http/https) with the Upgrade
    // header, read response.webSocket, .accept() it. ws: -> http:, wss: -> https:.
    const httpUrl = url.replace(/^ws:/i, "http:").replace(/^wss:/i, "https:");
    let socket: WebSocket;
    try {
      const reqHeaders: Record<string, string> = { Upgrade: "websocket" };
      if (protocols && protocols.length) reqHeaders["Sec-WebSocket-Protocol"] = protocols.join(", ");
      // redirect:"manual" — only the INITIAL host was SSRF-checked; if the upgrade endpoint 3xx's
      // we must re-check the Location host before following (else a public ws host could redirect
      // the upgrade to an internal address). A blocked hop rejects; we follow up to MAX_REDIRECT_HOPS.
      let curUrl = httpUrl;
      let resp = await fetch(curUrl, { headers: reqHeaders, redirect: "manual" });
      for (let hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
        if (!(resp.status >= 300 && resp.status < 400 && resp.headers.has("location"))) break;
        let next: URL;
        try { next = new URL(resp.headers.get("location") || "", curUrl); }
        catch { return { ok: false, error: "WsBlockedError: invalid redirect Location" }; }
        if (isBlockedSsrfHost(next.hostname)) {
          return { ok: false, error: "WsBlockedError: redirect to " + next.hostname + " is a blocked private/internal address" };
        }
        curUrl = next.toString();
        resp = await fetch(curUrl, { headers: reqHeaders, redirect: "manual" });
      }
      const sock = (resp as { webSocket?: WebSocket | null }).webSocket;
      if (!sock) {
        return { ok: false, error: "WsError: upstream did not upgrade (status " + resp.status + ")" };
      }
      socket = sock;
    } catch (e) {
      const err = e as { message?: string };
      return { ok: false, error: "WsError: " + String((err && err.message) || e) };
    }
    const st: WsState = { socket, url, queue: [], seq: 0, waiters: [], severed: false, closed: false, droppedThrough: 0 };
    this._ws.set(handleId, st);
    // (#5) accept() is MANDATORY for a fetch()+Upgrade outbound socket: until accept() is called,
    // workerd does NOT pump the socket's readable side, so the "message" listener never fires and the
    // VM's onmessage stays at 0 frames (the reported bug). The earlier optional-chained `accept?.()`
    // silently no-op'd if the property read threw. We call it explicitly BEFORE attaching listeners is
    // not required (workerd buffers until accept), but we MUST guarantee it runs. If accept() is
    // genuinely absent we surface a typed WsError rather than connecting a dead socket.
    const acc = (socket as { accept?: () => void }).accept;
    if (typeof acc !== "function") {
      this._ws.delete(handleId);
      return { ok: false, error: "WsError: outbound socket is not acceptable (no accept())" };
    }
    // set binaryType so binary frames arrive as ArrayBuffer (our message listener base64-encodes them).
    try { (socket as { binaryType?: string }).binaryType = "arraybuffer"; } catch { /* not settable */ }
    // attach listeners BEFORE accept() so no frame is missed, then accept to start delivery.
    socket.addEventListener("message", (ev: MessageEvent) => {
      const d = ev.data;

      if (typeof d === "string") {
        if (d.length > WS_FRAME_MAX_BYTES) { this._wsEnqueue(st, { type: "overflow", reason: "frame too large" }); return; }
        this._wsEnqueue(st, { type: "message", data: d, binary: false });
      } else {
        // binary: ArrayBuffer (binaryType=arraybuffer) -> base64.
        try {
          const u8 = d instanceof ArrayBuffer ? new Uint8Array(d) : new Uint8Array(d as ArrayBufferLike);
          if (u8.length > WS_FRAME_MAX_BYTES) { this._wsEnqueue(st, { type: "overflow", reason: "frame too large" }); return; }
          this._wsEnqueue(st, { type: "message", data: bytesToB64(u8), binary: true });
        } catch {
          this._wsEnqueue(st, { type: "error", reason: "bad binary frame" });
        }
      }
    });
    socket.addEventListener("close", (ev: CloseEvent) => {
      st.closed = true;
      this._wsEnqueue(st, { type: "close", code: (ev && ev.code) || 1000, reason: (ev && ev.reason) || "" });
    });
    socket.addEventListener("error", () => {
      this._wsEnqueue(st, { type: "error", reason: "socket error" });
    });
    // accept() starts frame delivery on the workerd-side accepted socket. The fetch-Upgrade socket
    // is already connected when fetch() resolved, so there is no async "open" event — synthesize one
    // so the VM shim's onopen fires and recv() sees an `open` frame first.
    try { acc.call(socket); } catch { /* some impls auto-accept */ }
    this._wsEnqueue(st, { type: "open" });
    const readyState = (socket as { readyState?: number }).readyState ?? 1;
    return { ok: true, value: { handleId, url, readyState } };
  }


  // _wsSend: send a text (or base64 binary) frame. Pure output, zero entropy.
  async _wsSend(handleId: string, data: string | null, isBinary: boolean): Promise<HostResult> {
    const st = this._ws && this._ws.get(handleId);
    if (!st) return { ok: true, value: { ok: false, error: "WsClosedError: handle gone (severed by hibernation?)" } };
    if (st.closed || st.severed) return { ok: true, value: { ok: false, error: "WsClosedError: socket closed" } };
    try {
      if (isBinary && data != null) st.socket.send(b64ToBytes(data));
      else st.socket.send(String(data ?? ""));
      return { ok: true, value: { ok: true } };
    } catch (e) {
      const err = e as { message?: string };
      return { ok: true, value: { ok: false, error: "WsSendError: " + String((err && err.message) || e) } };
    }
  }

  // _wsRecv: THE pull primitive. Return a queued frame with seq >= sinceSeq immediately; else PARK
  // until the next frame arrives or the idle deadline fires. A severed/missing handle (cold wake)
  // resolves a typed severed close. Every result is a recorded host effect (oplog) like fetch.
  //
  // (#5) SAME-CELL DELIVERY FIX. The outbound socket's `message` event listener (which enqueues
  // inbound frames via _wsEnqueue) only fires when workerd delivers the read to THIS I/O context.
  // While a single eval cell is parked here, the eval Promise has not settled, and a SINGLE long
  // `setTimeout(deadline)` park does not give workerd the repeated macrotask turns it needs to
  // deliver the buffered socket read into our listener — so the echo sat undelivered until the
  // cell ended (it surfaced only on the NEXT cell, the observed cross-cell-works/same-cell-fails
  // bug). The fix is to actively re-check the queue across MANY short `setTimeout` macrotask turns:
  // each tick is a fresh I/O turn that lets workerd run the socket `message` listener (enqueueing
  // the frame), and the very next tick observes it. We STILL register a waiter so a frame that the
  // listener enqueues mid-tick wakes us immediately (fast path); the polling loop is the safety net
  // that guarantees delivery within a single cell regardless of workerd's event-batching.
  async _wsRecv(handleId: string, sinceSeq: number, timeoutMs?: number): Promise<HostResult> {
    const st = this._ws && this._ws.get(handleId);
    if (!st) {
      // handle token survived the snapshot but the live socket did not: severed-on-wake.
      return { ok: true, value: { type: "close", code: 1006, severed: true, seq: sinceSeq } };
    }
    // overflow marker first if frames were dropped past the cursor.
    if (st.droppedThrough >= sinceSeq) {
      const through = st.droppedThrough;
      st.droppedThrough = 0;
      return { ok: true, value: { type: "overflow", droppedThrough: through, seq: through } };
    }
    // immediate: earliest queued frame at/after the cursor.
    const queued = st.queue.find((f) => f.seq >= sinceSeq);
    if (queued) return { ok: true, value: queued };
    if (st.closed) return { ok: true, value: { type: "close", code: 1006, severed: false, seq: st.seq } };
    // park: resolve when the next frame arrives, or the idle deadline fires.
    const deadline = (typeof timeoutMs === "number" && timeoutMs > 0) ? timeoutMs : WS_RECV_IDLE_MS;
    return new Promise<HostResult>((resolve) => {
      let done = false;
      const start = Date.now();
      let pollTimer: ReturnType<typeof setTimeout> | undefined;

      const settle = (value: WsFrame | { type: string; [k: string]: unknown }): void => {
        if (done) return;
        done = true;
        if (pollTimer) clearTimeout(pollTimer);
        const i = st.waiters.indexOf(waiter);
        if (i >= 0) st.waiters.splice(i, 1);
        resolve({ ok: true, value });
      };

      // The waiter is woken DIRECTLY by _wsEnqueue when the socket listener fires while we are parked.
      const waiter = (f: WsFrame) => { settle(f); };

      // poll(): one short macrotask turn. Re-checks the queue (a frame the listener enqueued during
      // the PREVIOUS turn is now visible), honors close, then re-arms until the idle deadline. Each
      // `setTimeout(WS_RECV_POLL_MS)` returns control to workerd's event loop so it can deliver the
      // buffered socket read into our `message` listener — the crux of same-cell delivery.
      const poll = (): void => {
        if (done) return;
        const f = st.queue.find((fr) => fr.seq >= sinceSeq);
        if (f) { settle(f); return; }
        if (st.closed) { settle({ type: "close", code: 1006, severed: false, seq: st.seq }); return; }
        if (Date.now() - start >= deadline) {
          settle({ type: "idle", seq: sinceSeq });
          return;
        }
        pollTimer = setTimeout(poll, WS_RECV_POLL_MS);
      };

      st.waiters.push(waiter);
      // RACE GUARD: a frame may have been enqueued AFTER our synchronous `find` above but BEFORE the
      // waiter was registered (no waiter was present, so _wsEnqueue could not wake us). Re-check now
      // that the waiter is in place and serve it immediately so we never park on an already-queued frame.
      const raced = st.queue.find((fr) => fr.seq >= sinceSeq);
      if (raced) { settle(raced); return; }
      // kick the polling loop (first tick yields a fresh I/O turn for socket delivery).
      pollTimer = setTimeout(poll, WS_RECV_POLL_MS);
    });
  }

  // _wsPoll: non-blocking drain of all queued frames with seq >= sinceSeq (the pump cell's fast path).
  async _wsPoll(handleId: string, sinceSeq: number): Promise<HostResult> {
    const st = this._ws && this._ws.get(handleId);
    if (!st) return { ok: true, value: { events: [{ type: "close", code: 1006, severed: true, seq: sinceSeq }], readyState: 3 } };
    const events = st.queue.filter((f) => f.seq >= sinceSeq);
    const readyState = st.closed ? 3 : ((st.socket as { readyState?: number }).readyState ?? 1);
    return { ok: true, value: { events, readyState } };
  }

  // _wsClose: close the real socket, mark the registry entry closed. Idempotent.
  async _wsClose(handleId: string, code?: number, reason?: string): Promise<HostResult> {
    const st = this._ws && this._ws.get(handleId);
    if (st) {
      try { st.socket.close(typeof code === "number" ? code : 1000, reason || ""); } catch { /* already gone */ }
      st.closed = true;
      // wake any parked waiters with a close so the cell does not hang to the idle deadline.
      const ws = st.waiters.splice(0);
      for (const w of ws) { try { w({ type: "close", code: code || 1000, reason: reason || "", seq: st.seq }); } catch { /* */ } }
    }
    return { ok: true, value: { ok: true } };
  }

  // dump: read the live linear memory + counters + kv, gzip, return the image + meta.
  //
  // W5 compaction (docs/W5-COMPACTION-PLAN.md). The size-admission GUARD lives here. WASM linear

  // memory is MONOTONIC — it never shrinks. A session that spikes then frees keeps a high-water-
  // mark buffer. So the admission is on the USED heap (the genuine OOM fence), NOT the monotonic
  // buffer. We keep an ABSOLUTE safe-serialize cap on the raw buffer to avoid the adversarial
  // W4-ceiling regression. Returns { raw, usedHeap, bufferBytes, scrubbed }. Throws
  // SizeAdmissionError on a too-big (or wedged) buffer/heap — socket alive, next eval works.
  _serializeForDump(): SerializeResult {
    const ex = this._ex();
    const bufBytes0 = Number(ex.buffer_bytes());

    // ABSOLUTE cap FIRST (before any full-buffer touch): a buffer above the safe-serialize
    // ceiling risks an uncatchable OOM (snapshot+gz is ~2-3x). Hard-reject, socket alive.
    if (bufBytes0 > SAFE_SERIALIZE_BUFFER_BYTES) {
      throw new Error(
        "SizeAdmissionError: linear buffer " + bufBytes0 +
          "B > SAFE_SERIALIZE_BUFFER_BYTES " + SAFE_SERIALIZE_BUFFER_BYTES +
          " (WASM memory cannot shrink in place; refusing snapshot — reset to recover)"
      );
    }

    // W5 (a): admit on the live used heap. We only GC/scrub when the buffer is actually BLOATED
    // (a freed spike) — running GC on every checkpoint is unnecessary and can disturb in-flight
    // job state, so the steady-state path leaves the heap untouched. used_heap() alone does not GC.
    let usedHeap = Number(ex.used_heap());
    let scrubbed = false;
    const bloated = bufBytes0 > COMPACT_TRIGGER_BYTES;
    if (bloated) {
      // GC first (scrub_arena(0) GCs without allocating) so freed slack is reclaimable.
      usedHeap = Number(ex.scrub_arena(0));
    }
    if (usedHeap > MAX_USED_BYTES) {
      throw new Error(
        "SizeAdmissionError: live used heap " + usedHeap +
          "B > MAX_USED_BYTES " + MAX_USED_BYTES + "; refusing snapshot"
      );
    }
    // TIER-1 ADDITIVE: gate the INCOMPRESSIBLE extent (max of the genuine allocator tally and the
    // buffer minus the zeroed scratch high-water) below the ~28MB transient-OOM cliff. This closes
    // the gap between MAX_USED_BYTES(50MB) and SAFE_SERIALIZE_BUFFER_BYTES(76MB) for the specific
    // incompressible-heap case that WS-1006s the DO during the dump's transient gz/copy expansion.
    // usedHeap here is the GC-refined value (scrub_arena ran above when the buffer was bloated).
    // NOTE: the SCRATCH_DELTA_BYTES discount intentionally accounts for ALL compressible high-water
    // (the freed dlmalloc arena + zero-filled WASM growth + the SCRATCH Vec), not just the resident
    // SCRATCH — that whole region gzips away (sizeGz << sizeRaw). An earlier attempt to discount only
    // resident scratch (scratch_cap()) over-rejected legitimate ~24-29MB-raw-but-compressible
    // snapshots and was reverted. The genuine BUG-1 (un-restorable freed-spike commit) is mitigated
    // on the RESTORE side (the triple-buffer reduction below), not here.
    const incompressibleBytes = Math.max(usedHeap, bufBytes0 - SCRATCH_DELTA_BYTES);
    if (incompressibleBytes >= INCOMPRESSIBLE_BUFFER_CEILING_BYTES) {
      throw new Error(
        "SizeAdmissionError: incompressible buffer " + incompressibleBytes +
          "B >= INCOMPRESSIBLE_BUFFER_CEILING_BYTES " + INCOMPRESSIBLE_BUFFER_CEILING_BYTES +
          " (below the ~28MB transient-OOM cliff; refusing snapshot — reset to recover)"
      );
    }
    // W5 (b)+(c): scrub freed slack so the STORED gz image shrinks below the soft ceiling.
    const slack = bufBytes0 - usedHeap;
    const wedgedRatio = bloated && usedHeap / bufBytes0 < COMPACT_USED_RATIO;
    if (bloated && (slack > SCRUB_SLACK_BYTES || wedgedRatio) && bufBytes0 <= SCRUB_MAX_BUFFER_BYTES) {
      const budgetMb = Math.max(0, Math.min(Math.floor((slack / (1024 * 1024)) * 0.6), 24));
      if (budgetMb >= 1) {
        usedHeap = Number(ex.scrub_arena(budgetMb));
        scrubbed = true;
      }
    }

    const bufferBytes = Number(ex.buffer_bytes());
    const raw = new Uint8Array(ex.memory.buffer.slice(0));
    return { raw, usedHeap, bufferBytes, scrubbed };
  }

  _dumpCommon(usedHeap: number, bufferBytes: number, scrubbed: boolean, raw: Uint8Array): DumpCommon {
    const ex = this._ex();
    return {
      sizeRaw: raw.length,
      usedHeap,
      bufferBytes,
      scratchLen: Number(ex.scratch_cap()),
      scrubbed,
      stackPointer: 0,
      clockCalls: Number(ex.clock_calls()),
      rngCalls: Number(ex.rng_calls()),
      kvJson: this._exportKv(),
    };
  }

  // dump: full-image W5-compacted snapshot (used by the non-delta path / first base).
  async dump(): Promise<DumpResult> {
    const { raw, usedHeap, bufferBytes, scrubbed } = this._serializeForDump();
    const gz = zstdCompress(raw);
    // W4 commit-ordering: stage; commitDump() promotes to _lastImage only after the DO txn commits.
    this._pendingImage = raw.slice();
    return { gz, sizeGz: gz.length, snapCodec: SNAP_CODEC, mode: "full", grain: DELTA_GRAIN_BYTES, imageLen: raw.length,
      nChanged: 0, indicesGz: null, imageCrc: crc32(raw),
      ...this._dumpCommon(usedHeap, bufferBytes, scrubbed, raw) };
  }

  // W4 BYTE-DELTA dump (docs/W4-BYTEDELTA-PLAN.md). Diffs the current raw image vs the retained
  // `this._lastImage` at a 256B grain. If a prior image exists, !forceFull, raw >= prev length,
  // AND the delta is small (< DELTA_FALLBACK_PCT of full) => emit mode:"delta" carrying ONLY the
  // changed grains + indices. Else => mode:"full" (W5-compacted base), reset the chain.
  async dumpW4(forceFull: boolean): Promise<DumpResult> {
    const { raw, usedHeap, bufferBytes, scrubbed } = this._serializeForDump();
    const common = this._dumpCommon(usedHeap, bufferBytes, scrubbed, raw);
    let prev = this._lastImage;
    // LARGE-BUFFER 2-COPY NO-RETAIN PATH (memory-safety fence; see LARGE_BUFFER_NO_RETAIN_BYTES).
    // Only trips when the buffer carries a big payload (well above the ~59MB normal-session ceiling),
    // so ordinary cells are unaffected and keep the delta path + oplog tail. RELEASE the retained
    // image FIRST (free prev), gzip raw to a single full base, and do NOT retain a new _lastImage —
    // peak memory is ~raw + gz. Emits mode:"full" (the DO full path resets the oplog tail).
    if (raw.byteLength > LARGE_BUFFER_NO_RETAIN_BYTES) {
      // No-retain: this image is too big to keep host-side. Compute crc BEFORE freeing, store a
      // full base, and clear BOTH bases — the next dump is forced full too (prev=null => canDelta
      // false), keeping the chain self-consistent. commitDump() is still called but finds no pending.
      const imageCrc = crc32(raw);
      this._lastImage = null;
      this._pendingImage = null;
      prev = null;
      const gz = zstdCompress(raw);
      return { mode: "full", gz, snapCodec: SNAP_CODEC, indicesGz: null, nChanged: 0, grain: DELTA_GRAIN_BYTES,
        imageLen: raw.byteLength, sizeGz: gz.byteLength, imageCrc, ...common };
    }
    // W4 (growth-tolerant): the WASM linear buffer is MONOTONIC and commonly grows by whole 64KB
    // pages between cells; allow a delta when the current buffer is the SAME size or LARGER than
    // prev — diff the overlapping prefix grain-by-grain and mark every newly-grown grain as
    // changed so its exact bytes are carried.
    const canDelta = !forceFull && prev && raw.byteLength >= prev.byteLength;

    // Emit a FULL (W5-compacted) base, resetting the delta chain. CHECKPOINT-PEAK fix: a large
    // full base does NOT retain a host-side image (peak ~raw+gz, not ~raw+gz+retain). A full base
    // resets the chain, so prev=null on the next dump is consistent (same pattern as the >63MB
    // no-retain fence). A small full base retains (commit-ordered via _pendingImage/commitDump).
    const emitFull = (): DumpResult => {
      const gz = zstdCompress(raw);
      const imageCrc = crc32(raw);
      if (raw.byteLength > FULL_BASE_NO_RETAIN_BYTES) {
        this._lastImage = null;
        this._pendingImage = null;
      } else {
        this._pendingImage = raw.slice();
      }
      return { mode: "full", gz, snapCodec: SNAP_CODEC, indicesGz: null, nChanged: 0, grain: DELTA_GRAIN_BYTES,
        imageLen: raw.byteLength, sizeGz: gz.byteLength, imageCrc, ...common };
    };

    if (canDelta && prev) {
      const grain = DELTA_GRAIN_BYTES;
      const nGrains = Math.ceil(raw.byteLength / grain);
      const prevLen = prev.byteLength;
      const changed: number[] = [];
      for (let i = 0; i < nGrains; i++) {
        const start = i * grain;
        const end = Math.min(start + grain, raw.byteLength);
        // grain entirely beyond prev => brand-new bytes, always carry.
        if (start >= prevLen) { changed.push(i); continue; }
        let diff = false;
        const cmpEnd = Math.min(end, prevLen);
        for (let j = start; j < cmpEnd; j++) { if (raw[j] !== prev[j]) { diff = true; break; } }
        // grain straddles the old/new boundary => the tail bytes are new, carry it.
        if (!diff && end > prevLen) diff = true;
        if (diff) changed.push(i);
      }
      // CHECKPOINT-PEAK fix: decide delta-vs-full from the CHEAP changed-grain estimate BEFORE any
      // big allocation. A dense mutation (e.g. a freed spike, where ~every grain changed) skips the
      // payload buffer + payGz + idxGz + a full `zstdCompress(raw)` fallback that previously all
      // lived simultaneously (the ~5x peak that WS-1006'd the DO) and goes straight to a no-retain
      // full base. Equivalent OUTPUT to the old deltaStored-vs-fullGz test (both delta and full
      // reconstruct identically); only the decision is cheaper and the dense path avoids the peak.
      const changedBytes = changed.length * grain;
      const dense = changedBytes >= raw.byteLength * DELTA_FALLBACK_PCT;
      if (!dense) {
        const payload = new Uint8Array(changedBytes);
        for (let k = 0; k < changed.length; k++) {
          const i = changed[k];
          const start = i * grain;
          const end = Math.min(start + grain, raw.byteLength);
          payload.set(raw.subarray(start, end), k * grain);
        }
        const idx = new Uint32Array(changed.length);
        for (let k = 0; k < changed.length; k++) idx[k] = changed[k];
        const idxBytes = new Uint8Array(idx.buffer, idx.byteOffset, idx.byteLength);
        const payGz = zstdCompress(payload);
        const idxGz = zstdCompress(idxBytes);
        const deltaBlobsFit = payGz.byteLength <= DELTA_MAX_BLOB_BYTES && idxGz.byteLength <= DELTA_MAX_BLOB_BYTES;
        if (deltaBlobsFit) {
          // STAGE the candidate base; commitDump() promotes it ONLY after the DO appends this delta
          // row + bumps delta_seq. A delta MUST retain (retained image == committed chain tail).
          this._pendingImage = raw.slice();
          return { mode: "delta", gz: payGz, indicesGz: idxGz, snapCodec: SNAP_CODEC, nChanged: changed.length, grain,
            imageLen: raw.byteLength, sizeGz: payGz.byteLength + idxGz.byteLength, imageCrc: crc32(raw), ...common };
        }
        // sparse but the delta blobs overflow the SQLite value cap => fall through to a full base.
      }
      // dense, or sparse-but-blobs-too-big => full base.
      return emitFull();
    }

    // No prev / forceFull => full base.
    return emitFull();
  }

  // W4 commit-ordering hook: the DO calls this AFTER its checkpoint SQL txn commits successfully,
  // promoting the staged candidate image to the live delta base. If the txn never ran or threw, the
  // DO does NOT call this, so _lastImage stays pinned to the last COMMITTED chain tail — the next
  // delta is guaranteed to diff against the image the stored chain reconstructs to. This is the
  // primary fix for the W4 module-drop desync. Idempotent; a no-op when nothing is staged.
  commitDump(): void {
    if (this._pendingImage) {
      this._lastImage = this._pendingImage;
      this._pendingImage = null;
    }
  }

  _exportKv(): string {
    const ex = this._ex();
    const ptr = ex.kv_export_ptr();
    const len = ex.kv_export_len();
    return TEXT_DEC.decode(new Uint8Array(ex.memory.buffer, ptr, len).slice());
  }
  _importKv(json: string): void {
    const ex = this._ex();
    const bytes = TEXT_ENC.encode(json);
    this._writeScratch(bytes);
    ex.kv_import(ex.scratch_ptr(), bytes.length);
  }

  // stdlib injection: report which modules were injected at create (the esbuilt bundle evaled
  // into the VM). The actual catalog is wired by lib.rs via the STDLIB bundle Text module.
  stdlibInfo(): string {
    return JSON.stringify({ loaded: this._stdlibLoaded || [], available: stdlibNames(), optIn: stdlibOptIn(), defaults: stdlibDefaults() });
  }
  // E6 ENGINE-MIGRATION journal replay (docs E6). On an engine-hash mismatch at cold wake, the
  // byte-blit image is invalid (a different engine layout), so the DO replays the retained cell
  // oplog into a FRESH instance under the same config — no re-fire of pure cells, host results
  // fed back from the recorded oplog. journal = [{src, hostResults:[...]}].
  async replayJournal(journal: ReplayCell[], configJson: string, kvJson: string): Promise<string> {
    await this.createFresh(configJson);
    if (kvJson && kvJson !== "{}") this._importKv(kvJson);
    const cells = Array.isArray(journal) ? journal : [];
    let replayed = 0, failed = 0, effectful = 0;
    const errors: string[] = [];
    for (const c of cells) {
      try {
        // feed recorded host results back in order (no re-fire of effects).
        this._replayHostResults = Array.isArray(c.hostResults) ? c.hostResults.slice() : [];
        if (this._replayHostResults.length) effectful++;
        const out = await this.evalCode(String(c.src || ""));
        const parsed = JSON.parse(out);
        if (parsed.ok === false) failed++; else replayed++;
      } catch (e) { const err = e as { message?: string }; failed++; errors.push(String((err && err.message) || e)); }
    }
    this._replayHostResults = null;
    return JSON.stringify({ replayed, failed, effectfulCells: effectful, errors });
  }

  // _writeScratch: copy `bytes` into the engine's FIXED 32MB SCRATCH buffer (raised from 1MB to
  // match FETCH_MAX_BODY_BYTES so large fetch bodies / fs writes can cross the boundary). The engine exposes
  // scratch_cap() = capacity; we bounds-check FIRST so an oversized payload throws a typed
  // ProtocolSizeError (caller turns it into {ok:false}) instead of a TypedArray.set RangeError
  // ("offset is out of bounds") that overruns linear memory. Universal backstop for every caller
  // (eval source, stdlib IIFE, resume payload, kv import).
  _writeScratch(bytes: Uint8Array): void {
    const ex = this._ex();
    // DYNAMIC scratch: grow to fit (up to the 32MB ceiling), THEN re-read cap / ptr / memory.buffer
    // — a reserve can relocate the Vec AND detach+replace memory.buffer (wasm grow), so nothing may
    // be cached across it. scratch_release() (in the eval finally) returns it to the 1MB floor.
    ex.scratch_reserve(bytes.length);
    const cap = ex.scratch_cap();
    if (bytes.length > cap) {
      const e = new Error(
        "ProtocolSizeError: payload " + bytes.length + "B exceeds scratch capacity " + cap +
          "B (max eval/source size)"
      );
      (e as Error & { name: string }).name = "ProtocolSizeError";
      throw e;
    }
    new Uint8Array(ex.memory.buffer).set(bytes, ex.scratch_ptr());
  }
  _readResult(): string {
    const ex = this._ex();
    const ptr = ex.result_ptr();
    const len = ex.result_len();
    return TEXT_DEC.decode(new Uint8Array(ex.memory.buffer, ptr, len).slice());
  }
  _readHostCall(): HostCall {
    const ex = this._ex();
    const ptr = ex.pending_host_call_ptr();
    const len = ex.pending_host_call_len();
    return JSON.parse(TEXT_DEC.decode(new Uint8Array(ex.memory.buffer, ptr, len).slice()));
  }

  drop(): void {
    this.inst = null;
    this.wasiCtl = null;
  }
}

// ---- gzip/gunzip via the platform CompressionStream (workerd-native) ----
async function gzip(u8: Uint8Array): Promise<Uint8Array> {
  const cs = new CompressionStream("gzip");
  const w = cs.writable.getWriter();
  w.write(u8);
  w.close();
  return new Uint8Array(await new Response(cs.readable).arrayBuffer());
}
async function gunzip(u8: Uint8Array): Promise<Uint8Array> {
  const ds = new DecompressionStream("gzip");
  const w = ds.writable.getWriter();
  w.write(u8);
  w.close();
  return new Uint8Array(await new Response(ds.readable).arrayBuffer());
}

// ---- zstd codec (issue #9) — a precompiled CompiledWasm module (workerd forbids runtime
// WebAssembly.compile). zstd level 9 is measurably SMALLER and FASTER than the platform gzip
// CompressionStream on the snapshot image (fresh -31%, stdlib -16%, incompressible -7%; compress
// 0.56-0.90x gzip time; decompress 2-4x faster). The codec is a tiny zstd-sys cdylib (codec/ crate)
// exporting a clean C ABI (cz_alloc/cz_free/cz_bound/cz_compress/cz_decompress/cz_frame_size) over
// its OWN linear memory; we drive it with a stubbed WASI (it only imports environ/fd_write/proc_exit
// and never touches them on the compress/decompress path). Lazy singleton — instantiated once on
// first use, reused for every dump/restore. GLUE-side: does NOT touch engine.wasm => ENGINE_HASH
// unchanged => no forced oplog-replay for live sessions on deploy.
const ZSTD_LEVEL = 9; // measured sweet spot (best size, still faster than gzip)
interface ZstdExports {
  memory: WebAssembly.Memory;
  cz_alloc: (len: number) => number;
  cz_free: (ptr: number, len: number) => void;
  cz_bound: (srcLen: number) => number;
  cz_compress: (dst: number, dstCap: number, src: number, srcLen: number, level: number) => number;
  cz_decompress: (dst: number, dstCap: number, src: number, srcLen: number) => number;
  cz_frame_size: (src: number, srcLen: number) => number;
}
let _zstd: ZstdExports | null = null;
function zstdCodec(): ZstdExports {
  if (_zstd) return _zstd;
  const mod = globalThis.__ZSTD_MODULE as WebAssembly.Module | undefined;
  if (!mod) throw new Error("ZstdCodecError: __ZSTD_MODULE not present (entry.ts must import zstd-codec.wasm)");
  const noop = (): number => 0;
  const inst = new WebAssembly.Instance(mod, {
    wasi_snapshot_preview1: {
      environ_get: noop,
      environ_sizes_get: noop,
      fd_write: noop,
      proc_exit: (): void => { throw new Error("ZstdCodecError: unexpected proc_exit"); },
    },
  });
  _zstd = inst.exports as unknown as ZstdExports;
  return _zstd;
}
function zstdCompress(u8: Uint8Array): Uint8Array {
  const z = zstdCodec();
  const srcLen = u8.length;
  const sp = z.cz_alloc(srcLen || 1);
  if (srcLen) new Uint8Array(z.memory.buffer).set(u8, sp);
  const cap = z.cz_bound(srcLen);
  const dp = z.cz_alloc(cap);
  try {
    const n = z.cz_compress(dp, cap, sp, srcLen, ZSTD_LEVEL);
    if (n === 0 && srcLen !== 0) throw new Error("ZstdCodecError: cz_compress failed (" + srcLen + "B)");
    return new Uint8Array(z.memory.buffer, dp, n).slice();
  } finally {
    z.cz_free(sp, srcLen || 1);
    z.cz_free(dp, cap);
  }
}
// Decompress a zstd frame. rawLenHint (the recorded uncompressed length) is used when present;
// otherwise the frame's own content-size header is read. Throws on a size/decode error.
function zstdDecompress(u8: Uint8Array, rawLenHint?: number): Uint8Array {
  const z = zstdCodec();
  const srcLen = u8.length;
  const sp = z.cz_alloc(srcLen || 1);
  if (srcLen) new Uint8Array(z.memory.buffer).set(u8, sp);
  let dstLen = rawLenHint && rawLenHint > 0 ? rawLenHint : 0;
  try {
    if (!dstLen) dstLen = z.cz_frame_size(sp, srcLen);
    if (!dstLen) { z.cz_free(sp, srcLen || 1); return new Uint8Array(0); }
    const dp = z.cz_alloc(dstLen);
    try {
      const n = z.cz_decompress(dp, dstLen, sp, srcLen);
      if (n === 0 && dstLen !== 0) throw new Error("ZstdCodecError: cz_decompress failed");
      return new Uint8Array(z.memory.buffer, dp, n).slice();
    } finally {
      z.cz_free(dp, dstLen);
    }
  } finally {
    z.cz_free(sp, srcLen || 1);
  }
}

// CODEC SELECTOR (back-compat). Snapshots/deltas written before issue #9 carry NO codec tag (or
// tag "gzip") and MUST still restore — the decompressor dispatches on the recorded tag. New dumps
// tag "zstd". A W4 delta chain can straddle a deploy boundary (base gzip, new deltas zstd); every
// chunk is decompressed by ITS OWN recorded codec, so mixed chains restore correctly.
const SNAP_CODEC = "zstd"; // codec for NEW dumps
type SnapCodec = "gzip" | "zstd";
function normCodec(tag: string | null | undefined): SnapCodec {
  return tag === "zstd" ? "zstd" : "gzip"; // absent/empty/"gzip" => gzip (back-compat default)
}
async function codecCompress(u8: Uint8Array): Promise<{ out: Uint8Array; codec: SnapCodec }> {
  return { out: zstdCompress(u8), codec: "zstd" };
}
async function codecDecompress(u8: Uint8Array, codec: string | null | undefined, rawLenHint?: number): Promise<Uint8Array> {
  return normCodec(codec) === "zstd" ? zstdDecompress(u8, rawLenHint) : gunzip(u8);
}

// CRC32 (IEEE 802.3, table-driven) over a byte buffer. Used as the W4 reconstruction checksum:
// the DO stores the dump's reconstructed-image CRC in the manifest; restoreW4 recomputes it after
// applying base+deltas and rejects (=> oplog replay) on mismatch. Returns an unsigned 32-bit int.
let _CRC_TABLE: Uint32Array | null = null;
function crc32(buf: Uint8Array): number {
  let t = _CRC_TABLE;
  if (!t) {
    t = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      t[n] = c >>> 0;
    }
    _CRC_TABLE = t;
  }
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) crc = (crc >>> 8) ^ t[(crc ^ buf[i]) & 0xff];
  return (crc ^ 0xffffffff) >>> 0;
}
