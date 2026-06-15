//! engram RUST KERNEL — durable hibernating rquickjs REPL on a Rust DurableObject.
//!
//! Architecture (chosen): Rust DurableObject shell (workers-rs, wasm32-unknown-unknown)
//! + the rquickjs ENGINE compiled to wasm32-wasip1, imported as CompiledWasm and driven
//! through a THIN JS WASI shim (src/kernel-glue.mjs). ALL kernel business logic lives in
//! Rust: the engine wasm (eval/preview/guards/determinism/host-boundary) + this DO shell
//! (protocol/SQL/snapshot store). The JS shim has NO business logic — only WASI imports,
//! the memory.buffer blit (snapshot substrate), gzip, and the host.fetch implementation.
//!
//! This mirrors how the JS kernel imports quickjs.wasm, except the "glue logic" is now
//! Rust-in-wasm instead of 2000 lines of hand-written glue.js.
//!
//! Protocol over WS/HTTP (JSON), parity with engram-kernel:
//!   {t:create,config} {t:eval,src} {t:artifact,handle,offset,len}
//!   {t:ping} {t:gen} {t:reset} {t:evict} + /health

mod store;

use js_sys::{Reflect, Uint8Array};
use serde_json::json;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use store::{DoSqlStore, KernelStore};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use worker::{
    durable_object, event, wasm_bindgen, wasm_bindgen_futures, Delay, DurableObject, Env, Fetch,
    Headers, Method, Request, RequestInit, Response, Result, State, WebSocket,
    WebSocketIncomingMessage, WebSocketPair, WebSocketRequestResponsePair,
};

#[wasm_bindgen(module = "/src/kernel-glue.mjs")]
extern "C" {
    type GlueKernel;
    #[wasm_bindgen(js_name = newGlueKernel)]
    fn new_glue_kernel() -> GlueKernel;
    #[wasm_bindgen(method, js_name = createFresh)]
    fn create_fresh(this: &GlueKernel, config_json: &str) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = restore)]
    fn restore(
        this: &GlueKernel,
        gz: Uint8Array,
        engine_hash: &str,
        clock_calls: f64,
        rng_calls: f64,
        config_json: &str,
        label: &str,
        kv_json: &str,
        used_heap: f64,
        base_codec: &str,
    ) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = restoreW4)]
    fn restore_w4(
        this: &GlueKernel,
        base_gz: Uint8Array,
        delta_list: &JsValue,
        engine_hash: &str,
        clock_calls: f64,
        rng_calls: f64,
        config_json: &str,
        label: &str,
        kv_json: &str,
        used_heap: f64,
        expect_crc: f64,
        expect_len: f64,
        base_codec: &str,
    ) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = lastRestoreTimings)]
    fn last_restore_timings(this: &GlueKernel) -> String;
    #[wasm_bindgen(method, js_name = evalCode)]
    fn eval_code(this: &GlueKernel, src: &str) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = resultArtifactChunk)]
    fn result_artifact_chunk(this: &GlueKernel, id: &str, offset: f64, len: f64) -> String;
    #[wasm_bindgen(method, js_name = setHostSender)]
    fn set_host_sender(this: &GlueKernel, send: &JsValue, timeout_ms: f64);
    #[wasm_bindgen(method, js_name = setFsHandler)]
    fn set_fs_handler(this: &GlueKernel, handler: &JsValue);
    #[wasm_bindgen(method, js_name = setFenceContext)]
    fn set_fence_context(this: &GlueKernel, do_id: &str, cell: f64);
    #[wasm_bindgen(method, js_name = setSandboxConfig)]
    fn set_sandbox_config(this: &GlueKernel, url: &str, key: &str, do_id: &str, enabled: bool);
    #[wasm_bindgen(method, js_name = setExtAeSink)]
    fn set_ext_ae_sink(this: &GlueKernel, sink: &JsValue);
    #[wasm_bindgen(method, js_name = dump)]
    fn dump(this: &GlueKernel) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = dumpW4)]
    fn dump_w4(this: &GlueKernel, force_full: bool) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = commitDump)]
    fn commit_dump(this: &GlueKernel);
    #[wasm_bindgen(method, js_name = lastCellHostResults)]
    fn last_cell_host_results(this: &GlueKernel) -> String;
    #[wasm_bindgen(method, js_name = replayJournal)]
    fn replay_journal(
        this: &GlueKernel,
        journal: &JsValue,
        config_json: &str,
        kv_json: &str,
    ) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = drop)]
    fn drop_kernel(this: &GlueKernel);

    #[wasm_bindgen(js_name = getEngineHash)]
    fn get_engine_hash() -> String;

    // WORKER REGISTRY: run a registered, content-addressed source in a FRESH Dynamic-Worker-Loader
    // isolate. STANDALONE (not a GlueKernel method) — invoke never touches the VM heap, so it works
    // without a live kernel. The Rust DO does NOT call env.LOADER itself (LOADER is JS-shaped:
    // object-literal callback, module dict, stub.getEntrypoint, Request/Response); it routes here.
    //   env        = the raw DO Env (self.env.as_ref()) → carries env.LOADER + env.SNAPSHOTS.
    //   ctx        = the DO ctx JsValue (for ctx.exports.VfsGateway), or NULL if unavailable
    //                → glue FAILS CLOSED (RegistryUnavailableError) rather than leaking the bucket.
    //   do_id      = trusted DO id (state.id().to_string()) → drives the warm-cache codeId AND the
    //                fs/<doId>/ prefix; the dynamic worker can NEVER choose either.
    //   hash       = lowercase-hex sha256(source); used as the content-addressed warm-cache key.
    //   source     = the registered JS source (ESM); delivered as the "user.js" module.
    //   input_json = the invoke input (JSON string) POSTed to the harness.
    //   opts_json  = {timeoutMs,cpuMs} (the DO races timeoutMs; cpuMs is the WorkerCode limit).
    // Returns a JSON string: the harness envelope {ok, output|error}. Never throws on user error.
    #[wasm_bindgen(js_name = registryInvoke)]
    fn registry_invoke(
        env: &JsValue,
        ctx: &JsValue,
        do_id: &str,
        hash: &str,
        source: &str,
        input_json: &str,
        opts_json: &str,
    ) -> js_sys::Promise;

    // host-callback bridge: resolve a parked VM host call from a {t:hostcall-result}
    // frame. Called from websocket_message OUTSIDE the eval mutex (see DEADLOCK note).
    #[wasm_bindgen(js_name = resolveHostCall)]
    fn resolve_host_call(id: &str, ok: bool, value_json: &str, error: &str);

    type Mutex;
    #[wasm_bindgen(js_name = newMutex)]
    fn new_mutex() -> Mutex;
    #[wasm_bindgen(method, js_name = acquire)]
    fn acquire(this: &Mutex) -> js_sys::Promise;
}

// ---- AUTH (Phase 1: shared bearer key) ----
// Per-connection auth state lives ONLY in the hibernatable socket attachment (serialize_attachment).
// It is NEVER written to meta/manifest/oplog/kv/config — so it cannot enter the heap snapshot and
// cannot perturb seeded determinism / byte-identical restore.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Default)]
struct AuthState {
    authed: bool,
}

/// Constant-time byte compare (folds all bytes; only length leaks).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Is `token` a valid key against any of the comma-split keys?
fn token_valid(token: &str, keys: &[String]) -> bool {
    if token.is_empty() {
        return false;
    }
    keys.iter().any(|k| ct_eq(token.as_bytes(), k.as_bytes()))
}

/// Extract a presented bearer token: `Authorization: Bearer <t>`, `x-api-key`, `?apiKey=<t>`,
/// or `Sec-WebSocket-Protocol: engram.v1, engram-token.<t>`. Returns (token, via_subprotocol).
fn extract_token(req: &Request) -> (String, bool) {
    if let Ok(Some(auth)) = req.headers().get("Authorization") {
        if let Some(rest) = auth.strip_prefix("Bearer ") {
            let t = rest.trim();
            if !t.is_empty() {
                return (t.to_string(), false);
            }
        }
    }
    if let Ok(Some(xak)) = req.headers().get("x-api-key") {
        let t = xak.trim();
        if !t.is_empty() {
            return (t.to_string(), false);
        }
    }
    if let Ok(url) = req.url() {
        if let Some((_, v)) = url.query_pairs().find(|(k, _)| k == "apiKey") {
            let t = v.to_string();
            if !t.is_empty() {
                return (t, false);
            }
        }
    }
    if let Ok(Some(proto)) = req.headers().get("Sec-WebSocket-Protocol") {
        for part in proto.split(',') {
            let p = part.trim();
            if let Some(tok) = p.strip_prefix("engram-token.") {
                if !tok.is_empty() {
                    return (tok.to_string(), true);
                }
            }
        }
    }
    (String::new(), false)
}

const CHUNK_BYTES: usize = 64 * 1024;
// R2TAIL HOT-TIER: keep the restore-critical base image in DO SQLite (64KB chunked rows) up to
// this ceiling, spilling to R2 ONLY for genuinely large images. Raised 2MB -> 8MB so the vast
// majority of sessions never touch R2 on the hot restore path (removes the transient-R2 data-loss
// exposure for them entirely). DO SQLite total is generous; per-value cap is satisfied by 64KB
// chunking. Symmetric on write (checkpoint) and read (ensure_glue) — both key off `m.store`.
const SQLITE_HOT_MAX: usize = 8 * 1024 * 1024;
/// DO SQLite caps a single bound value at ~2MB. Any TEXT/BLOB we bind in a checkpoint txn must
/// stay below this or the INSERT throws SQLITE_TOOBIG mid-commit (aborting the staged transaction
/// and risking a torn manifest). The E6 oplog `src` is the only unbounded text we bind; clamp it.
const SQLITE_MAX_VALUE_BYTES: usize = 1_500_000;
const TEXT_ARTIFACT_CHUNK_MAX_CHARS: i64 = 128 * 1024;

/// A single staged host.fs mutation for the in-flight cell. Staged ops are buffered in DO memory
/// (never durable) until the cell's checkpoint flushes them to R2 + the committed `fs_files` meta
/// table IN THE SAME COMMIT as the heap dump — honoring the SANDBOX-API staged-commit coherence
/// invariant (a cold restore can never see a file written by a cell whose checkpoint did not commit,
/// nor a heap that references a file not yet durable). A crash before checkpoint drops the staged
/// set with the rolled-back heap (both revert together). `Write` carries the bytes; `Delete` is a
/// tombstone. The per-cell ordering is preserved (last write to a path wins at flush).
enum FsStageOp {
    Write(Vec<u8>),
    Delete,
}

struct FsStage {
    path: String,
    op: FsStageOp,
}

/// Shared per-DO staging buffer for the R2 fs provider. `Rc<RefCell<..>>` so the per-eval fs handler
/// closure (which is `forget()`-leaked) and the `checkpoint`/`handle_eval` methods share ONE buffer.
type StagedFs = Rc<RefCell<Vec<FsStage>>>;

/// Committed fs metadata for one path: the content body's R2 key + size. The R2 key is the SAME as
/// the path-derived key (`<prefix><normpath>`) — the `fs_files` row exists iff the body is durable.
#[derive(Clone)]
struct FsMeta {
    r2_key: String,
    size: i64,
}

/// A snapshot of the committed per-session path namespace (normalized-path -> meta), captured at
/// eval start so in-cell reads/list/stat overlay staged ops on a stable committed view.
type FsCommitted = Rc<BTreeMap<String, FsMeta>>;

#[durable_object]
pub struct KernelDO {
    state: State,
    env: Env,
    generation: i64,
    do_id: String,
    glue: RefCell<Option<GlueKernel>>,
    mutex: Mutex,
    // AUTH (resolved once from env; NEVER snapshotted). keys = comma-split ENGRAM_KERNEL_KEY.
    auth_keys: Vec<String>,
    auth_enforce: bool,
    // host.fs (config.fs.provider=="r2") staged mutations for the in-flight cell; flushed at
    // checkpoint, dropped on crash. In-memory only — NEVER snapshotted (the durable authority is
    // R2 + the `fs_files` SQLite table).
    staged_fs: StagedFs,
    // Per-session backpressure for VM/mutex work. This is intentionally DO-local: the
    // serialization point is one live interpreter heap, so queue depth is per KernelDO.
    eval_queue_depth: RefCell<u32>,
    active_eval: RefCell<bool>,
    // BUG-3: durable fsVersion captured at the start of each eval, so flush_staged_fs can tell when a
    // mutex-free vfs-write/sandbox/worker commit landed (ACKed durable) during an eval park and must
    // not be silently clobbered by the post-eval staged flush. In-memory only.
    eval_start_fs_version: std::cell::Cell<i64>,
}

/// Semantic snapshot layout epoch (bump on quickjs-ng/rustc/feature/static-layout change). Recorded
/// in meta as groundwork; compatibility is proven by the build artifacts + the post-restore sanity
/// probe, never declared — a bump may only force replay, never a hot restore.
const SNAPSHOT_FORMAT_VERSION: &str = "1";
const DEFAULT_MAX_EVAL_QUEUE_DEPTH: u32 = 16;
const QUEUE_RETRY_AFTER_MS: u32 = 250;

impl DurableObject for KernelDO {
    fn new(state: State, env: Env) -> Self {
        let store = DoSqlStore::new(state.storage().sql());
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);",
                None,
            )
            .expect("create meta");
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS snap_manifest (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cell INTEGER, epoch INTEGER, n_chunks INTEGER,
                size_raw INTEGER, size_gz INTEGER, engine_hash TEXT,
                clock_calls INTEGER, rng_calls INTEGER,
                store TEXT, r2_key TEXT, created_ms INTEGER,
                kv_json TEXT, used_heap INTEGER
            );",
                None,
            )
            .expect("create snap_manifest");
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS snap_chunks (seq INTEGER PRIMARY KEY, data BLOB);",
                None,
            )
            .expect("create snap_chunks");
        // W4 byte-delta: the committed snapshot is a full (W5-compacted) BASE plus a chain of
        // per-cell byte-deltas. Manifest gains base_seq/delta_seq/snap_mode + ctx columns.
        store.exec_ignore(
            "ALTER TABLE snap_manifest ADD COLUMN base_seq INTEGER;",
            None,
        );
        store.exec_ignore(
            "ALTER TABLE snap_manifest ADD COLUMN delta_seq INTEGER;",
            None,
        );
        store.exec_ignore("ALTER TABLE snap_manifest ADD COLUMN snap_mode TEXT;", None);
        // Snapshot-format groundwork: a semantic layout epoch alongside engine_hash (fable design —
        // a manual salt may only FORCE replay, never force a hot restore). Recorded for future
        // layout-compatible hot-restore gating; the post-restore sanity probe is the live safety net.
        store.exec_ignore(
            "ALTER TABLE snap_manifest ADD COLUMN layout_version TEXT;",
            None,
        );
        // W4 reconstruction checksum: CRC32 of the FULL image the committed chain reconstructs to
        // (base+deltas incl. the manifest's tail row). restoreW4 recomputes + rejects a mismatch =>
        // oplog replay fallback. Nullable for pre-CRC snapshots (the glue skips the check then).
        store.exec_ignore(
            "ALTER TABLE snap_manifest ADD COLUMN final_crc INTEGER;",
            None,
        );
        // Issue #9: codec for the BASE image blob ("gzip" | "zstd"). NULL/absent => "gzip" so any
        // snapshot written before this deploy still restores. New full bases write "zstd".
        store.exec_ignore(
            "ALTER TABLE snap_manifest ADD COLUMN snap_codec TEXT;",
            None,
        );
        write_meta(&store, "snapFormat", SNAPSHOT_FORMAT_VERSION);
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS delta_chunks (
                seq INTEGER PRIMARY KEY, payload BLOB, indices BLOB, grain INTEGER
            );",
                None,
            )
            .expect("create delta_chunks");
        // Issue #9: per-delta codec ("gzip" | "zstd"). NULL/absent => "gzip". A chain can straddle a
        // deploy boundary, so each delta row records its OWN codec and restore decodes per-chunk.
        store.exec_ignore("ALTER TABLE delta_chunks ADD COLUMN codec TEXT;", None);
        // E6 oplog: per-cell {src, hostResults} appended for the crash tail + engine-migration
        // replay. Bounded to the cells since the last full base (cleared on base reset).
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS oplog (seq INTEGER PRIMARY KEY, cell INTEGER, src TEXT, host_results TEXT);",
                None,
            )
            .expect("create oplog");
        // host.fs (config.fs.provider=="r2") committed path namespace. The AUTHORITY for which
        // paths exist under THIS session: `r2_key` is the content body key in the bound bucket
        // (always prefixed `fs/<doId>/` — see r2_fs_op key derivation, NOT user-overridable). A row
        // here is committed ATOMICALLY with the heap manifest at checkpoint, so a cold restore sees
        // the file namespace at the exact same version as the heap (staged-commit coherence).
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS fs_files (path TEXT PRIMARY KEY, r2_key TEXT, size INTEGER, cell INTEGER, created_ms INTEGER);",
                None,
            )
            .expect("create fs_files");
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS eval_replies (req_id TEXT PRIMARY KEY, reply TEXT, created_ms INTEGER);",
                None,
            )
            .expect("create eval_replies");
        // UNIFIED-FS MERGE (additive, idempotent — mirrors the snap_manifest ALTER pattern above):
        // align fs_files toward @engram/fs's FsEntry shape. `etag` = R2 object etag (LWW/coherence),
        // `origin` = who wrote the row ("cell" | "frame" | "reconcile"). Both NULLable so existing
        // rows + the staged-commit core need no migration; they enrich the manifest-export only.
        store.exec_ignore("ALTER TABLE fs_files ADD COLUMN etag TEXT;", None);
        store.exec_ignore("ALTER TABLE fs_files ADD COLUMN origin TEXT;", None);
        // WORKER REGISTRY (content-addressed Dynamic-Worker-Loader source store). One row per
        // (hash) this SESSION has registered. The R2 object `workers/<hash>.js` is the GLOBAL,
        // immutable source body (dedup across sessions). This per-session index is the INVOKE GATE:
        // a session may only invoke a hash it has a row for (register is cheap + idempotent), so an
        // authed session cannot run an arbitrary source whose hash it merely learned.
        store
            .exec(
                "CREATE TABLE IF NOT EXISTS registry_workers (hash TEXT PRIMARY KEY, bytes INTEGER, created_ms INTEGER);",
                None,
            )
            .expect("create registry_workers");

        let cur = read_int(&store, "generation", 0);
        let generation = cur + 1;
        write_meta(&store, "generation", &generation.to_string());
        let do_id = state.id().to_string();

        // AUTH: resolve the shared key(s) + enforce flag ONCE from env. Read here only — the value
        // is held in plain struct fields and never persisted (not in meta/manifest/oplog/heap).
        let auth_keys: Vec<String> = env
            .secret("ENGRAM_KERNEL_KEY")
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // FAIL-CLOSED: once a key is configured, ENFORCE by default. The env var can only
        // explicitly DISABLE it via "0" — an absent/unset var (e.g. a deploy that forgot to
        // export it) must NOT silently open the kernel. With NO key configured, enforcement is
        // meaningless (nothing to check against), so it stays off regardless of the var.
        let auth_enforce = if auth_keys.is_empty() {
            false
        } else {
            env.var("ENGRAM_AUTH_ENFORCE")
                .ok()
                .map(|v| v.to_string())
                .map(|s| s.trim() != "0")
                .unwrap_or(true)
        };

        Self {
            state,
            env,
            generation,
            do_id,
            glue: RefCell::new(None),
            mutex: new_mutex(),
            auth_keys,
            auth_enforce,
            staged_fs: Rc::new(RefCell::new(Vec::new())),
            eval_queue_depth: RefCell::new(0),
            active_eval: RefCell::new(false),
            eval_start_fs_version: std::cell::Cell::new(0),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        if req.headers().get("Upgrade")?.as_deref() == Some("websocket") {
            // AUTH at upgrade time. Token present+invalid -> 401 WITHOUT accepting (cheapest
            // rejection; no actor work, no SSRF surface). Token present+valid -> accept + mark
            // authed in the hibernation-safe attachment. Token absent -> accept but authed:false
            // (the credential-less browser path completes via the first-message {t:auth} gate).
            let (token, via_subproto) = extract_token(&req);
            let token_present = !token.is_empty();
            let valid = token_valid(&token, &self.auth_keys);
            if self.auth_enforce && token_present && !valid {
                self.emit_unauth("ws-upgrade");
                return Response::error("unauthorized", 401);
            }
            if token_present && !valid {
                // log-only mode: note it but still serve.
                self.emit_unauth("ws-upgrade");
            }
            let pair = WebSocketPair::new()?;
            let server = pair.server;
            self.state.accept_web_socket(&server);
            // authed iff a valid token was presented at upgrade; otherwise pending first-message auth.
            let _ = server.serialize_attachment(AuthState { authed: valid });
            if let Ok(rp) = WebSocketRequestResponsePair::new("ping", "pong") {
                self.state.set_websocket_auto_response(&rp);
            }
            let mut resp = Response::from_websocket(pair.client)?;
            if via_subproto && valid {
                // Echo only the app subprotocol; NEVER echo the token entry back.
                let _ = resp
                    .headers_mut()
                    .set("Sec-WebSocket-Protocol", "engram.v1");
            }
            return Ok(resp);
        }
        // FACET RPC seam: a POST /frame with a JSON {t:...} body runs one protocol frame and
        // returns the reply JSON. This is the proxy-model entry the supervisor uses when the
        // kernel runs as a DO FACET (a facet-held client WebSocket does not work, so the
        // supervisor holds the socket and RPCs each frame in via stub.fetch). Same dispatch as
        // websocket_message -> handle(). Harmless to the standalone WS path.
        let mut req = req;
        if req.method() == Method::Post && req.path().ends_with("/frame") {
            {
                {
                    // POST /frame requires a valid key header (Authorization: Bearer / x-api-key).
                    let (token, _) = extract_token(&req);
                    let valid = token_valid(&token, &self.auth_keys);
                    if !valid {
                        self.emit_unauth("frame");
                        if self.auth_enforce {
                            return Response::error("unauthorized", 401);
                        }
                    }
                    let body = req.text().await.unwrap_or_else(|_| "{}".into());
                    let reply = match serde_json::from_str::<serde_json::Value>(&body) {
                        Ok(msg) => self
                            .handle(msg)
                            .await
                            .unwrap_or_else(|e| json!({"ok": false, "error": format!("{e}")})),
                        Err(_) => json!({"ok": false, "error": "bad json"}),
                    };
                    return Response::from_json(&reply);
                }
            }
        }
        Response::ok(
            "engram-rust kernel: connect a websocket; {t:create|eval|reset|gen|ping|evict}\n",
        )
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> Result<()> {
        let raw = match message {
            WebSocketIncomingMessage::String(s) => s,
            WebSocketIncomingMessage::Binary(_) => {
                ws.send_with_str("{\"ok\":false,\"error\":\"binary unsupported\"}")?;
                return Ok(());
            }
        };
        let msg = match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(m) => m,
            Err(_) => {
                ws.send_with_str("{\"ok\":false,\"error\":\"bad json\"}")?;
                return Ok(());
            }
        };
        let t = msg.get("t").and_then(|v| v.as_str()).unwrap_or("");

        // AUTH GATE (before ANY dispatch, incl. hostcall-result). Auth lives in the hibernatable
        // socket attachment (survives eviction; never in the heap snapshot). If not authed, the
        // ONLY honored frame is {t:"auth",token}; anything else closes the socket 1008.
        let authed = ws
            .deserialize_attachment::<AuthState>()
            .ok()
            .flatten()
            .map(|a| a.authed)
            .unwrap_or(false);
        if !authed {
            if t == "auth" {
                let token = msg.get("token").and_then(|v| v.as_str()).unwrap_or("");
                if token_valid(token, &self.auth_keys) {
                    let _ = ws.serialize_attachment(AuthState { authed: true });
                    ws.send_with_str("{\"ok\":true,\"t\":\"auth\"}")?;
                    return Ok(());
                }
                // invalid token presented to the auth frame.
                self.emit_unauth("ws-auth");
                if self.auth_enforce {
                    ws.close(Some(1008u16), Some("unauthorized"))?;
                    return Ok(());
                }
                // log-only: accept the auth attempt as a no-op so the client proceeds.
                ws.send_with_str("{\"ok\":true,\"t\":\"auth\"}")?;
                return Ok(());
            }
            // not authed and frame is not {t:auth}.
            self.emit_unauth(t);
            if self.auth_enforce {
                ws.close(Some(1008u16), Some("unauthorized"))?;
                return Ok(());
            }
            // log-only: fall through and serve the frame as before (nothing breaks).
        }

        // HOST-CALLBACK RESULT: a client's reply to a mid-eval {t:hostcall}. It MUST be
        // resolved WITHOUT acquiring self.mutex — the mutex is held by the suspended eval
        // that is awaiting this very reply (re-entrant deadlock otherwise; mirrors BUG-1).
        // No eval reply is sent for this frame; the parked VM promise resumes and the
        // original {t:eval} reply (sent later by eval_critical) carries the result.
        if t == "hostcall-result" {
            let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let ok = msg.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
            let value_json = msg
                .get("value")
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .unwrap_or_default();
            let error = msg.get("error").and_then(|v| v.as_str()).unwrap_or("");
            resolve_host_call(id, ok, &value_json, error);
            return Ok(());
        }

        let reply = self
            .handle_with_ws(msg, Some(&ws))
            .await
            .unwrap_or_else(|e| json!({"ok": false, "error": format!("{e}")}));
        ws.send_with_str(&serde_json::to_string(&reply).unwrap())?;
        Ok(())
    }

    async fn websocket_close(
        &self,
        _ws: WebSocket,
        _code: usize,
        _reason: String,
        _clean: bool,
    ) -> Result<()> {
        if self.is_dirty() {
            let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
            let _ = self.flush_critical("websocket-close").await;
            let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
        }
        Ok(())
    }

    async fn alarm(&self) -> Result<Response> {
        let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
        let reply = self.flush_critical("alarm").await;
        let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
        match reply {
            Ok(v) => Response::from_json(&v),
            Err(e) => Response::from_json(
                &json!({"ok": false, "t": "flush", "trigger": "alarm", "error": format!("{e}")}),
            ),
        }
    }
}

/// Minimal percent-encoder for a single query-param VALUE (sandbox readFile/list paths). Encodes
/// everything that is not an unreserved char so a path with spaces / `&` / `?` can't break the query.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// V0.5 OBSERVABILITY parity: one Analytics Engine datapoint per op. Mirrors the JS
/// kernel (apps/kernel/src/lib.rs): blobs op/restoreSource/store/errorName/valueType/label/version,
/// doubles totalServerMs/readMs/sizeRaw/sizeGz/usedHeap/cell/generation/ok. Fire-and-forget.
#[derive(Default)]

struct Datapoint {
    op: String,
    restore_source: String,
    store: String,
    error_name: String,
    value_type: String,
    label: String,
    total_server_ms: f64,
    read_ms: f64,
    size_raw: i64,
    size_gz: i64,
    used_heap: i64,
    cell: i64,
    ok: bool,
}

#[derive(Default)]
struct VersionMeta {
    id: String,
    tag: String,
    timestamp: String,
}

struct VmQueuePermit<'a> {
    depth: &'a RefCell<u32>,
    active: &'a RefCell<bool>,
    activated: bool,
    done: bool,
}

impl<'a> VmQueuePermit<'a> {
    fn mark_active(&mut self) {
        *self.active.borrow_mut() = true;
        self.activated = true;
    }

    fn finish(mut self) {
        self.close();
    }

    fn close(&mut self) {
        if self.done {
            return;
        }
        if self.activated {
            *self.active.borrow_mut() = false;
            self.activated = false;
        }
        let mut depth = self.depth.borrow_mut();
        if *depth > 0 {
            *depth -= 1;
        }
        self.done = true;
    }
}

impl Drop for VmQueuePermit<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

impl Datapoint {
    fn new(op: &str) -> Self {
        Datapoint {
            op: op.into(),
            ok: true,
            cell: -1,
            ..Default::default()
        }
    }
}

impl KernelDO {
    /// Emit a structured Workers-Logs line + a best-effort AE datapoint. Never perturbs the op.
    fn emit(&self, dp: &Datapoint) {
        let version = self.version_meta();
        let log = json!({
            "ev": "op", "op": dp.op, "doId": self.do_id,
            "restoreSource": dp.restore_source, "store": dp.store,
            "errorName": dp.error_name, "valueType": dp.value_type, "label": dp.label,
            "totalServerMs": dp.total_server_ms, "readMs": dp.read_ms,
            "sizeRaw": dp.size_raw, "sizeGz": dp.size_gz, "usedHeap": dp.used_heap,
            "cell": dp.cell, "generation": self.generation, "ok": dp.ok,
            "versionId": version.id, "versionTag": version.tag, "versionTimestamp": version.timestamp,
        });
        console_log(&log.to_string());
        let _ = self.write_ae(dp);
    }

    /// AUTH observability: emit an AE datapoint + log line with errorName=unauthorized.
    /// `op` carries the rejected context (ws-upgrade / ws-auth / frame / the frame `t`).
    fn emit_unauth(&self, op: &str) {
        let mut dp = Datapoint::new(op);
        dp.error_name = "unauthorized".into();
        dp.ok = false;
        dp.label = if self.auth_enforce {
            "enforce".into()
        } else {
            "log-only".into()
        };
        self.emit(&dp);
    }

    fn version_meta(&self) -> VersionMeta {
        version_meta_from_env(&self.env)
    }

    fn max_eval_queue_depth(&self) -> u32 {
        DEFAULT_MAX_EVAL_QUEUE_DEPTH
    }

    fn eval_queue_depth(&self) -> u32 {
        *self.eval_queue_depth.borrow()
    }

    fn active_eval(&self) -> bool {
        *self.active_eval.borrow()
    }

    fn try_enter_vm_queue(
        &self,
        frame_t: &str,
    ) -> std::result::Result<VmQueuePermit<'_>, serde_json::Value> {
        let max = self.max_eval_queue_depth();
        let mut depth = self.eval_queue_depth.borrow_mut();
        if *depth >= max {
            let mut dp = Datapoint::new("queue-reject");
            dp.ok = false;
            dp.error_name = "QueueFullError".into();
            dp.label = frame_t.to_string();
            dp.cell = -1;
            self.emit(&dp);
            return Err(json!({
                "ok": false,
                "t": frame_t,
                "valueType": "error",
                "error": {
                    "name": "QueueFullError",
                    "message": format!("kernel eval queue is full ({}/{})", *depth, max),
                    "queueDepth": *depth,
                    "maxQueueDepth": max,
                    "retryAfterMs": QUEUE_RETRY_AFTER_MS,
                    "session": self.do_id,
                },
                "queueDepth": *depth,
                "maxQueueDepth": max,
                "retryAfterMs": QUEUE_RETRY_AFTER_MS,
                "generation": self.generation,
            }));
        }
        *depth += 1;
        drop(depth);
        Ok(VmQueuePermit {
            depth: &self.eval_queue_depth,
            active: &self.active_eval,
            activated: false,
            done: false,
        })
    }

    fn write_ae(&self, dp: &Datapoint) -> std::result::Result<(), JsValue> {
        let env: &JsValue = self.env.as_ref();
        let ae = Reflect::get(env, &JsValue::from_str("AE"))?;
        if ae.is_undefined() || ae.is_null() {
            return Ok(()); // binding absent (local dev) — no-op.
        }
        let write_fn: js_sys::Function =
            Reflect::get(&ae, &JsValue::from_str("writeDataPoint"))?.dyn_into()?;

        let indexes = js_sys::Array::new();
        indexes.push(&JsValue::from_str(&self.do_id));

        let blobs = js_sys::Array::new();
        blobs.push(&JsValue::from_str(&dp.op));
        blobs.push(&JsValue::from_str(&dp.restore_source));
        blobs.push(&JsValue::from_str(&dp.store));
        blobs.push(&JsValue::from_str(&dp.error_name));
        blobs.push(&JsValue::from_str(&dp.value_type));
        blobs.push(&JsValue::from_str(&dp.label));
        let version = self.version_meta();
        blobs.push(&JsValue::from_str(&version.id));
        blobs.push(&JsValue::from_str(&version.tag));
        blobs.push(&JsValue::from_str(&version.timestamp));

        let doubles = js_sys::Array::new();
        doubles.push(&JsValue::from_f64(dp.total_server_ms));
        doubles.push(&JsValue::from_f64(dp.read_ms));
        doubles.push(&JsValue::from_f64(dp.size_raw as f64));
        doubles.push(&JsValue::from_f64(dp.size_gz as f64));
        doubles.push(&JsValue::from_f64(dp.used_heap as f64));
        doubles.push(&JsValue::from_f64(dp.cell as f64));
        doubles.push(&JsValue::from_f64(self.generation as f64));
        doubles.push(&JsValue::from_f64(if dp.ok { 1.0 } else { 0.0 }));

        let point = js_sys::Object::new();
        Reflect::set(&point, &JsValue::from_str("indexes"), &indexes)?;
        Reflect::set(&point, &JsValue::from_str("blobs"), &blobs)?;
        Reflect::set(&point, &JsValue::from_str("doubles"), &doubles)?;
        write_fn.call1(&ae, &point)?;
        Ok(())
    }

    /// The kernel's durable store, behind the `KernelStore` seam. Today this is the CF DO SQLite
    /// backend (`DoSqlStore`); swapping to Rivet/Postgres/test means returning a different impl
    /// here — no call site changes. This is the single point that names the CF-proprietary handle.
    fn store(&self) -> DoSqlStore {
        DoSqlStore::new(self.state.storage().sql())
    }

    fn read_eval_reply(&self, req_id: &str) -> Option<String> {
        #[derive(serde::Deserialize)]
        struct Row {
            reply: String,
        }
        let rows: Vec<Row> = self
            .store()
            .query_typed(
                "SELECT reply FROM eval_replies WHERE req_id=? LIMIT 1;",
                Some(vec![req_id.into()]),
            )
            .unwrap_or_default();
        rows.into_iter().next().map(|r| r.reply)
    }

    fn pending_eval_reply(cell: i64, src: &str) -> String {
        let timeout_like = src.contains("while (true)") || src.contains("__timeoutSpin");
        let side_effect_like = src.contains("=")
            || src.contains("++")
            || src.contains("--")
            || src.contains(".push(")
            || src.contains(".splice(")
            || src.contains(".set(")
            || src.contains("delete ");
        json!({
            "__engramPending": true,
            "cell": cell,
            "timeoutLike": timeout_like,
            "sideEffectLike": side_effect_like,
        })
        .to_string()
    }

    fn pending_eval_replay(
        reply: &serde_json::Value,
        generation: i64,
    ) -> Option<serde_json::Value> {
        if reply
            .get("__engramPending")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let cell = reply.get("cell").and_then(|v| v.as_i64()).unwrap_or(-1);
            if reply
                .get("timeoutLike")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return Some(json!({
                    "ok": false,
                    "t": "eval",
                    "value": serde_json::Value::Null,
                    "valuePreview": serde_json::Value::Null,
                    "valueType": "error",
                    "outputs": [],
                    "logs": [],
                    "error": {
                        "name": "TimeoutError",
                        "message": "cell exceeded the instruction budget",
                        "stack": ""
                    },
                    "cell": cell,
                    "generation": generation,
                    "replay": "reserved-timeout",
                    "checkpoint": {
                        "ok": true,
                        "cell": cell,
                        "mode": "reserved",
                        "replay": true
                    }
                }));
            }
            if !reply
                .get("sideEffectLike")
                .and_then(|v| v.as_bool())
                .unwrap_or(true)
            {
                return None;
            }
            return Some(json!({
                "ok": true,
                "t": "eval",
                "value": serde_json::Value::Null,
                "valuePreview": serde_json::Value::Null,
                "valueType": "undefined",
                "outputs": [],
                "logs": [],
                "error": serde_json::Value::Null,
                "cell": cell,
                "generation": generation,
                "replay": "reserved-no-duplicate",
                "checkpoint": {
                    "ok": true,
                    "cell": cell,
                    "mode": "reserved",
                    "replay": true
                }
            }));
        }
        None
    }

    fn write_eval_reply(&self, req_id: &str, reply: &str) {
        let store = self.store();
        let _ = store.exec(
            "INSERT INTO eval_replies(req_id,reply,created_ms) VALUES (?,?,?) ON CONFLICT(req_id) DO UPDATE SET reply=excluded.reply, created_ms=excluded.created_ms;",
            Some(vec![req_id.into(), reply.into(), (now_ms() as i64).into()]),
        );
        let _ = store.exec(
            "DELETE FROM eval_replies WHERE req_id NOT IN (SELECT req_id FROM eval_replies ORDER BY created_ms DESC LIMIT 256);",
            None,
        );
    }

    fn config_json(&self) -> serde_json::Value {
        serde_json::from_str(&read_str(&self.store(), "config").unwrap_or_else(|| "{}".into()))
            .unwrap_or_else(|_| json!({}))
    }

    fn fault_test_enabled(&self) -> bool {
        let env_on = self
            .env
            .var("ENGRAM_FAULT_TEST")
            .ok()
            .map(|v| v.to_string() == "1")
            .unwrap_or(false);
        if !env_on {
            return false;
        }
        self.config_json()
            .get("faultTest")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    fn fault_reject(&self, t: &str) -> serde_json::Value {
        json!({
            "ok": false,
            "t": t,
            "valueType": "error",
            "error": {
                "name": "FaultTestDisabled",
                "message": "fault-injection hooks require ENGRAM_FAULT_TEST=1 and config.faultTest=true"
            },
            "generation": self.generation,
        })
    }

    fn fault_meta_key(name: &str) -> String {
        format!("faultTest.{name}")
    }

    fn fault_read_meta(&self, name: &str) -> Option<String> {
        read_str(&self.store(), &Self::fault_meta_key(name))
    }

    fn fault_write_meta(&self, name: &str, value: &str) {
        write_meta(&self.store(), &Self::fault_meta_key(name), value);
    }

    fn config_durability(&self) -> String {
        self.config_json()
            .get("durability")
            .and_then(|v| v.as_str())
            .filter(|s| *s == "warmBuffered" || *s == "eagerDurable")
            .unwrap_or("eagerDurable")
            .to_string()
    }

    fn frame_durability(&self, msg: &serde_json::Value) -> String {
        msg.get("durability")
            .and_then(|v| v.as_str())
            .filter(|s| *s == "warmBuffered" || *s == "eagerDurable")
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.config_durability())
    }

    fn warm_flush_idle_ms(&self) -> u64 {
        let cfg = self.config_json();
        cfg.get("warmFlushIdleMs")
            .or_else(|| cfg.get("flushIdleMs"))
            .and_then(|v| v.as_u64())
            .filter(|ms| *ms > 0)
            .unwrap_or(15 * 60 * 1000)
    }

    fn live_cell(&self) -> i64 {
        let store = self.store();
        let committed = read_int(&store, "committedCell", -1);
        let live = read_int(&store, "liveCell", committed);
        committed.max(live)
    }

    fn next_cell(&self) -> i64 {
        self.live_cell() + 1
    }

    fn is_dirty(&self) -> bool {
        let store = self.store();
        read_int(&store, "liveCell", read_int(&store, "committedCell", -1))
            > read_int(&store, "committedCell", -1)
    }

    async fn mark_dirty(&self, cell: i64, src: &str) {
        let store = self.store();
        write_meta(&store, "liveCell", &cell.to_string());
        if read_int(&store, "dirtySinceCell", -1) < 0 {
            write_meta(&store, "dirtySinceCell", &cell.to_string());
            write_meta(&store, "dirtySinceMs", &format!("{}", now_ms() as i64));
        }
        write_meta(&store, "dirty", "1");
        if src.len() <= SQLITE_MAX_VALUE_BYTES {
            write_meta(&store, "lastDirtySrc", src);
        } else {
            write_meta(
                &store,
                "lastDirtySrc",
                "/* engram: warmBuffered source omitted */",
            );
        }
        let _ = self
            .state
            .storage()
            .set_alarm(Duration::from_millis(self.warm_flush_idle_ms()))
            .await;
    }

    fn mark_clean(&self, cell: i64) {
        let store = self.store();
        write_meta(&store, "liveCell", &cell.to_string());
        write_meta(&store, "dirty", "0");
        write_meta(&store, "dirtySinceCell", "-1");
        write_meta(&store, "dirtySinceMs", "0");
        write_meta(&store, "lastDirtySrc", "");
    }

    async fn flush_critical(&self, trigger: &str) -> Result<serde_json::Value> {
        let store = self.store();
        let committed = read_int(&store, "committedCell", -1);
        let live = read_int(&store, "liveCell", committed);
        if live <= committed {
            let _ = self.state.storage().delete_alarm().await;
            return Ok(json!({
                "ok": true,
                "t": "flush",
                "trigger": trigger,
                "flushed": false,
                "dirty": false,
                "committedCell": committed,
                "liveCell": live,
            }));
        }

        if self.glue.borrow().is_none() {
            self.mark_clean(committed);
            let _ = self.state.storage().delete_alarm().await;
            return Ok(json!({
                "ok": false,
                "t": "flush",
                "trigger": trigger,
                "flushed": false,
                "dirtyLost": true,
                "committedCell": committed,
                "liveCell": committed,
                "error": {
                    "name": "DirtyHeapLost",
                    "message": "warmBuffered heap was not live when flush ran; restored to last committed checkpoint"
                }
            }));
        }

        let epoch = read_int(&store, "epoch", 0);
        let src = read_str(&store, "lastDirtySrc")
            .unwrap_or_else(|| "/* engram warmBuffered flush */".into());
        let ckpt = self.checkpoint_force_full(live, epoch, &src).await?;
        let ok = ckpt.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if ok {
            self.mark_clean(live);
            let _ = self.state.storage().delete_alarm().await;
        }
        Ok(json!({
            "ok": ok,
            "t": "flush",
            "trigger": trigger,
            "flushed": ok,
            "dirty": !ok,
            "committedCell": if ok { live } else { committed },
            "liveCell": live,
            "checkpoint": ckpt,
        }))
    }

    /// Read the committed host.fs path namespace for THIS session from `fs_files`. Returned as a
    /// shareable snapshot so the per-eval fs handler closure can overlay staged ops on a stable
    /// committed view (read-after-write within a cell, list/stat consistency). Cheap: paths+sizes,
    /// no bodies. Best-effort: a query error yields an empty namespace (treated as no files).
    fn read_fs_committed(&self) -> FsCommitted {
        #[derive(serde::Deserialize)]
        struct Row {
            path: String,
            r2_key: String,
            size: i64,
        }
        let rows: Vec<Row> = self
            .store()
            .query_typed("SELECT path, r2_key, size FROM fs_files;", None)
            .unwrap_or_default();
        let mut map = BTreeMap::new();
        for r in rows {
            map.insert(
                r.path,
                FsMeta {
                    r2_key: r.r2_key,
                    size: r.size,
                },
            );
        }
        Rc::new(map)
    }

    /// UNIFIED-FS MERGE: write the committed file index to R2 at `fs/<doId>/.engram/manifest.json`
    /// (the @engram/fs `manifestKey()`), mirroring `EngramFs.exportManifest`'s shape so an EXTERNAL
    /// consumer (a container, another binding) can read the session's file namespace + fsVersion
    /// straight from R2 with NO SQLite access. Called AFTER flush_staged_fs at each checkpoint (both
    /// the delta and full-base commit paths) and after reconcile_fs_files. Best-effort: a failure here
    /// must NEVER fail the checkpoint (the manifest is a convenience export, not the coherence
    /// authority — fs_files remains the truth). The shape matches index.ts:236-252:
    ///   { doId, fsVersion, exportedMs, files:[{path,size,etag,sha256,origin}] } (sha256 null today).
    async fn export_manifest(&self) {
        #[derive(serde::Deserialize)]
        struct Row {
            path: String,
            size: i64,
            etag: Option<String>,
            origin: Option<String>,
        }
        let store = self.store();
        let rows: Vec<Row> = store
            .query_typed(
                "SELECT path, size, etag, origin FROM fs_files ORDER BY path ASC;",
                None,
            )
            .unwrap_or_default();
        let files: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| {
                json!({
                    "path": r.path,
                    "size": r.size,
                    "etag": r.etag,
                    "sha256": serde_json::Value::Null,
                    "origin": r.origin,
                })
            })
            .collect();
        let doc = json!({
            "doId": self.do_id,
            "fsVersion": read_fs_version(&store),
            "exportedMs": now_ms(),
            "files": files,
        })
        .to_string();
        let binding = self.vfs_binding();
        let key = format!("fs/{}/.engram/manifest.json", self.do_id);
        // Best-effort PUT: a failure leaves the prior manifest (or none) — never fails the checkpoint.
        let _ = self
            .r2_put_resilient(&binding, &key, doc.into_bytes())
            .await;
    }

    /// Flush this cell's STAGED host.fs mutations to durable storage as part of `store_ckpt` — i.e.
    /// IN THE SAME COMMIT ORDERING as the heap manifest, per the SANDBOX-API staged-commit coherence
    /// invariant. Body bytes are written to R2 FIRST (write-ordering rule: a body is durable before
    /// its `fs_files` meta row references it), then the `fs_files` rows are upserted/deleted via the
    /// provided store (the same store handle that commits the manifest txn). The staged buffer is
    /// drained on the way out, whether or not it was non-empty, so a subsequent cell starts clean.
    /// A crash before this runs drops the in-memory staged set with the rolled-back heap (O3).
    async fn flush_staged_fs<S: KernelStore>(&self, store: &S, cell: i64) {
        let ops: Vec<FsStage> = std::mem::take(&mut *self.staged_fs.borrow_mut());
        if ops.is_empty() {
            return;
        }
        // Resolve binding from config (default SNAPSHOTS); prefix is hard-bound to the DO id.
        let cfg: serde_json::Value =
            serde_json::from_str(&read_str(store, "config").unwrap_or_else(|| "{}".into()))
                .unwrap_or_else(|_| json!({}));
        let binding = cfg
            .get("fs")
            .and_then(|f| f.get("binding"))
            .and_then(|v| v.as_str())
            .unwrap_or("SNAPSHOTS")
            .to_string();
        let prefix = format!("fs/{}/", self.do_id);
        let bucket = match self.env.bucket(&binding) {
            Ok(b) => b,
            Err(_) => return, // bucket missing: nothing to flush (writes already returned ok in-cell)
        };
        // Collapse to last-op-per-path (a path written then deleted in one cell ends deleted, etc.),
        // preserving the engine's per-cell semantics, then apply in a deterministic path order.
        let mut last: BTreeMap<String, FsStageOp> = BTreeMap::new();
        for o in ops {
            last.insert(o.path, o.op);
        }
        let now = now_ms();
        // BUG-3 FIX (parked-continuation vs mutex-free vfs-write clobber): vfs-write/sandbox/worker
        // arms commit to the SAME fs/<doId>/ + fs_files namespace WITHOUT the eval mutex. If this
        // eval parked on a host call, such a frame write could have committed (and been ACKed
        // durable) to a staged path during the park. Last-writer-wins below would silently destroy
        // it. If fsVersion advanced since eval start, a mutex-free write landed: do NOT clobber a
        // path now owned by an `origin='frame'` row. (No-race path: byte-identical to before.)
        #[derive(serde::Deserialize)]
        struct OriginRow {
            origin: String,
        }
        let fs_raced = read_fs_version(store) > self.eval_start_fs_version.get();
        for (path, op) in last {
            let key = format!("{}{}", prefix, path.trim_start_matches('/'));
            if fs_raced {
                let cur: Vec<OriginRow> = self
                    .store()
                    .query_typed(
                        "SELECT origin FROM fs_files WHERE path=?;",
                        Some(vec![path.clone().into()]),
                    )
                    .unwrap_or_default();
                if cur.first().map(|r| r.origin == "frame").unwrap_or(false) {
                    // A mutex-free frame write won this path during the eval park; preserve the
                    // ACKed durable write rather than silently overwriting/deleting it.
                    continue;
                }
            }
            match op {
                FsStageOp::Write(bytes) => {
                    let size = bytes.len() as i64;
                    // Body to R2 first (durable before the meta row references it).
                    if self.r2_put_resilient(&binding, &key, bytes).await.is_err() {
                        // R2 unavailable: skip the meta row so the namespace stays coherent (the
                        // file is simply absent on restore — never a dangling reference).
                        continue;
                    }
                    let _ = store.exec(
                        "INSERT INTO fs_files(path,r2_key,size,cell,created_ms,origin) VALUES(?,?,?,?,?,'cell')                          ON CONFLICT(path) DO UPDATE SET r2_key=excluded.r2_key, size=excluded.size, cell=excluded.cell, created_ms=excluded.created_ms, origin='cell';",
                        Some(vec![path.clone().into(), key.clone().into(), size.into(), cell.into(), now.into()]),
                    );
                }
                FsStageOp::Delete => {
                    let _ = store.exec(
                        "DELETE FROM fs_files WHERE path=?;",
                        Some(vec![path.clone().into()]),
                    );
                    let _ = bucket.delete(&key).await; // best-effort body GC; orphan is harmless
                }
            }
        }
        // UNIFIED-FS MERGE: this cell mutated the durable fs — bump fsVersion in the SAME store
        // handle (so it lands in the same coalesced flush as the manifest txn, preserving staged-
        // commit coherence). Early-returned above when there were no ops, so this only fires on a
        // real mutation.
        bump_fs_version(store);
    }

    // ─── VFS-* out-of-band file I/O (DO-side, never the VM) ──────────────────────────────────────
    //
    // These service file uploads/downloads/listing DIRECTLY against the host.fs R2 store, reusing
    // its EXACT durability primitives so an uploaded file is the SAME object a later `config.fs.
    // provider:"r2"` cell sees via `fs.*` / `host.fs.*`:
    //   - bucket  = config.fs.binding || "SNAPSHOTS"  (mirror of flush_staged_fs)
    //   - prefix  = "fs/<doId>/"                       (HARD-bound to the DO id, never the frame)
    //   - path    = norm_fs_path(frame.path)           (REQUIRED; rejects `..`/NUL — isolation)
    //   - key     = "<prefix><normpath>"               (identical to r2_fs_op)
    //   - meta    = `fs_files(path,r2_key,size,cell,created_ms)` row (exists iff body durable in R2)
    //
    // Unlike the eval path (which STAGES fs ops and flushes them in the heap checkpoint), vfs-write
    // commits IMMEDIATELY (body to R2 FIRST, then the meta row) — there is no eval checkpoint for an
    // out-of-band upload. This is by design; it does NOT couple to the heap version, so a cell that
    // already captured its committed view at eval-start won't see a concurrently-arriving upload until
    // its next eval. The DO is single-threaded per id, so these `await`s never race a flush_staged_fs.

    /// Resolve the host.fs R2 bucket binding for THIS session (config.fs.binding || "SNAPSHOTS"),
    /// exactly as flush_staged_fs does. The `fs/<doId>/` prefix is NEVER read from config.
    fn vfs_binding(&self) -> String {
        let store = self.store();
        let cfg: serde_json::Value =
            serde_json::from_str(&read_str(&store, "config").unwrap_or_else(|| "{}".into()))
                .unwrap_or_else(|_| json!({}));
        cfg.get("fs")
            .and_then(|f| f.get("binding"))
            .and_then(|v| v.as_str())
            .unwrap_or("SNAPSHOTS")
            .to_string()
    }

    /// Look up the committed `fs_files` meta row for a normalized path: (r2_key, size, created_ms).
    fn vfs_meta(&self, path: &str) -> Option<(String, i64, i64)> {
        #[derive(serde::Deserialize)]
        struct Row {
            r2_key: String,
            size: i64,
            created_ms: i64,
        }
        let rows: Vec<Row> = self
            .store()
            .query_typed(
                "SELECT r2_key, size, created_ms FROM fs_files WHERE path=?;",
                Some(vec![path.into()]),
            )
            .unwrap_or_default();
        rows.into_iter()
            .next()
            .map(|r| (r.r2_key, r.size, r.created_ms))
    }

    /// vfs-write `{path, dataB64, offset?, truncate?}` -> `{ok, bytesWritten, error?}`.
    /// Commits durably + immediately: body to R2 FIRST (r2_put_resilient), then upsert the
    /// `fs_files` row with the SAME SQL as flush_staged_fs. R2 has no partial PUT, so offset/append
    /// is read-modify-write of the whole object (GET existing -> splice at offset -> PUT). `truncate`
    /// (or offset 0 with no existing file) replaces. Append = offset == current size.
    async fn vfs_write(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let raw_path = msg.get("path").and_then(|v| v.as_str()).unwrap_or("");
        // CWD-aware resolution: relative paths resolve against the frame cwd (default /workspace).
        let cwd = msg
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or(WORKSPACE_ROOT);
        let path = match norm_fs_path_cwd(raw_path, cwd) {
            Ok(p) => p,
            Err(_) => {
                return Ok(json!({"t":"vfs-write-result","id":id,"ok":false,
                    "error":"EINVAL: bad path"}))
            }
        };
        let data_b64 = msg.get("dataB64").and_then(|v| v.as_str()).unwrap_or("");
        let new_bytes = match b64_decode(data_b64) {
            Some(b) => b,
            None => {
                return Ok(json!({"t":"vfs-write-result","id":id,"ok":false,
                    "error":"EINVAL: bad base64 dataB64"}))
            }
        };
        let truncate = msg
            .get("truncate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let offset = msg
            .get("offset")
            .and_then(|v| v.as_f64())
            .map(|f| f as usize);

        let binding = self.vfs_binding();
        let prefix = format!("fs/{}/", self.do_id);
        let key = format!("{}{}", prefix, path.trim_start_matches('/'));

        // Assemble the full object to PUT. For a plain replace (truncate, or no offset and no
        // existing file) we PUT `new_bytes` as-is. For offset/append we GET the existing committed
        // body and splice `new_bytes` in at `offset` (zero-filling any gap past current end).
        let existing = self.vfs_meta(&path);
        let full: Vec<u8> = if truncate || (offset.is_none() && existing.is_none()) {
            new_bytes
        } else {
            // read-modify-write: load the current committed body (empty if none).
            let mut base: Vec<u8> = if existing.is_some() {
                match self.env.bucket(&binding) {
                    Ok(bucket) => match bucket.get(&key).execute().await {
                        Ok(Some(obj)) => match obj.body() {
                            Some(body) => body.bytes().await.unwrap_or_default(),
                            None => Vec::new(),
                        },
                        _ => Vec::new(),
                    },
                    Err(_) => Vec::new(),
                }
            } else {
                Vec::new()
            };
            let at = offset.unwrap_or(base.len());
            if at > base.len() {
                base.resize(at, 0u8); // zero-fill a sparse gap
            }
            let end = at + new_bytes.len();
            if end > base.len() {
                base.resize(end, 0u8);
            }
            base[at..end].copy_from_slice(&new_bytes);
            base
        };

        let size = full.len() as i64;
        let bytes_written = if truncate || (offset.is_none() && existing.is_none()) {
            size
        } else {
            // bytes actually placed by this call (the spliced window length).
            msg.get("dataB64")
                .and_then(|v| v.as_str())
                .map(|s| b64_decode(s).map(|b| b.len()).unwrap_or(0))
                .unwrap_or(0) as i64
        };

        // Body to R2 FIRST (durable before the meta row references it), then upsert the meta row.
        if self.r2_put_resilient(&binding, &key, full).await.is_err() {
            let mut dp = Datapoint::new("vfs-write");
            dp.ok = false;
            dp.error_name = "r2-put-failed".into();
            self.emit(&dp);
            return Ok(json!({"t":"vfs-write-result","id":id,"ok":false,
                "error":"FsError: R2 put failed"}));
        }
        let now = now_ms();
        let store = self.store();
        let _ = store.exec(
            "INSERT INTO fs_files(path,r2_key,size,cell,created_ms,origin) VALUES(?,?,?,?,?,'frame') \
             ON CONFLICT(path) DO UPDATE SET r2_key=excluded.r2_key, size=excluded.size, cell=excluded.cell, created_ms=excluded.created_ms, origin='frame';",
            Some(vec![
                path.clone().into(),
                key.into(),
                size.into(),
                (-1i64).into(),
                now.into(),
            ]),
        );
        // UNIFIED-FS MERGE: an out-of-band vfs-write is a durable fs mutation — bump fsVersion and
        // refresh the exported manifest so external readers see the new file index immediately.
        bump_fs_version(&store);
        self.export_manifest().await;
        self.emit(&Datapoint::new("vfs-write"));
        Ok(
            json!({"t":"vfs-write-result","id":id,"ok":true,"bytesWritten":bytes_written,"fsVersion":read_fs_version(&store)}),
        )
    }

    /// vfs-read `{path, offset?, len?}` -> `{ok, dataB64, eof, size?, error?}`. Client-driven
    /// chunked/streamable reads (one frame per range), like the artifact streaming model. `size` is
    /// always the TOTAL committed file size (so the client can drive the chunk loop / detect eof).
    async fn vfs_read(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let raw_path = msg.get("path").and_then(|v| v.as_str()).unwrap_or("");
        // CWD-aware resolution: relative paths resolve against the frame cwd (default /workspace).
        let cwd = msg
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or(WORKSPACE_ROOT);
        let path = match norm_fs_path_cwd(raw_path, cwd) {
            Ok(p) => p,
            Err(_) => {
                return Ok(json!({"t":"vfs-read-result","id":id,"ok":false,
                    "error":"EINVAL: bad path"}))
            }
        };
        // The engram-sandbox CONTAINER writes bodies DIRECTLY to R2 at the identical canonical key
        // fs/<doId>/<rel> with NO committed fs_files row, so vfs_meta() misses them. Fall back to a
        // direct R2 HEAD at the same key (the transparent container->cell read of the shared VFS).
        let (key, size, _created) = match self.vfs_meta(&path) {
            Some(m) => m,
            None => {
                let binding0 = self.vfs_binding();
                let key0 = format!("fs/{}/{}", self.do_id, path.trim_start_matches('/'));
                let head_size = match self.env.bucket(&binding0) {
                    Ok(bucket) => match bucket.head(&key0).await {
                        Ok(Some(obj)) => Some(obj.size() as i64),
                        _ => None,
                    },
                    Err(_) => None,
                };
                match head_size {
                    Some(sz) => (key0, sz, 0i64),
                    None => {
                        return Ok(json!({"t":"vfs-read-result","id":id,"ok":false,"eof":true,
                            "error":"ENOENT: no such file"}))
                    }
                }
            }
        };
        let total = size.max(0) as u64;
        let offset = msg
            .get("offset")
            .and_then(|v| v.as_f64())
            .map(|f| (f as u64).min(total))
            .unwrap_or(0);
        let len = msg.get("len").and_then(|v| v.as_f64()).map(|f| f as u64);

        let binding = self.vfs_binding();
        let bucket = self
            .env
            .bucket(&binding)
            .map_err(|e| to_err(JsValue::from_str(&format!("FsError: bucket — {e}"))))?;
        let mut gb = bucket.get(&key);
        if offset > 0 || len.is_some() {
            let length = len.unwrap_or(total.saturating_sub(offset));
            gb = gb.range(worker::Range::OffsetWithLength { offset, length });
        }
        let bytes: Vec<u8> = match gb.execute().await {
            Ok(Some(obj)) => match obj.body() {
                Some(body) => body
                    .bytes()
                    .await
                    .map_err(|e| to_err(JsValue::from_str(&format!("{e}"))))?,
                None => Vec::new(),
            },
            // Committed meta but missing body = torn write (per SANDBOX-API): report distinctly.
            _ => {
                let mut dp = Datapoint::new("vfs-read");
                dp.ok = false;
                dp.error_name = "torn-file".into();
                self.emit(&dp);
                return Ok(json!({"t":"vfs-read-result","id":id,"ok":false,"eof":true,
                    "size":size,"error":"ENOENT: torn file (committed meta, body absent)"}));
            }
        };
        let eof = offset + (bytes.len() as u64) >= total;
        self.emit(&Datapoint::new("vfs-read"));
        Ok(json!({"t":"vfs-read-result","id":id,"ok":true,
            "dataB64": b64_encode(&bytes),"eof":eof,"size":size}))
    }

    /// vfs-stat `{path}` -> `{ok, stat:{size,isFile,isDir,mtime}, error?}`. Reads the `fs_files`
    /// meta row (mtime maps to `created_ms`, which is last-write time). A path that is a prefix of
    /// other rows but has no own row is a synthetic directory.
    async fn vfs_stat(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let raw_path = msg.get("path").and_then(|v| v.as_str()).unwrap_or("");
        // CWD-aware resolution: relative paths resolve against the frame cwd (default /workspace).
        let cwd = msg
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or(WORKSPACE_ROOT);
        let path = match norm_fs_path_cwd(raw_path, cwd) {
            Ok(p) => p,
            Err(_) => {
                return Ok(json!({"t":"vfs-stat-result","id":id,"ok":false,
                    "error":"EINVAL: bad path"}))
            }
        };
        if let Some((_key, size, created)) = self.vfs_meta(&path) {
            return Ok(json!({"t":"vfs-stat-result","id":id,"ok":true,
                "stat":{"size":size,"isFile":true,"isDir":false,"mtime":created}}));
        }
        // No own row: synthetic dir if it is a parent prefix of any committed row.
        let committed = self.read_fs_committed();
        let dir = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };
        let is_dir = path == "/" || committed.keys().any(|k| k.starts_with(&dir));
        if is_dir {
            return Ok(json!({"t":"vfs-stat-result","id":id,"ok":true,
                "stat":{"size":0,"isFile":false,"isDir":true,"mtime":0}}));
        }
        // Container-written file (direct-to-R2, no fs_files row): fall back to an R2 HEAD at the
        // identical canonical key so stat() sees the shared-VFS bytes the same way read() does.
        let binding0 = self.vfs_binding();
        let key0 = format!("fs/{}/{}", self.do_id, path.trim_start_matches('/'));
        if let Ok(bucket) = self.env.bucket(&binding0) {
            if let Ok(Some(obj)) = bucket.head(&key0).await {
                return Ok(json!({"t":"vfs-stat-result","id":id,"ok":true,
                    "stat":{"size":obj.size() as i64,"isFile":true,"isDir":false,"mtime":0}}));
            }
        }
        Ok(json!({"t":"vfs-stat-result","id":id,"ok":false,"error":"ENOENT: no such file"}))
    }

    /// vfs-ls `{path}` -> `{ok, entries:[{name,size,isDir}], error?}`. Immediate children of a dir
    /// prefix over the committed `fs_files` namespace (files + synthetic subdirs), deduped — same
    /// list semantics as r2_fs_op's "list". Reads ONLY this DO's per-session SQLite rows (isolation).
    fn vfs_ls(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let raw_path = msg.get("path").and_then(|v| v.as_str()).unwrap_or("");
        // CWD-aware resolution: relative paths resolve against the frame cwd (default /workspace).
        let cwd = msg
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or(WORKSPACE_ROOT);
        let path = match norm_fs_path_cwd(raw_path, cwd) {
            Ok(p) => p,
            Err(_) => {
                return Ok(json!({"t":"vfs-ls-result","id":id,"ok":false,
                    "error":"EINVAL: bad path"}))
            }
        };
        let committed = self.read_fs_committed();
        let pre = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };
        // child segment -> (size, isDir). A file child has its own row (isDir=false); a dir child
        // appears as the first segment of a deeper path with no own row.
        let mut seen: std::collections::BTreeMap<String, (i64, bool)> =
            std::collections::BTreeMap::new();
        for (full, meta) in committed.iter() {
            if let Some(rest) = full.strip_prefix(pre.as_str()) {
                if rest.is_empty() {
                    continue;
                }
                match rest.split_once('/') {
                    None => {
                        // immediate file child (a real row always wins over a synthetic dir).
                        seen.insert(rest.to_string(), (meta.size, false));
                    }
                    Some((name, _)) => {
                        // deeper path => `name` is a subdirectory (unless a file row claims it).
                        seen.entry(name.to_string()).or_insert((0, true));
                    }
                }
            }
        }
        let list: Vec<serde_json::Value> = seen
            .into_iter()
            .map(|(name, (size, is_dir))| json!({"name":name,"size":size,"isDir":is_dir}))
            .collect();
        Ok(json!({"t":"vfs-ls-result","id":id,"ok":true,"entries":list}))
    }

    /// vfs-sync `{}` -> `{ok, files:[{path,size}]}`. Reconcile the fs_files namespace from the live
    /// R2 prefix `fs/<doId>/`, then return the committed file list. The engram-sandbox container
    /// writes bodies DIRECTLY to R2 (s3fs mount), so files it creates carry NO fs_files row and are
    /// invisible to host.fs / vfs-* until this runs — it is the explicit, on-demand container->cell
    /// half of the shared-VFS round-trip (mirrors worker_invoke's post-invoke reconcile). Acquires
    /// the mutex ONLY around the reconcile so it cannot interleave an eval's staged flush.
    async fn vfs_sync(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
        self.reconcile_fs_files().await;
        let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
        let committed = self.read_fs_committed();
        let files: Vec<serde_json::Value> = committed
            .iter()
            .map(|(p, m)| json!({"path": p, "size": m.size}))
            .collect();
        let mut dp = Datapoint::new("vfs-sync");
        dp.cell = files.len() as i64; // record reconciled file count on the `cell` double
        self.emit(&dp);
        Ok(json!({"t":"vfs-sync-result","id":id,"ok":true,"files":files}))
    }

    // ─── WORKER REGISTRY — content-addressed Dynamic-Worker-Loader compute ──────────────────────
    //
    // The "muscle" tier: a STATIC, hash-identified JS source registered once and invokable many
    // times in a FRESH isolate, decoupled from the single-threaded durable kernel DO. The isolate
    // shares the SAME R2-backed VFS the kernel + vfs-* frames use (fs/<doId>/ prefix), so a
    // hash-worker reads/writes the exact files a cell sees via host.fs / vfs-*.
    //
    // ISOLATION (held end-to-end):
    //   (a) hash = sha256(source) — immutable, content-addressed; same source = same hash.
    //   (b) warm cache keys on the codeId (glue: `wkr1:<doIdShort>:<hash>`) so two sessions sharing
    //       a hash get SEPARATE isolates with SEPARATE VFS env — never a cross-session R2 leak.
    //   (c) the fs/<doId>/ prefix + path normalization are computed gateway-side from the TRUSTED
    //       do_id this DO passes; the dynamic worker can never choose either.
    //   (d) globalOutbound: null (glue) — VFS RPC is the worker's ONLY I/O channel.
    //   (e) the per-session registry_workers row is the invoke gate (see worker_invoke).

    /// worker-register `{id, source}` -> `{ok, hash, bytes, cached}` | `{ok:false, error}`.
    /// Content-addressed + idempotent: hash = lowercase-hex sha256(source); the source body is
    /// stored GLOBALLY at R2 `workers/<hash>.js` (immutable — a double PUT of identical content is
    /// harmless), and a per-session `registry_workers` row is upserted (the invoke gate). `cached`
    /// is true iff this session already had a row for the hash (no work beyond the response).
    async fn worker_register(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        const MAX_SOURCE_BYTES: usize = 512 * 1024; // mirrors the stdlib 500KB source-cap culture
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let source = msg.get("source").and_then(|v| v.as_str()).unwrap_or("");
        let bytes = source.len() as i64;
        if source.len() > MAX_SOURCE_BYTES {
            let mut dp = Datapoint::new("worker-register");
            dp.ok = false;
            dp.error_name = "SourceTooLargeError".into();
            dp.size_raw = bytes;
            self.emit(&dp);
            return Ok(json!({"t":"worker-register-result","id":id,"ok":false,
                "error":{"name":"SourceTooLargeError",
                    "message":format!("source {}B exceeds cap {}B", source.len(), MAX_SOURCE_BYTES)}}));
        }
        let hash = sha256_hex(source.as_bytes());

        // Already registered by THIS session? Idempotent no-op (the R2 body is immutable + global).
        let cached = self.registry_has(&hash);

        if !cached {
            // Store the source body GLOBALLY (immutable, content-addressed). r2_put_resilient gives
            // the same retry/backoff/breaker discipline as the host.fs body writes.
            let key = format!("workers/{}.js", hash);
            if self
                .r2_put_resilient("SNAPSHOTS", &key, source.as_bytes().to_vec())
                .await
                .is_err()
            {
                let mut dp = Datapoint::new("worker-register");
                dp.ok = false;
                dp.error_name = "R2WriteError".into();
                dp.size_raw = bytes;
                self.emit(&dp);
                return Ok(json!({"t":"worker-register-result","id":id,"ok":false,
                    "error":{"name":"R2WriteError","message":"failed to persist source to R2"}}));
            }
            let now = now_ms();
            let _ = self.store().exec(
                "INSERT INTO registry_workers(hash,bytes,created_ms) VALUES(?,?,?) \
                 ON CONFLICT(hash) DO UPDATE SET bytes=excluded.bytes;",
                Some(vec![hash.clone().into(), bytes.into(), now.into()]),
            );
        }

        let mut dp = Datapoint::new("worker-register");
        dp.size_raw = bytes;
        dp.label = if cached {
            "cached".into()
        } else {
            "stored".into()
        };
        self.emit(&dp);
        Ok(json!({"t":"worker-register-result","id":id,"ok":true,
            "hash":hash,"bytes":bytes,"cached":cached}))
    }

    /// worker-invoke `{id, hash, input?, timeoutMs?, cpuMs?}` -> `{ok, output, ms}` |
    /// `{ok:false, error, ms}`. Validates the hash format + the per-session invoke gate, loads the
    /// source from R2 `workers/<hash>.js`, runs it in a fresh Worker-Loader isolate (glue), races
    /// the run against `timeoutMs`, reconciles the shared fs_files namespace, emits AE, replies.
    async fn worker_invoke(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let t0 = now_ms();
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let hash = msg.get("hash").and_then(|v| v.as_str()).unwrap_or("");
        let reply_err = |name: &str, message: String, ms: f64| {
            json!({"t":"worker-invoke-result","id":id,"ok":false,
                "error":{"name":name,"message":message},"ms":ms})
        };

        // hash format: exactly 64 lowercase hex chars (defends the R2 key + the codeId).
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Ok(reply_err(
                "ContractError",
                "hash must match ^[0-9a-f]{64}$".into(),
                0.0,
            ));
        }
        // INVOKE GATE: this session must have registered the hash (closes "any authed session can
        // run any source whose hash it learned"). register is cheap + idempotent.
        if !self.registry_has(hash) {
            let mut dp = Datapoint::new("worker-invoke");
            dp.ok = false;
            dp.error_name = "NotRegisteredError".into();
            self.emit(&dp);
            return Ok(reply_err(
                "NotRegisteredError",
                "hash not registered by this session; call worker-register first".into(),
                0.0,
            ));
        }

        // timeout/cpu caps (clamped). timeoutMs default 30000 cap 120000; cpuMs default 5000.
        let timeout_ms = msg
            .get("timeoutMs")
            .and_then(|v| v.as_f64())
            .map(|f| f as u64)
            .unwrap_or(30000)
            .clamp(1, 120_000);
        let cpu_ms = msg
            .get("cpuMs")
            .and_then(|v| v.as_f64())
            .map(|f| f as u64)
            .unwrap_or(5000)
            .clamp(1, 60_000);
        let input_json = msg
            .get("input")
            .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "null".into()))
            .unwrap_or_else(|| "null".into());
        const MAX_INPUT_BYTES: usize = 1024 * 1024;
        if input_json.len() > MAX_INPUT_BYTES {
            return Ok(reply_err(
                "ContractError",
                format!("input JSON {}B exceeds 1MB cap", input_json.len()),
                now_ms() - t0,
            ));
        }

        // Load the GLOBAL immutable source body. A durable miss/exhaust = the R2 object is gone
        // (registry row without body) — surface a typed error, never wedge.
        let key = format!("workers/{}.js", hash);
        let source: String = match self.r2_get_resilient(&key).await {
            R2Read::Got(arr) => {
                let mut b = vec![0u8; arr.length() as usize];
                arr.copy_to(&mut b);
                match String::from_utf8(b) {
                    Ok(s) => s,
                    Err(_) => {
                        return Ok(reply_err(
                            "WorkerRuntimeError",
                            "registered source is not valid UTF-8".into(),
                            now_ms() - t0,
                        ))
                    }
                }
            }
            R2Read::Missing | R2Read::Exhausted => {
                let mut dp = Datapoint::new("worker-invoke");
                dp.ok = false;
                dp.error_name = "NotRegisteredError".into();
                self.emit(&dp);
                return Ok(reply_err(
                    "NotRegisteredError",
                    "source body missing in R2 (registry desync)".into(),
                    now_ms() - t0,
                ));
            }
        };

        // Run the loaded worker via the JS glue: env.LOADER.get(codeId, cb) -> stub.getEntrypoint
        // -> ep.fetch(POST input). The glue builds the VfsGateway from ctx.exports (fail-closed if
        // unavailable). lib.rs passes env + ctx as raw JsValue (no LOADER calls in Rust).
        let opts_json = json!({"timeoutMs": timeout_ms, "cpuMs": cpu_ms}).to_string();
        let env_js: &JsValue = self.env.as_ref();
        // ctx for ctx.exports.VfsGateway. `&State` does not expose the raw DO ctx JsValue by
        // reference (worker-rs keeps it private), so we pass NULL HERE — but the wiring IS LIVE and
        // proven (NOT a future TODO): entry.ts's `KernelDO` subclass captures the real DO ctx into
        // `globalThis.__ENGRAM_DO_CTX` keyed by the trusted do_id (entry.ts), and kernel-glue.mjs
        // resolves it out-of-band from that map to mint `captured.exports.VfsGateway({props:{doId}})`.
        // So NULL ctx is EXPECTED and the glue supplies the gateway; it fails closed (typed
        // RegistryUnavailableError) only if the map lookup also misses. (Never falls back to handing
        // the dynamic worker the raw bucket — that is the leak.)
        let ctx_js = JsValue::NULL;
        let promise = registry_invoke(
            env_js,
            &ctx_js,
            &self.do_id,
            hash,
            &source,
            &input_json,
            &opts_json,
        );

        let out_json: String = match race_timeout(JsFuture::from(promise), timeout_ms).await {
            Some(Ok(v)) => v.as_string().unwrap_or_else(|| {
                json!({"ok":false,"error":{"name":"WorkerRuntimeError",
                    "message":"worker returned a non-string result"}})
                .to_string()
            }),
            Some(Err(e)) => {
                // glue threw (e.g. RegistryUnavailableError / loader failure) — surface its name+msg.
                let (name, message) = js_error_parts(&e);
                json!({"ok":false,"error":{"name":name,"message":message}}).to_string()
            }
            None => {
                let mut dp = Datapoint::new("worker-invoke");
                dp.ok = false;
                dp.error_name = "WorkerTimeoutError".into();
                dp.total_server_ms = now_ms() - t0;
                self.emit(&dp);
                return Ok(reply_err(
                    "WorkerTimeoutError",
                    format!("worker exceeded {}ms wall timeout", timeout_ms),
                    now_ms() - t0,
                ));
            }
        };

        // RECONCILE the shared fs_files namespace: the VfsGateway writes to R2 directly (it has no
        // DO storage), so after the invoke we re-list fs/<doId>/ and upsert/delete fs_files rows
        // with the SAME SQL as flush_staged_fs, so a later cell / vfs-* sees gateway-written files.
        // Acquire self.mutex for the reconcile ONLY, so it cannot interleave an eval's staged flush.
        // Gateway writes are vfs-write-semantics (immediate-durable, OUTSIDE the eval staged-commit).
        {
            let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
            self.reconcile_fs_files().await;
            let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
        }

        let ms = now_ms() - t0;
        // The harness envelope is {ok, output|error}. Parse it; cap the output size.
        const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
        let parsed: serde_json::Value = serde_json::from_str(&out_json).unwrap_or_else(|_| {
            json!({"ok":false,
                "error":{"name":"WorkerRuntimeError","message":"invalid worker envelope"}})
        });
        let ok = parsed.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        let mut dp = Datapoint::new("worker-invoke");
        dp.total_server_ms = ms;
        if ok {
            let output = parsed
                .get("output")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let out_str = serde_json::to_string(&output).unwrap_or_else(|_| "null".into());
            if out_str.len() > MAX_OUTPUT_BYTES {
                dp.ok = false;
                dp.error_name = "OutputTooLargeError".into();
                self.emit(&dp);
                return Ok(reply_err(
                    "OutputTooLargeError",
                    format!(
                        "output JSON {}B exceeds 1MB cap (use env.VFS for large data)",
                        out_str.len()
                    ),
                    ms,
                ));
            }
            dp.size_raw = out_str.len() as i64;
            self.emit(&dp);
            Ok(json!({"t":"worker-invoke-result","id":id,"ok":true,"output":output,"ms":ms}))
        } else {
            let err = parsed
                .get("error")
                .cloned()
                .unwrap_or_else(|| json!({"name":"WorkerRuntimeError","message":"worker failed"}));
            dp.ok = false;
            dp.error_name = err
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("WorkerRuntimeError")
                .to_string();
            self.emit(&dp);
            Ok(json!({"t":"worker-invoke-result","id":id,"ok":false,"error":err,"ms":ms}))
        }
    }

    /// worker-list `{id}` -> `{ok, workers:[{hash, bytes, createdMs}]}`. This session's registry
    /// index (NOT the global R2 namespace) — reads ONLY this DO's per-session rows (isolation).
    fn worker_list(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        #[derive(serde::Deserialize)]
        struct Row {
            hash: String,
            bytes: i64,
            created_ms: i64,
        }
        let rows: Vec<Row> = self
            .store()
            .query_typed(
                "SELECT hash, bytes, created_ms FROM registry_workers ORDER BY created_ms ASC;",
                None,
            )
            .unwrap_or_default();
        let workers: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| json!({"hash":r.hash,"bytes":r.bytes,"createdMs":r.created_ms}))
            .collect();
        Ok(json!({"t":"worker-list-result","id":id,"ok":true,"workers":workers}))
    }

    /// sandbox_frame: SDK `s.sandbox.*` over the kernel WS. Out-of-band (NO self.mutex, never touches
    /// the VM heap) DO-side fetch to the engram-sandbox container worker over the SHARED R2 VFS. The
    /// trusted KERNEL do_id is sent as `x-engram-session` (=> the sandbox mounts `fs/<doId>/` as
    /// /session, the SAME keys host.fs / vfs-* read/write); the Bearer key is read from ENV and added
    /// HERE — it NEVER enters the VM heap nor any frame echoed to the client. Capability-gated by
    /// config.sandbox. URL is a FIXED trusted endpoint (no SSRF allowlist needed). Echoes `id` back.
    async fn sandbox_frame(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let op = msg.get("op").and_then(|v| v.as_str()).unwrap_or("");
        // Capability gate: only sessions with config.sandbox:true get the route.
        let cfg: serde_json::Value =
            serde_json::from_str(&read_str(&self.store(), "config").unwrap_or_else(|| "{}".into()))
                .unwrap_or_else(|_| json!({}));
        let enabled = cfg
            .get("sandbox")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !enabled {
            return Ok(json!({"t":"sandbox-result","id":id,"ok":false,
                "error":"SandboxUnavailable: config.sandbox not enabled"}));
        }
        let url = self
            .env
            .var("ENGRAM_SANDBOX_URL")
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_default();
        let key = self
            .env
            .secret("ENGRAM_SANDBOX_KEY")
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default();
        if url.is_empty() || key.is_empty() {
            return Ok(json!({"t":"sandbox-result","id":id,"ok":false,
                "error":"SandboxUnavailable: sandbox endpoint/key not configured"}));
        }

        // Map the frame op -> route + HTTP method + JSON body (mutations) / query (reads). Mirrors the
        // engram-sandbox routes (/exec /git /files /expose /mount /unmount) and the glue _doSandbox.
        let mut method = Method::Post;
        let route;
        let mut body: Option<serde_json::Value> = None;
        let mut query = String::new();
        match op {
            "exec" => {
                route = "/exec";
                let cmd = msg.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
                let mut b = json!({ "cmd": cmd });
                if let Some(cwd) = msg.get("cwd").and_then(|v| v.as_str()) {
                    b["cwd"] = json!(cwd);
                }
                body = Some(b);
            }
            "git" => {
                route = "/git";
                body = Some(json!({
                    "op": msg.get("gitOp").and_then(|v| v.as_str())
                        .or_else(|| msg.get("subop").and_then(|v| v.as_str()))
                        .unwrap_or("checkout"),
                    "repo": msg.get("repo").and_then(|v| v.as_str()),
                    "branch": msg.get("branch").and_then(|v| v.as_str()),
                    "dir": msg.get("dir").and_then(|v| v.as_str()),
                }));
            }
            "write" | "writeFile" => {
                route = "/files";
                body = Some(json!({
                    "op": "write",
                    "path": msg.get("path").and_then(|v| v.as_str()).unwrap_or(""),
                    "content": msg.get("content").and_then(|v| v.as_str()).unwrap_or(""),
                }));
            }
            "read" | "readFile" => {
                route = "/files";
                method = Method::Get;
                let path = msg.get("path").and_then(|v| v.as_str()).unwrap_or("");
                query = format!("op=read&path={}", urlencode(path));
            }
            "list" => {
                route = "/files";
                method = Method::Get;
                let path = msg.get("path").and_then(|v| v.as_str()).unwrap_or("");
                query = format!("op=list&path={}", urlencode(path));
            }
            "expose" => {
                route = "/expose";
                let port = msg.get("port").and_then(|v| v.as_i64()).unwrap_or(0);
                body = Some(json!({ "port": port }));
            }
            "mount" => {
                route = "/mount";
            }
            "unmount" => {
                route = "/unmount";
            }
            _ => {
                return Ok(json!({"t":"sandbox-result","id":id,"ok":false,
                    "error":format!("SandboxError: unknown op '{}'", op)}));
            }
        }

        let full = if query.is_empty() {
            format!("{}{}", url.trim_end_matches('/'), route)
        } else {
            format!("{}{}?{}", url.trim_end_matches('/'), route, query)
        };

        // Build the DO-side request: trusted do_id header + Bearer key (never in the VM heap).
        let headers = Headers::new();
        let _ = headers.set("authorization", &format!("Bearer {}", key));
        let _ = headers.set("x-engram-session", &self.do_id);
        let mut init = RequestInit::new();
        init.with_method(method.clone());
        if matches!(method, Method::Post) {
            if let Some(b) = &body {
                let _ = headers.set("content-type", "application/json");
                let s = serde_json::to_string(b).unwrap_or_else(|_| "{}".into());
                init.with_body(Some(wasm_bindgen::JsValue::from_str(&s)));
            }
        }
        init.with_headers(headers);

        let req = match Request::new_with_init(&full, &init) {
            Ok(r) => r,
            Err(e) => {
                return Ok(json!({"t":"sandbox-result","id":id,"ok":false,
                    "error":format!("SandboxError: bad request: {}", e)}));
            }
        };
        let mut resp = match Fetch::Request(req).send().await {
            Ok(r) => r,
            Err(e) => {
                let mut dp = Datapoint::new("sandbox");
                dp.ok = false;
                dp.error_name = "fetch-failed".into();
                self.emit(&dp);
                return Ok(json!({"t":"sandbox-result","id":id,"ok":false,
                    "error":format!("SandboxError: {}", e)}));
            }
        };
        let status = resp.status_code();
        let text = resp.text().await.unwrap_or_default();
        let value: serde_json::Value =
            serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }));
        let mut dp = Datapoint::new("sandbox");
        dp.ok = (200..300).contains(&status);
        self.emit(&dp);
        if (200..300).contains(&status) {
            Ok(json!({"t":"sandbox-result","id":id,"ok":true,"value":value}))
        } else {
            let emsg = value
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("HTTP {}", status));
            Ok(json!({"t":"sandbox-result","id":id,"ok":false,
                "error":format!("SandboxError: {}", emsg)}))
        }
    }

    /// Has THIS session registered `hash`? (the invoke gate / register idempotency check.)
    fn registry_has(&self, hash: &str) -> bool {
        #[derive(serde::Deserialize)]
        struct Row {
            #[allow(dead_code)]
            hash: String,
        }
        let rows: Vec<Row> = self
            .store()
            .query_typed(
                "SELECT hash FROM registry_workers WHERE hash=? LIMIT 1;",
                Some(vec![hash.into()]),
            )
            .unwrap_or_default();
        !rows.is_empty()
    }

    /// Reconcile fs_files from the live R2 fs/<doId>/ listing after a gateway-writing invoke. Pages
    /// the R2 list, upserts a row per object (same SQL as flush_staged_fs) and deletes rows whose
    /// body no longer exists, so the per-session SQLite namespace matches what the VfsGateway wrote.
    /// Best-effort: any R2/list error leaves the existing namespace untouched (a later invoke / the
    /// next reconcile re-syncs). Caller holds self.mutex.
    async fn reconcile_fs_files(&self) {
        let binding = self.vfs_binding();
        let prefix = format!("fs/{}/", self.do_id);
        let bucket = match self.env.bucket(&binding) {
            Ok(b) => b,
            Err(_) => return,
        };
        // Collect the live R2 keys under the prefix (paginated via cursor).
        let mut live: BTreeMap<String, i64> = BTreeMap::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut lb = bucket.list().prefix(prefix.clone()).limit(1000);
            if let Some(c) = &cursor {
                lb = lb.cursor(c.clone());
            }
            let listed = match lb.execute().await {
                Ok(l) => l,
                Err(_) => return, // list failed: leave namespace untouched (best-effort)
            };
            for obj in listed.objects() {
                let k = obj.key();
                // path = key with the fs/<doId>/ prefix stripped, normalized to a leading-slash form
                // identical to the fs_files `path` column (norm_fs_path of the suffix).
                if let Some(rest) = k.strip_prefix(&prefix) {
                    if let Ok(path) = norm_fs_path(rest) {
                        live.insert(path, obj.size() as i64);
                    }
                }
            }
            if listed.truncated() {
                cursor = listed.cursor();
                if cursor.is_none() {
                    break;
                }
            } else {
                break;
            }
        }

        let store = self.store();
        let committed = self.read_fs_committed();
        let now = now_ms();
        // Upsert every live object (body present in R2 ⇒ row exists, the fs_files invariant).
        let mut changed = false;
        for (path, size) in live.iter() {
            let key = format!("{}{}", prefix, path.trim_start_matches('/'));
            let _ = store.exec(
                "INSERT INTO fs_files(path,r2_key,size,cell,created_ms,origin) VALUES(?,?,?,?,?,'reconcile') \
                 ON CONFLICT(path) DO UPDATE SET r2_key=excluded.r2_key, size=excluded.size, origin='reconcile';",
                Some(vec![
                    path.clone().into(),
                    key.into(),
                    (*size).into(),
                    (-1i64).into(),
                    now.into(),
                ]),
            );
            changed = true;
        }
        // Delete rows whose body the gateway removed (committed but no longer live in R2).
        for path in committed.keys() {
            if !live.contains_key(path) {
                let _ = store.exec(
                    "DELETE FROM fs_files WHERE path=?;",
                    Some(vec![path.clone().into()]),
                );
                changed = true;
            }
        }
        // UNIFIED-FS MERGE: a gateway/hash-worker invoke that mutated the shared VFS bumps fsVersion
        // and refreshes the exported manifest so cells + external readers see the reconciled index.
        if changed {
            bump_fs_version(&store);
            self.export_manifest().await;
        }
    }

    fn r2_key_for(&self, cell: i64, epoch: i64) -> String {
        format!("benchrustf/{}/e{}-c{}.qjs.gz", self.do_id, epoch, cell)
    }

    /// Circuit-breaker: is R2 currently considered degraded (skip it, go straight to oplog replay)?
    /// State persists across DO requests in `meta`; cooldown uses wall-clock (advances between turns).
    fn r2_breaker_open(&self) -> bool {
        let store = self.store();
        let open_until = read_int(&store, R2_BREAKER_OPEN_UNTIL_KEY, 0);
        open_until > 0 && (now_ms() as i64) < open_until
    }

    fn r2_breaker_record_failure(&self) {
        let store = self.store();
        let fails = read_int(&store, R2_BREAKER_FAILS_KEY, 0) + 1;
        write_meta(&store, R2_BREAKER_FAILS_KEY, &fails.to_string());
        if fails >= R2_BREAKER_TRIP_AT {
            let open_until = (now_ms() + R2_BREAKER_COOLDOWN_MS) as i64;
            write_meta(&store, R2_BREAKER_OPEN_UNTIL_KEY, &open_until.to_string());
            console_log(&format!(
                "[r2-breaker] OPEN after {fails} consecutive failures; cooldown {}ms",
                R2_BREAKER_COOLDOWN_MS as i64
            ));
        }
    }

    fn r2_breaker_record_success(&self) {
        let store = self.store();
        if read_int(&store, R2_BREAKER_FAILS_KEY, 0) != 0
            || read_int(&store, R2_BREAKER_OPEN_UNTIL_KEY, 0) != 0
        {
            write_meta(&store, R2_BREAKER_FAILS_KEY, "0");
            write_meta(&store, R2_BREAKER_OPEN_UNTIL_KEY, "0");
        }
    }

    /// Resilient R2 GET for the snapshot overflow image: retry-with-(deterministic)-backoff +
    /// per-attempt timeout, distinguishing a transient failure (→ retry, then `Exhausted` keeping the
    /// session intact) from a durable miss (404-class → `Missing`). Never bubbles a 500 that the
    /// client would read as "no session, create fresh". Determinism preserved (fixed backoffs, R2
    /// reads add no entropy). The image body is read whole (range/streamed read deferred — see #10
    /// notes; SQLITE_HOT_MAX=8MB keeps most images off this path entirely).
    async fn r2_get_resilient(&self, key: &str) -> R2Read {
        if self.fault_test_enabled() {
            let target = self.fault_read_meta("r2MissOnceKey").unwrap_or_default();
            let fired = self.fault_read_meta("r2MissOnceFired").unwrap_or_default();
            if !target.is_empty() && fired != "1" && (target == key || target == "*") {
                self.fault_write_meta("r2MissOnceFired", "1");
                console_log(&format!(
                    "[fault-test] injecting one-shot durable R2 miss for key {key}"
                ));
                return R2Read::Missing;
            }
        }
        for attempt in 0..R2_GET_ATTEMPTS {
            if attempt > 0 {
                let backoff = R2_GET_BACKOFF_MS
                    .get(attempt - 1)
                    .copied()
                    .unwrap_or(*R2_GET_BACKOFF_MS.last().unwrap());
                Delay::from(Duration::from_millis(backoff)).await;
            }
            let bucket = match self.env.bucket("SNAPSHOTS") {
                Ok(b) => b,
                Err(e) => {
                    console_log(&format!(
                        "[r2-get] bucket bind failed (attempt {attempt}): {e:?}"
                    ));
                    continue;
                }
            };
            // Race the GET (head + body read) against a per-attempt timeout.
            let fut = async {
                let obj = bucket.get(key).execute().await?;
                let obj = match obj {
                    Some(o) => o,
                    None => return Ok::<Option<Uint8Array>, worker::Error>(None), // durable miss
                };
                let body = match obj.body() {
                    Some(b) => b,
                    None => return Ok(None),
                };
                let bytes = body.bytes().await?;
                let arr = Uint8Array::new_with_length(bytes.len() as u32);
                arr.copy_from(&bytes);
                Ok(Some(arr))
            };
            match race_timeout(fut, R2_GET_TIMEOUT_MS).await {
                Some(Ok(Some(arr))) => {
                    self.r2_breaker_record_success();
                    return R2Read::Got(arr);
                }
                Some(Ok(None)) => {
                    // Durable miss: NOT transient. Don't burn retries; recover via oplog replay.
                    console_log(&format!("[r2-get] durable miss for key {key}"));
                    self.r2_breaker_record_success();
                    return R2Read::Missing;
                }
                Some(Err(e)) => {
                    console_log(&format!(
                        "[r2-get] transient error attempt {attempt}: {e:?}"
                    ));
                }
                None => {
                    console_log(&format!(
                        "[r2-get] timeout (>{}ms) attempt {attempt}",
                        R2_GET_TIMEOUT_MS
                    ));
                }
            }
        }
        self.r2_breaker_record_failure();
        console_log(&format!(
            "[r2-get] EXHAUSTED {R2_GET_ATTEMPTS} attempts for key {key}"
        ));
        R2Read::Exhausted
    }

    /// Resilient R2 PUT for host.fs body bytes — mirrors `r2_get_resilient`'s retry-with-backoff +
    /// per-attempt timeout + circuit-breaker (#10). Returns Ok on a durable put, Err after exhausting
    /// retries (caller then SKIPS the meta row so the namespace stays coherent — the file is absent on
    /// restore, never a dangling reference). Determinism preserved (fixed backoffs; R2 adds no entropy).
    async fn r2_put_resilient(
        &self,
        binding: &str,
        key: &str,
        bytes: Vec<u8>,
    ) -> std::result::Result<(), ()> {
        if self.r2_breaker_open() {
            console_log("[r2-fs-put] breaker OPEN — skipping put");
            return Err(());
        }
        for attempt in 0..R2_GET_ATTEMPTS {
            if attempt > 0 {
                let backoff = R2_GET_BACKOFF_MS
                    .get(attempt - 1)
                    .copied()
                    .unwrap_or(*R2_GET_BACKOFF_MS.last().unwrap());
                Delay::from(Duration::from_millis(backoff)).await;
            }
            let bucket = match self.env.bucket(binding) {
                Ok(b) => b,
                Err(e) => {
                    console_log(&format!(
                        "[r2-fs-put] bucket bind failed (attempt {attempt}): {e:?}"
                    ));
                    continue;
                }
            };
            let body = bytes.clone();
            let k = key.to_string();
            let fut = async move { bucket.put(&k, body).execute().await.map(|_| ()) };
            match race_timeout(fut, R2_GET_TIMEOUT_MS).await {
                Some(Ok(())) => {
                    self.r2_breaker_record_success();
                    return Ok(());
                }
                Some(Err(e)) => console_log(&format!(
                    "[r2-fs-put] transient error attempt {attempt}: {e:?}"
                )),
                None => console_log(&format!("[r2-fs-put] timeout attempt {attempt}")),
            }
        }
        self.r2_breaker_record_failure();
        console_log(&format!(
            "[r2-fs-put] EXHAUSTED {R2_GET_ATTEMPTS} attempts for key {key}"
        ));
        Err(())
    }

    /// No-ws entry (HTTP /frame / facet proxy path). Host callbacks are unavailable
    /// here (no held client socket), so a non-fetch host.<name> call rejects cleanly.
    async fn handle(&self, msg: serde_json::Value) -> Result<serde_json::Value> {
        self.handle_with_ws(msg, None).await
    }

    async fn handle_with_ws(
        &self,
        msg: serde_json::Value,
        ws: Option<&WebSocket>,
    ) -> Result<serde_json::Value> {
        let t = msg.get("t").and_then(|v| v.as_str()).unwrap_or("");
        match t {
            "gen" => {
                let store = self.store();
                let committed = read_int(&store, "committedCell", -1);
                let live = read_int(&store, "liveCell", committed);
                let version = self.version_meta();
                Ok(json!({
                    "ok": true, "t": "gen",
                    "generation": self.generation,
                    "inMemory": self.glue.borrow().is_some(),
                    "epoch": read_int(&store, "epoch", 0),
                    "committedCell": committed,
                    "liveCell": live,
                    "dirty": live > committed,
                    "durability": self.config_durability(),
                    "engineHash": get_engine_hash(),
                    "version": "rust-v0.9.3",
                    "versionId": version.id,
                    "versionTag": version.tag,
                    "versionTimestamp": version.timestamp,
                    "activeEval": self.active_eval(),
                    "queueDepth": self.eval_queue_depth(),
                    "maxQueueDepth": self.max_eval_queue_depth(),
                }))
            }
            "ping" => {
                let version = self.version_meta();
                Ok(json!({
                    "ok": true, "t": "ping",
                    "inMemory": self.glue.borrow().is_some(),
                    "generation": self.generation,
                    "keepAlive": msg.get("keepAlive").and_then(|v| v.as_bool()).unwrap_or(false),
                    "versionId": version.id,
                    "versionTag": version.tag,
                    "versionTimestamp": version.timestamp,
                    "activeEval": self.active_eval(),
                    "queueDepth": self.eval_queue_depth(),
                    "maxQueueDepth": self.max_eval_queue_depth(),
                }))
            }
            "_fault" => {
                if !self.fault_test_enabled() {
                    return Ok(self.fault_reject("_fault"));
                }
                let op = msg.get("op").and_then(|v| v.as_str()).unwrap_or("status");
                match op {
                    "status" => Ok(json!({
                        "ok": true,
                        "t": "_fault",
                        "enabled": true,
                        "generation": self.generation,
                        "activeEval": self.active_eval(),
                        "r2MissOnceKey": self.fault_read_meta("r2MissOnceKey").unwrap_or_default(),
                        "r2MissOnceFired": self.fault_read_meta("r2MissOnceFired").unwrap_or_default(),
                    })),
                    "r2-miss-once" => {
                        let key = msg.get("key").and_then(|v| v.as_str()).unwrap_or("*");
                        self.fault_write_meta("r2MissOnceKey", key);
                        self.fault_write_meta("r2MissOnceFired", "0");
                        Ok(json!({"ok": true, "t": "_fault", "op": op, "key": key}))
                    }
                    "clear" => {
                        self.fault_write_meta("r2MissOnceKey", "");
                        self.fault_write_meta("r2MissOnceFired", "");
                        Ok(json!({"ok": true, "t": "_fault", "op": op}))
                    }
                    "checkpoint-evict" => {
                        if self.glue.borrow().is_none() {
                            return Ok(json!({
                                "ok": false,
                                "t": "_fault",
                                "op": op,
                                "valueType": "error",
                                "error": {"name":"FaultNoLiveKernel","message":"no live kernel to checkpoint/evict"}
                            }));
                        }
                        let cell = self.next_cell();
                        let epoch = read_int(&self.store(), "epoch", 0);
                        let force_full = msg
                            .get("forceFull")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);
                        let ckpt = if force_full {
                            self.checkpoint_force_full(
                                cell,
                                epoch,
                                "/* fault checkpoint while parked */",
                            )
                            .await
                        } else {
                            self.checkpoint(cell, epoch, "/* fault checkpoint while parked */")
                                .await
                        };
                        let dropped = self
                            .glue
                            .borrow_mut()
                            .take()
                            .map(|g| {
                                g.drop_kernel();
                                true
                            })
                            .unwrap_or(false);
                        match ckpt {
                            Ok(v) => Ok(
                                json!({"ok": true, "t": "_fault", "op": op, "checkpoint": v, "droppedInMemory": dropped, "generation": self.generation}),
                            ),
                            Err(e) => Ok(
                                json!({"ok": false, "t": "_fault", "op": op, "error": format!("{e}"), "droppedInMemory": dropped, "generation": self.generation}),
                            ),
                        }
                    }
                    _ => Ok(
                        json!({"ok": false, "t": "_fault", "error": format!("unknown fault op {op}")}),
                    ),
                }
            }
            // VFS-* out-of-band file I/O. Serviced DIRECTLY against the host.fs R2 store (same
            // key scheme `fs/<doId>/<normpath>`, same SNAPSHOTS bucket, same `fs_files` meta table,
            // same `norm_fs_path` isolation) — NEVER through the VM eval. These arms do NOT acquire
            // `self.mutex`, so they bypass the instruction budget AND the WASM heap/mem-cap entirely.
            // Auth + path-isolation are already enforced upstream (websocket_message auth gate +
            // norm_fs_path below). A file uploaded here is visible to a later cell that runs with
            // config.fs.provider=="r2" (read_fs_committed -> r2_fs_op share this exact namespace).
            "vfs-write" => self.vfs_write(&msg).await,
            "vfs-read" => self.vfs_read(&msg).await,
            "vfs-ls" => self.vfs_ls(&msg),
            "vfs-stat" => self.vfs_stat(&msg).await,
            // VFS-SYNC — reconcile the fs_files namespace from the live R2 prefix `fs/<doId>/`.
            // The container (engram-sandbox) writes bodies DIRECTLY to R2 via the s3fs mount, so a
            // file it creates has NO fs_files row and is INVISIBLE to host.fs / vfs-* until this runs
            // (same mechanism worker_invoke uses post-invoke). This makes the container->cell half of
            // the shared-VFS round-trip explicit + on-demand. Acquires the mutex ONLY for the
            // reconcile (so it can't interleave an eval's staged flush). Returns the reconciled file
            // list. Auth is enforced upstream; per-session isolation is the hard-bound prefix.
            "vfs-sync" => self.vfs_sync(&msg).await,
            // WORKER REGISTRY — content-addressed Dynamic-Worker-Loader compute. Like the vfs-*
            // arms: NO self.mutex (they never touch the VM heap; the mutex is acquired ONLY for the
            // post-invoke fs_files reconcile, inside worker_invoke). Auth + per-session isolation are
            // already enforced upstream (websocket_message auth gate + the registry_workers invoke
            // gate + the fs/<doId>/ prefix). register = sha256(source) -> R2 `workers/<hash>.js` +
            // per-session row; invoke = LOADER.get(hash) fresh isolate with the shared R2 VFS;
            // list = this session's registry index.
            "worker-register" => self.worker_register(&msg).await,
            "worker-invoke" => self.worker_invoke(&msg).await,
            "worker-list" => self.worker_list(&msg),
            // SANDBOX BRIDGE (additive, SDK `s.sandbox.*` surface). Like the vfs-* / worker-* arms:
            // serviced DIRECTLY (NO self.mutex — it never touches the VM heap) by a DO-side fetch to
            // the engram-sandbox container worker, passing the trusted KERNEL do_id (=> the R2 prefix
            // `fs/<doId>/` the sandbox mounts as /workspace) + the Bearer key read from ENV. Auth on the
            // kernel WS is already enforced upstream (websocket_message gate); capability-gated by
            // config.sandbox. The key NEVER enters the VM heap nor any client frame.
            "sandbox" => self.sandbox_frame(&msg).await,
            "create" => {
                let mut permit = match self.try_enter_vm_queue("create") {
                    Ok(p) => p,
                    Err(reply) => return Ok(reply),
                };
                let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
                permit.mark_active();
                let res = self.create_critical(&msg).await;
                permit.finish();
                let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
                res
            }
            "eval" => {
                let mut permit = match self.try_enter_vm_queue("eval") {
                    Ok(p) => p,
                    Err(reply) => return Ok(reply),
                };
                let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
                permit.mark_active();
                let res = self.eval_critical(&msg, ws).await;
                permit.finish();
                let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
                res
            }
            "flush" => {
                let mut permit = match self.try_enter_vm_queue("flush") {
                    Ok(p) => p,
                    Err(reply) => return Ok(reply),
                };
                let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
                permit.mark_active();
                let res = self.flush_critical("explicit").await;
                permit.finish();
                let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
                res
            }
            "artifact" => {
                let mut permit = match self.try_enter_vm_queue("artifact") {
                    Ok(p) => p,
                    Err(reply) => return Ok(reply),
                };
                let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
                permit.mark_active();
                let res = self.artifact_critical(&msg).await;
                permit.finish();
                let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
                res
            }
            "reset" => {
                let mut permit = match self.try_enter_vm_queue("reset") {
                    Ok(p) => p,
                    Err(reply) => return Ok(reply),
                };
                let release = JsFuture::from(self.mutex.acquire()).await.map_err(to_err)?;
                permit.mark_active();
                let res = self.reset_critical().await;
                permit.finish();
                let _ = js_sys::Function::from(release).call0(&JsValue::NULL);
                res
            }
            "evict" => {
                let taken = self.glue.borrow_mut().take();
                let had = taken.is_some();
                if let Some(g) = taken {
                    g.drop_kernel();
                }
                Ok(json!({"ok": true, "t": "evict", "droppedInMemory": had,
                    "generation": self.generation}))
            }
            "_forceEngineMismatch" => {
                // TEST-ONLY (E6 engine-migration): rewrite the committed manifest's engine_hash to
                // a bogus value + drop the in-memory glue, so the next eval cold-restores via the
                // OPLOG-REPLAY path (the byte-blit image is "from a different engine"). Proves the
                // engine-migration journal replay works without a real redeploy.
                let taken = self.glue.borrow_mut().take();
                if let Some(g) = taken {
                    g.drop_kernel();
                }
                self.store().exec_ignore(
                    "UPDATE snap_manifest SET engine_hash='STALE-ENGINE-HASH' WHERE id=1;",
                    None,
                );
                Ok(json!({"ok": true, "t": "_forceEngineMismatch", "generation": self.generation}))
            }
            "health" => Ok(json!({"ok": true, "t": "health", "generation": self.generation})),
            other => Ok(json!({"ok": false, "error": format!("unknown msg type {other}")})),
        }
    }

    async fn create_critical(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let cfg = msg.get("config").cloned().unwrap_or_else(|| json!({}));
        // EXTENSIBILITY API — REDUCED Phase 1 (#32): validate config.extensions BEFORE persisting.
        // CLIENT backend only (http/worker rejected); reject __-prefixed / built-in-shadowing names;
        // cap manifest size. A failure returns a typed {ok:false} create reply (socket stays alive),
        // never persists a bad manifest. Zero engine change (discoverability is glue-seeded).
        if let Some(err) = validate_extensions(&cfg) {
            return Ok(json!({
                "ok": false, "t": "create",
                "valueType": "error",
                "error": err,
                "generation": self.generation,
            }));
        }
        let cfg_str = serde_json::to_string(&cfg).unwrap_or_else(|_| "{}".into());
        if let Some(err) = persist_config(&self.store(), &cfg_str) {
            return Ok(json!({
                "ok": false, "t": "create",
                "valueType": "error",
                "error": err,
                "generation": self.generation,
            }));
        }
        let info = self.ensure_glue().await?;
        let mut dp = Datapoint::new("create");
        dp.restore_source = info.source.clone();
        dp.read_ms = info.read_ms;
        dp.total_server_ms = info.total_server_ms;
        self.emit(&dp);
        Ok(json!({
            "ok": true, "t": "create",
            "config": cfg,
            "generation": self.generation,
            "restoreSource": info.source,
            "restoreTimings": restore_timings_value(&info),
        }))
    }

    async fn eval_critical(
        &self,
        msg: &serde_json::Value,
        ws: Option<&WebSocket>,
    ) -> Result<serde_json::Value> {
        // BUG-3 FIX: snapshot the durable fsVersion at eval start. A mutex-free frame write that
        // commits during this eval's park bumps fsVersion; flush_staged_fs compares against this to
        // avoid clobbering an ACKed out-of-band write with the staged last-writer-wins flush.
        self.eval_start_fs_version
            .set(read_fs_version(&self.store()));
        let src = msg.get("src").and_then(|v| v.as_str()).unwrap_or("");
        let req_id = msg
            .get("reqId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && s.len() <= 128);
        if let Some(id) = req_id {
            if let Some(reply) = self.read_eval_reply(id) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&reply) {
                    if let Some(reserved) = Self::pending_eval_replay(&v, self.generation) {
                        return Ok(reserved);
                    }
                    return Ok(v);
                }
            }
        }
        let before = self.glue.borrow().is_some();
        let planned_cell = self.next_cell();
        if let Some(id) = req_id {
            self.write_eval_reply(id, &Self::pending_eval_reply(planned_cell, src));
        }

        // first-eval config (only if none yet). Apply the SAME validation as create_critical so an
        // eval-first `{t:eval,config:{...}}` frame cannot bypass the extension name-shadow / __-prefix
        // / manifest-size guards, and reject an oversize config BEFORE write_meta (which would
        // otherwise panic on SQLITE_TOOBIG -> DoS). A validation/size failure returns a typed
        // {ok:false} eval reply (socket stays alive), never persists a bad config.
        if !before {
            if let Some(cfg) = msg.get("config") {
                if read_str(&self.store(), "config").is_none() {
                    if let Some(err) = validate_extensions(cfg) {
                        return Ok(json!({
                            "ok": false, "t": "eval",
                            "valueType": "error",
                            "error": err,
                            "generation": self.generation,
                        }));
                    }
                    let cfg_str = serde_json::to_string(cfg).unwrap_or_else(|_| "{}".into());
                    if let Some(err) = persist_config(&self.store(), &cfg_str) {
                        return Ok(json!({
                            "ok": false, "t": "eval",
                            "valueType": "error",
                            "error": err,
                            "generation": self.generation,
                        }));
                    }
                }
            }
        }

        let info = self.ensure_glue().await?;

        // HOST-CALLBACK BRIDGE: install a per-eval sender on the glue so a non-fetch
        // host.<name>(...) call round-trips to the connected client over THIS ws. The
        // sender is a JS closure that ws.send's the {t:hostcall} frame out-of-band; the
        // client's {t:hostcall-result} reply is demuxed in websocket_message (no mutex)
        // and resolves the parked VM call. When ws is None (HTTP /frame / facet path) we
        // clear the sender, so non-fetch host calls reject cleanly (no held client socket).
        {
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            if let Some(ws) = ws {
                let ws_for_send: WebSocket = ws.clone();
                let sender = Closure::wrap(Box::new(move |frame: JsValue| -> JsValue {
                    let s = frame.as_string().unwrap_or_default();
                    let ok = ws_for_send.send_with_str(&s).is_ok();
                    JsValue::from_bool(ok)
                })
                    as Box<dyn FnMut(JsValue) -> JsValue>);
                glue.set_host_sender(sender.as_ref().unchecked_ref(), 60000.0);
                // Leak the closure for the lifetime of this eval; it is replaced/cleared on
                // the next eval. (One small closure per eval; bounded by serialized evals.)
                sender.forget();
            } else {
                glue.set_host_sender(&JsValue::NULL, 0.0);
            }
        }

        // FS PROVIDER: when config.fs.provider == "r2", install a DO-side handler for the
        // engine's `host.__fs` effect. R2 is a DO binding (env), NOT a glue global, so it must
        // be serviced here (unlike host.fetch). The handler is an async JS closure returning a
        // Promise; the glue awaits it from the eval pump. provider == vfs (default) clears the
        // handler (in-heap fs). COHERENCE: writes/deletes are STAGED in DO memory (self.staged_fs)
        // and flushed to R2 + the committed `fs_files` table only at checkpoint, in the SAME commit
        // as the heap dump (SANDBOX-API staged-commit invariant). Reads within a cell see the
        // committed namespace (captured below) OVERLAID with this cell's staged ops.
        // ISOLATION: the R2 key prefix is ALWAYS `fs/<doId>/` — derived from the DO id, NEVER from
        // user config — so a session can never address another session's fs namespace.
        {
            let cfg: serde_json::Value = serde_json::from_str(
                &read_str(&self.store(), "config").unwrap_or_else(|| "{}".into()),
            )
            .unwrap_or_else(|_| json!({}));
            let fsv = cfg
                .get("fs")
                .and_then(|f| f.get("provider"))
                .and_then(|v| v.as_str())
                .unwrap_or("vfs");
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            if fsv == "r2" {
                // Bucket binding is user-selectable (which R2 bucket), but the KEY PREFIX is not.
                let binding = cfg
                    .get("fs")
                    .and_then(|f| f.get("binding"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("SNAPSHOTS")
                    .to_string();
                // Per-session prefix is hard-bound to the DO id (isolation invariant); any user
                // `config.fs.prefix` is intentionally IGNORED.
                let prefix = format!("fs/{}/", self.do_id);
                // Snapshot the committed path namespace so in-cell reads/list/stat see prior cells'
                // files overlaid with this cell's staged ops. Cheap (paths + sizes, no bodies).
                let committed = self.read_fs_committed();
                let env = self.env.clone();
                let staged = self.staged_fs.clone();
                // Eager cells start from a clean staged buffer. warmBuffered keeps staged ops across
                // dirty cells so later cells see unflushed writes and the eventual checkpoint commits
                // heap + fs together.
                if !self.is_dirty() {
                    staged.borrow_mut().clear();
                }
                let handler = Closure::wrap(Box::new(move |payload: JsValue| -> js_sys::Promise {
                    let env = env.clone();
                    let binding = binding.clone();
                    let prefix = prefix.clone();
                    let staged = staged.clone();
                    let committed = committed.clone();
                    wasm_bindgen_futures::future_to_promise(async move {
                        r2_fs_op(&env, &binding, &prefix, &staged, &committed, payload).await
                    })
                })
                    as Box<dyn FnMut(JsValue) -> js_sys::Promise>);
                glue.set_fs_handler(handler.as_ref().unchecked_ref());
                handler.forget();
            } else {
                glue.set_fs_handler(&JsValue::NULL);
            }
        }

        // EXTENSIBILITY API — REDUCED Phase 1 (#32): install a per-eval AE sink so the glue can emit
        // ONE datapoint per ext call (op="ext:<name>"). Mirrors the host-sender install (a 'static
        // closure capturing env+do_id+generation). Cleared to NULL on the no-ws path.
        {
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            if ws.is_some() {
                let env = self.env.clone();
                let do_id = self.do_id.clone();
                let generation = self.generation;
                let sink = Closure::wrap(Box::new(move |op: JsValue| {
                    let op = op.as_string().unwrap_or_default();
                    write_ext_ae(&env, &do_id, generation, &op);
                }) as Box<dyn FnMut(JsValue)>);
                glue.set_ext_ae_sink(sink.as_ref().unchecked_ref());
                sink.forget();
            } else {
                glue.set_ext_ae_sink(&JsValue::NULL);
            }
        }

        // FETCH FENCE: install the deterministic (doId, cell) identity so host.fetch can derive a
        // stable Idempotency-Key per (session, cell, in-cell fetch ordinal); replay reproduces it.
        {
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            glue.set_fence_context(&self.do_id, planned_cell as f64);
        }

        // SANDBOX BRIDGE (additive): install the engram-sandbox endpoint + Bearer key (from ENV) +
        // the trusted KERNEL do_id + the capability flag (config.sandbox) so the in-cell
        // `host.sandbox.*` effect can DO-side fetch the container worker over the shared R2 VFS. The
        // KEY is read from env.secret here and handed to the glue closure ONLY; it NEVER enters the VM
        // heap (the cell sees only the host.sandbox.* effect name). Cleared/disabled when config.sandbox
        // is falsey or the env bindings are absent -> SandboxUnavailable. Pass self.do_id (trusted
        // 64-hex KERNEL DO id == the R2 prefix the sandbox mounts), NEVER a user-chosen value.
        {
            let cfg: serde_json::Value = serde_json::from_str(
                &read_str(&self.store(), "config").unwrap_or_else(|| "{}".into()),
            )
            .unwrap_or_else(|_| json!({}));
            let enabled = cfg
                .get("sandbox")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let url = self
                .env
                .var("ENGRAM_SANDBOX_URL")
                .ok()
                .map(|v| v.to_string())
                .unwrap_or_default();
            let key = self
                .env
                .secret("ENGRAM_SANDBOX_KEY")
                .ok()
                .map(|s| s.to_string())
                .unwrap_or_default();
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            // Only enable when the capability flag is set AND both bindings are present; the glue also
            // re-checks and returns SandboxUnavailable on a missing url/key.
            let on = enabled && !url.is_empty() && !key.is_empty();
            glue.set_sandbox_config(&url, &key, &self.do_id, on);
        }

        // eval (async; cell may await host.fetch OR a client host-callback). Returns rich
        // JSON; never throws.
        let eval_promise = {
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            glue.eval_code(src)
        };
        let eval_json = JsFuture::from(eval_promise)
            .await
            .map_err(to_err)?
            .as_string()
            .unwrap_or_else(|| "{\"ok\":true,\"value\":null}".to_string());
        let mut parsed: serde_json::Value = match serde_json::from_str(&eval_json) {
            Ok(v) => v,
            Err(e) => json!({
                "ok": false,
                "valueType": "error",
                "error": {
                    "name": "ProtocolSizeError",
                    "message": format!("eval result JSON was invalid or truncated ({} bytes): {e}", eval_json.len()),
                },
            }),
        };

        // Allocate the next live cell inside the critical section. In eagerDurable mode this cell
        // is checkpointed before reply. In warmBuffered mode it is marked dirty and returned before
        // snapshot work; a later flush/alarm commits the live heap.
        let store = self.store();
        let cell = planned_cell;
        let epoch = read_int(&store, "epoch", 0);
        let has_artifacts = stamp_artifact_handles(&mut parsed, cell);
        let requested_mode = self.frame_durability(msg);
        // Artifact handles are durable addresses keyed by committed cell. Force eager durability for
        // artifact-producing cells until we add explicit ephemeral-artifact handles.
        let buffered = requested_mode == "warmBuffered" && !has_artifacts;
        let ckpt = if buffered {
            self.mark_dirty(cell, src).await;
            json!({
                "ok": true,
                "cell": cell,
                "mode": "warmBuffered",
                "deferred": true,
                "dirty": true,
                "committedCell": read_int(&store, "committedCell", -1),
                "liveCell": cell,
            })
        } else {
            let v = match self.checkpoint(cell, epoch, src).await {
                Ok(v) => v,
                Err(e) => json!({ "ok": false, "error": format!("{e}") }),
            };
            if v.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                self.mark_clean(cell);
            }
            v
        };
        let ok = parsed.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
        if ckpt.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            let artifact_cell = if has_artifacts {
                cell.to_string()
            } else {
                "-1".to_string()
            };
            write_meta(&store, "lastArtifactCell", &artifact_cell);
        }

        // V0.5: per-eval AE datapoint (op/restoreSource/store/sizeGz/usedHeap/cell/ok + valueType).
        let mut dp = Datapoint::new("eval");
        dp.cell = cell;
        dp.ok = ok;
        dp.restore_source = info.source.clone();
        dp.read_ms = info.read_ms;
        dp.total_server_ms = info.total_server_ms;
        dp.value_type = parsed
            .get("valueType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        dp.error_name = parsed
            .get("error")
            .and_then(|e| e.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        dp.store = ckpt
            .get("store")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        dp.size_raw = ckpt.get("sizeRaw").and_then(|v| v.as_i64()).unwrap_or(0);
        dp.size_gz = ckpt.get("sizeGz").and_then(|v| v.as_i64()).unwrap_or(0);
        dp.used_heap = ckpt.get("usedHeap").and_then(|v| v.as_i64()).unwrap_or(0);
        self.emit(&dp);
        let version = self.version_meta();

        let reply = json!({
            "ok": ok, "t": "eval",
            "value": parsed.get("value").cloned().unwrap_or(serde_json::Value::Null),
            "valuePreview": parsed.get("valuePreview").cloned().unwrap_or(serde_json::Value::Null),
            "valueType": parsed.get("valueType").cloned().unwrap_or(serde_json::Value::Null),
            "mimeBundle": parsed.get("mimeBundle").cloned().unwrap_or(serde_json::Value::Null),
            "outputs": parsed.get("outputs").cloned().unwrap_or_else(|| json!([])),
            "logs": parsed.get("logs").cloned().unwrap_or_else(|| json!([])),
            "error": parsed.get("error").cloned().unwrap_or(serde_json::Value::Null),
            "cell": cell,
            "generation": self.generation,
            "inMemoryBefore": before,
            "restoreSource": info.source,
            "restoreTimings": restore_timings_value(&info),
            "durability": if buffered { "warmBuffered" } else { "eagerDurable" },
            "dirty": buffered,
            "checkpoint": ckpt,
            "versionId": version.id,
            "versionTag": version.tag,
            "versionTimestamp": version.timestamp,
        });

        if let Some(id) = req_id {
            if let Ok(reply_str) = serde_json::to_string(&reply) {
                if reply_str.len() <= SQLITE_MAX_VALUE_BYTES {
                    self.write_eval_reply(id, &reply_str);
                }
            }
        }

        Ok(reply)
    }

    async fn artifact_critical(&self, msg: &serde_json::Value) -> Result<serde_json::Value> {
        let handle = msg.get("handle").and_then(|v| v.as_str()).unwrap_or("");
        let Some((cell, artifact_id)) = parse_artifact_handle(handle) else {
            return Ok(json!({
                "ok": false,
                "t": "artifact",
                "error": { "name": "ArtifactError", "message": "malformed artifact handle" },
            }));
        };

        let committed = read_int(&self.store(), "committedCell", -1);
        let artifact_cell = read_int(&self.store(), "lastArtifactCell", -1);
        if cell != committed || cell != artifact_cell {
            return Ok(json!({
                "ok": false,
                "t": "artifact",
                "handle": handle,
                "error": {
                    "name": "ArtifactError",
                    "message": format!("stale or unknown artifact handle for cell {cell}; committed cell is {committed}"),
                },
            }));
        }

        let offset = msg
            .get("offset")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .max(0);
        let len = msg
            .get("len")
            .and_then(|v| v.as_i64())
            .unwrap_or(TEXT_ARTIFACT_CHUNK_MAX_CHARS)
            .clamp(0, TEXT_ARTIFACT_CHUNK_MAX_CHARS);

        let _ = self.ensure_glue().await?;
        let chunk_json = {
            let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
            glue.result_artifact_chunk(&artifact_id, offset as f64, len as f64)
        };
        let mut parsed: serde_json::Value = match serde_json::from_str(&chunk_json) {
            Ok(v) => v,
            Err(e) => json!({
                "ok": false,
                "t": "artifact",
                "handle": handle,
                "error": {
                    "name": "ArtifactError",
                    "message": format!("artifact chunk JSON was invalid or truncated ({} bytes): {e}", chunk_json.len()),
                },
            }),
        };
        if let Some(obj) = parsed.as_object_mut() {
            obj.insert("handle".to_string(), json!(handle));
        }
        Ok(parsed)
    }

    async fn reset_critical(&self) -> Result<serde_json::Value> {
        let taken = self.glue.borrow_mut().take();
        if let Some(g) = taken {
            g.drop_kernel();
        }
        let r2_key = self.read_manifest().and_then(|m| {
            if m.store == "r2" && !m.r2_key.is_empty() {
                Some(m.r2_key)
            } else {
                None
            }
        });
        let store = self.store();
        store.exec_ignore("DELETE FROM snap_chunks;", None);
        store.exec_ignore("DELETE FROM delta_chunks;", None);
        store.exec_ignore("DELETE FROM oplog;", None);
        store.exec_ignore("DELETE FROM eval_replies;", None);
        store.exec_ignore("DELETE FROM snap_manifest;", None);
        if let Ok(bucket) = self.env.bucket("SNAPSHOTS") {
            if let Some(k) = r2_key {
                let _ = bucket.delete(&k).await;
            }
        }
        let epoch = read_int(&store, "epoch", 0) + 1;
        write_meta(&store, "epoch", &epoch.to_string());
        write_meta(&store, "committedCell", "-1");
        self.mark_clean(-1);
        write_meta(&store, "lastArtifactCell", "-1");
        let _ = self.state.storage().delete_alarm().await;
        Ok(json!({"ok": true, "t": "reset", "epoch": epoch, "generation": self.generation}))
    }

    /// Ensure a live kernel. On a cold wake, restore from the committed checkpoint; else
    /// create a fresh seeded kernel under the persisted config.
    async fn ensure_glue(&self) -> Result<RestoreInfo> {
        if self.glue.borrow().is_some() {
            return Ok(RestoreInfo::warm());
        }
        let t0 = now_ms();
        let glue = new_glue_kernel();
        let cfg_str = read_str(&self.store(), "config").unwrap_or_else(|| "{}".into());
        let mut read_ms = 0.0_f64;
        let mut timings_json = String::from("{}");

        let manifest = self.read_manifest();
        let source = if let Some(m) = manifest {
            let is_r2 = m.store == "r2";
            // R2 OVERFLOW RESTORE (issue #10): a transient R2 hiccup MUST NOT bubble a 500 that the
            // client reads as "no session" and answers with a state-wiping {t:create}. Read R2 with
            // retry/backoff/timeout + circuit-breaker; on a durable miss OR exhausted retries OR an
            // open breaker, keep the session INTACT and recover via the oplog-replay path below
            // (never create-fresh). `gz=None` signals "could not load the byte image — replay".
            let mut r2_replay_reason: Option<&'static str> = None;
            let gz: Option<Uint8Array> = if is_r2 {
                if self.r2_breaker_open() {
                    console_log("[r2-get] breaker OPEN — skipping R2, routing to oplog replay");
                    r2_replay_reason = Some("r2-breaker-open-replay");
                    None
                } else {
                    match self.r2_get_resilient(&m.r2_key).await {
                        R2Read::Got(arr) => Some(arr),
                        R2Read::Missing => {
                            r2_replay_reason = Some("r2-missing-replay");
                            None
                        }
                        R2Read::Exhausted => {
                            r2_replay_reason = Some("r2-unavailable-replay");
                            None
                        }
                    }
                }
            } else {
                Some(self.read_chunks(m.n_chunks)?)
            };
            read_ms = now_ms() - t0;

            // R2-UNAVAILABLE FALLBACK: byte image unreadable but the session is durable — recover by
            // replaying the committed oplog tail into a fresh instance (no re-fire of host effects).
            // This is the data-loss fix: failure mode is "temporarily recovered via replay", NOT "gone".
            if is_r2 && gz.is_none() {
                let journal = self.read_oplog();
                let _ = JsFuture::from(glue.replay_journal(journal.as_ref(), &cfg_str, &m.kv_json))
                    .await
                    .map_err(to_err)?;
                timings_json = glue.last_restore_timings();
                r2_replay_reason
                    .unwrap_or("r2-unavailable-replay")
                    .to_string()
            }
            // E6 ENGINE-MIGRATION: if the committed snapshot was written by a DIFFERENT engine
            // build, the byte-blit image is invalid (different layout). Replay the oplog tail into
            // a FRESH instance under the same config instead of wedging. Pure cells re-run; host
            // effects are fed back from the recorded oplog (no re-fire).
            else if !m.engine_hash.is_empty() && m.engine_hash != get_engine_hash() {
                let journal = self.read_oplog();
                let _ = JsFuture::from(glue.replay_journal(journal.as_ref(), &cfg_str, &m.kv_json))
                    .await
                    .map_err(to_err)?;
                timings_json = glue.last_restore_timings();
                "engine-migration-replay".to_string()
            } else {
                let label = if is_r2 {
                    "r2-restore"
                } else {
                    "sqlite-restore"
                };
                let delta_list = self.read_delta_chain(m.delta_seq)?;
                match JsFuture::from(glue.restore_w4(
                    gz.expect("gz image present on the non-replay restore path"),
                    delta_list.as_ref(),
                    &m.engine_hash,
                    m.clock_calls as f64,
                    m.rng_calls as f64,
                    &cfg_str,
                    label,
                    &m.kv_json,
                    m.used_heap as f64,
                    m.final_crc as f64,
                    m.size_raw as f64,
                    &m.snap_codec,
                ))
                .await
                {
                    Ok(src) => {
                        timings_json = glue.last_restore_timings();
                        src.as_string().unwrap_or_else(|| label.into())
                    }
                    Err(_) => {
                        // SANITY-PROBE FALLBACK: the hot byte-blit failed its post-restore canary/GC
                        // probe (possible image corruption) — discard and replay the oplog tail into
                        // a fresh instance. Always correct; pure cells re-run, host effects fed back.
                        let journal = self.read_oplog();
                        let _ = JsFuture::from(glue.replay_journal(
                            journal.as_ref(),
                            &cfg_str,
                            &m.kv_json,
                        ))
                        .await
                        .map_err(to_err)?;
                        timings_json = glue.last_restore_timings();
                        "sanity-fallback-replay".to_string()
                    }
                }
            }
        } else {
            let src = JsFuture::from(glue.create_fresh(&cfg_str))
                .await
                .map_err(to_err)?;
            src.as_string().unwrap_or_else(|| "fresh".into())
        };

        let total_server_ms = now_ms() - t0;
        {
            let store = self.store();
            let committed = read_int(&store, "committedCell", -1);
            let live = read_int(&store, "liveCell", committed);
            if live > committed {
                write_meta(&store, "lastDirtyLostCell", &live.to_string());
                self.mark_clean(committed);
            }
        }
        *self.glue.borrow_mut() = Some(glue);
        Ok(RestoreInfo {
            source,
            read_ms,
            total_server_ms,
            timings_json,
        })
    }

    /// W4 BYTE-DELTA checkpoint (docs/W4-BYTEDELTA-PLAN.md). The committed snapshot is a full
    /// (W5-compacted) BASE plus a chain of per-cell byte-deltas. A full base is forced when there
    /// is no prior snapshot OR the chain would reach BASE_EVERY (caps the restore chain length);
    /// glue.dumpW4 may ALSO downgrade to a full base (length change / dense mutation). Crash-atomic
    /// via DO synchronous write-coalescing; R2 swap-then-delete for the base overflow.
    /// `src` is the cell source (for the E6 oplog tail).
    async fn checkpoint(&self, cell: i64, epoch: i64, src: &str) -> Result<serde_json::Value> {
        self.checkpoint_impl(cell, epoch, src, false).await
    }

    async fn checkpoint_force_full(
        &self,
        cell: i64,
        epoch: i64,
        src: &str,
    ) -> Result<serde_json::Value> {
        self.checkpoint_impl(cell, epoch, src, true).await
    }

    async fn checkpoint_impl(
        &self,
        cell: i64,
        epoch: i64,
        src: &str,
        force_full_override: bool,
    ) -> Result<serde_json::Value> {
        const BASE_EVERY: i64 = 20;
        let prev = self.read_manifest();
        let force_full = force_full_override
            || match &prev {
                None => true,
                Some(m) => m.delta_seq + 1 >= BASE_EVERY,
            };

        let glue = clone_glue(self.glue.borrow().as_ref().unwrap());
        let dump = JsFuture::from(glue.dump_w4(force_full))
            .await
            .map_err(to_err)?;
        let mode = str_field(&dump, "mode").unwrap_or_else(|| "full".to_string());
        let gz: Uint8Array = Reflect::get(&dump, &JsValue::from_str("gz"))
            .map_err(to_err)?
            .into();
        let size_raw = num_field(&dump, "sizeRaw").unwrap_or(0.0) as i64;
        let size_gz = num_field(&dump, "sizeGz").unwrap_or(0.0) as i64;
        let used_heap = num_field(&dump, "usedHeap").unwrap_or(0.0) as i64;
        let buffer_bytes = num_field(&dump, "bufferBytes").unwrap_or(0.0) as i64;
        let scratch_len = num_field(&dump, "scratchLen").unwrap_or(0.0) as i64;
        let scrubbed = Reflect::get(&dump, &JsValue::from_str("scrubbed"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let clock_calls = num_field(&dump, "clockCalls").unwrap_or(0.0) as i64;
        let rng_calls = num_field(&dump, "rngCalls").unwrap_or(0.0) as i64;
        let grain = num_field(&dump, "grain").unwrap_or(256.0) as i64;
        let n_changed = num_field(&dump, "nChanged").unwrap_or(0.0) as i64;
        let image_crc = num_field(&dump, "imageCrc").unwrap_or(0.0) as i64;
        let snap_codec = str_field(&dump, "snapCodec").unwrap_or_else(|| "gzip".to_string());
        let kv_json = str_field(&dump, "kvJson").unwrap_or_else(|| "{}".to_string());

        let mut bytes = vec![0u8; gz.length() as usize];
        gz.copy_to(&mut bytes);

        // E6 oplog row for this cell (host results captured during eval).
        let host_results = glue.last_cell_host_results();

        // (b) CHECKPOINT size: clamp every value we will BIND in the staged txn below SQLite's
        // ~2MB single-value cap, so the INSERT can never throw SQLITE_TOOBIG mid-commit. The oplog
        // `src` is the only unbounded text; an oversized cell is recorded as a non-replayable marker
        // (the byte-blit image stays the primary restore path; only engine-migration replay of THIS
        // cell would no-op). host_results is bounded by the engine HOSTCALL buffer, but clamp it too
        // as a belt-and-suspenders guard.
        let src: String = if src.len() > SQLITE_MAX_VALUE_BYTES {
            format!(
                "/* engram: cell source {}B omitted from oplog (exceeds SQLite value cap) */",
                src.len()
            )
        } else {
            src.to_string()
        };
        let src = src.as_str();
        let host_results = if host_results.len() > SQLITE_MAX_VALUE_BYTES {
            "[]".to_string()
        } else {
            host_results
        };

        let is_delta = mode == "delta" && prev.is_some();

        if is_delta {
            // ---- DELTA PATH: keep the base intact; append ONE delta row + bump delta_seq + oplog.
            let pm = prev.as_ref().unwrap();
            let idx_arr: Uint8Array = Reflect::get(&dump, &JsValue::from_str("indicesGz"))
                .map_err(to_err)?
                .into();
            let mut idx_bytes = vec![0u8; idx_arr.length() as usize];
            idx_arr.copy_to(&mut idx_bytes);
            let delta_seq_new = pm.delta_seq; // 0-based row seq == prior chain length
            let base_store = pm.store.clone();
            let base_r2_key = pm.r2_key.clone();
            let base_engine = pm.engine_hash.clone();
            let base_n_chunks = pm.n_chunks;
            let _base_size_raw = pm.size_raw; // superseded: delta tail stores its own reconstructed size_raw
            let base_size_gz = pm.size_gz;
            let base_codec = pm.snap_codec.clone(); // base blob's codec is unchanged by a delta append
            let oplog_seq = delta_seq_new + 1;
            let store = self.store();
            let txn = || -> Result<()> {
                store.exec(
                    "INSERT INTO delta_chunks(seq, payload, indices, grain, codec) VALUES (?,?,?,?,?);",
                    Some(vec![
                        delta_seq_new.into(),
                        bytes.clone().into(),
                        idx_bytes.clone().into(),
                        grain.into(),
                        snap_codec.clone().into(),
                    ]),
                )?;
                // append oplog tail row.
                store.exec(
                    "INSERT INTO oplog(seq,cell,src,host_results) VALUES (?,?,?,?);",
                    Some(vec![
                        oplog_seq.into(),
                        cell.into(),
                        src.into(),
                        host_results.clone().into(),
                    ]),
                )?;
                store.exec("DELETE FROM snap_manifest;", None)?;
                store.exec(
                    "INSERT INTO snap_manifest
                        (id,cell,epoch,n_chunks,size_raw,size_gz,engine_hash,clock_calls,rng_calls,
                         store,r2_key,created_ms,kv_json,used_heap,delta_seq,snap_mode,final_crc,snap_codec)
                     VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                    Some(vec![
                        cell.into(),
                        epoch.into(),
                        base_n_chunks.into(),
                        // size_raw is the FULL reconstructed image length of THIS delta tail (the
                        // glue's imageLen), NOT the base length — restoreW4's CRC is over size_raw.
                        size_raw.into(),
                        base_size_gz.into(),
                        base_engine.clone().into(),
                        clock_calls.into(),
                        rng_calls.into(),
                        base_store.clone().into(),
                        base_r2_key.clone().into(),
                        now_ms().into(),
                        kv_json.clone().into(),
                        used_heap.into(),
                        (delta_seq_new + 1).into(),
                        "delta".to_string().into(),
                        image_crc.into(),
                        base_codec.clone().into(),
                    ]),
                )?;
                store.exec("INSERT INTO meta(k,v) VALUES('committedCell',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;",
                    Some(vec![cell.to_string().into()]))?;
                Ok(())
            };
            // COHERENCE: stage host.fs body bytes -> R2 (awaits; durable before meta), then the
            // `fs_files` meta rows land in the SAME coalesced SQLite flush as the manifest txn below.
            self.flush_staged_fs(&store, cell).await;
            txn()?;
            // UNIFIED-FS MERGE: refresh the external manifest export AFTER the fs_files rows + heap
            // manifest are committed (best-effort; never fails the checkpoint).
            self.export_manifest().await;
            // W4 commit-ordering: the delta row is now the committed chain tail — promote the staged
            // candidate to the live delta base so the NEXT diff is against exactly this image.
            glue.commit_dump();
            return Ok(json!({
                "ok": true, "cell": cell, "store": base_store, "mode": "delta",
                "nChunks": base_n_chunks, "deltaSeq": delta_seq_new + 1, "nChanged": n_changed,
                "grain": grain, "sizeRaw": size_raw, "sizeGz": size_gz, "usedHeap": used_heap,
                "scrubbed": scrubbed, "clockCalls": clock_calls, "rngCalls": rng_calls,
                "faultTelemetry": if self.fault_test_enabled() {
                    json!({"bufferBytes": buffer_bytes, "usedHeap": used_heap, "scratchLen": scratch_len})
                } else { serde_json::Value::Null },
            }));
        }

        // ---- FULL BASE PATH (W5-compacted base; resets the delta chain + oplog) ----
        let old_r2_key: Option<String> = prev.as_ref().and_then(|m| {
            if m.store == "r2" && !m.r2_key.is_empty() {
                Some(m.r2_key.clone())
            } else {
                None
            }
        });
        let want_r2 = bytes.len() > SQLITE_HOT_MAX;
        let (store, new_r2_key, to_r2) = if want_r2 {
            let new_key = self.r2_key_for(cell, epoch);
            let bucket = self.env.bucket("SNAPSHOTS")?;
            bucket.put(&new_key, bytes.clone()).execute().await?;
            // BUG-2 FIX (verify-before-truncate): R2 is read-after-write eventually consistent for a
            // freshly-PUT key. The destructive txn below DELETEs the prior base+chain+oplog and
            // re-seeds the oplog with ONLY this cell. If the new base were momentarily unreadable on
            // a later cold wake, restore() would route to a 1-row oplog replay and SILENTLY resurrect
            // a truncated namespace (committed cells lost, reported ok). Confirm the new base is
            // GET-readable NOW — before we truncate — via the SAME resilient path restore uses. If it
            // is NOT durably readable, do not trust R2 for this rollover: persist the base into the
            // DO's own SQLite chunks (no read-after-write gap), so truncation only ever discards the
            // old chain once the new authority is verified durable. Fail-safe: a false negative just
            // keeps the base in SQLite this round.
            match self.r2_get_resilient(&new_key).await {
                R2Read::Got(_) => ("r2".to_string(), new_key, true),
                _ => {
                    console_log(
                        "[checkpoint] new R2 base unverified after PUT (read-after-write gap) — \
                         persisting base to SQLite chunks for this rollover (no data loss)",
                    );
                    ("sqlite".to_string(), String::new(), false)
                }
            }
        } else {
            ("sqlite".to_string(), String::new(), false)
        };

        let kstore = self.store();
        let r2_key_for_manifest = new_r2_key.clone();
        let txn = || -> Result<i64> {
            kstore.exec("DELETE FROM snap_chunks;", None)?;
            kstore.exec("DELETE FROM delta_chunks;", None)?;
            kstore.exec("DELETE FROM oplog;", None)?;
            kstore.exec("DELETE FROM snap_manifest;", None)?;
            let n_chunks = if to_r2 {
                0i64
            } else {
                let mut seq = 0i64;
                for chunk in bytes.chunks(CHUNK_BYTES) {
                    kstore.exec(
                        "INSERT INTO snap_chunks(seq,data) VALUES (?,?);",
                        Some(vec![seq.into(), chunk.to_vec().into()]),
                    )?;
                    seq += 1;
                }
                seq
            };
            // oplog: a full base is a fresh recovery point — seed the tail with THIS cell.
            kstore.exec(
                "INSERT INTO oplog(seq,cell,src,host_results) VALUES (0,?,?,?);",
                Some(vec![cell.into(), src.into(), host_results.clone().into()]),
            )?;
            kstore.exec(
                "INSERT INTO snap_manifest
                    (id,cell,epoch,n_chunks,size_raw,size_gz,engine_hash,clock_calls,rng_calls,
                     store,r2_key,created_ms,kv_json,used_heap,delta_seq,snap_mode,final_crc,snap_codec)
                 VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                Some(vec![
                    cell.into(),
                    epoch.into(),
                    n_chunks.into(),
                    size_raw.into(),
                    size_gz.into(),
                    get_engine_hash().into(),
                    clock_calls.into(),
                    rng_calls.into(),
                    store.clone().into(),
                    r2_key_for_manifest.clone().into(),
                    now_ms().into(),
                    kv_json.clone().into(),
                    used_heap.into(),
                    0i64.into(),
                    "full".to_string().into(),
                    image_crc.into(),
                    snap_codec.clone().into(),
                ]),
            )?;
            kstore.exec("INSERT INTO meta(k,v) VALUES('committedCell',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;",
                Some(vec![cell.to_string().into()]))?;
            Ok(n_chunks)
        };
        // COHERENCE: same as the delta path — stage host.fs bodies to R2 (durable first), then the
        // `fs_files` meta rows land in the SAME coalesced SQLite flush as the manifest txn.
        self.flush_staged_fs(&kstore, cell).await;
        let n_chunks = txn()?;
        // UNIFIED-FS MERGE: refresh the external manifest export AFTER the commit (best-effort).
        self.export_manifest().await;
        // W4 commit-ordering: the full base is committed — promote the staged image to the live base.
        glue.commit_dump();

        if let Some(old) = old_r2_key {
            if old != new_r2_key {
                if let Ok(bucket) = self.env.bucket("SNAPSHOTS") {
                    let _ = bucket.delete(&old).await;
                }
            }
        }

        Ok(json!({
            "ok": true, "cell": cell, "store": store, "mode": "full", "nChunks": n_chunks,
            "deltaSeq": 0, "sizeRaw": size_raw, "sizeGz": size_gz, "usedHeap": used_heap,
            "scrubbed": scrubbed, "clockCalls": clock_calls, "rngCalls": rng_calls, "r2Key": new_r2_key,
            "faultTelemetry": if self.fault_test_enabled() {
                json!({"bufferBytes": buffer_bytes, "usedHeap": used_heap, "scratchLen": scratch_len})
            } else { serde_json::Value::Null },
        }))
    }

    fn read_manifest(&self) -> Option<Manifest> {
        let store = self.store();
        #[derive(serde::Deserialize)]
        struct Row {
            cell: i64,
            epoch: i64,
            n_chunks: i64,
            engine_hash: String,
            clock_calls: i64,
            rng_calls: i64,
            store: String,
            r2_key: String,
            #[serde(default)]
            kv_json: Option<String>,
            #[serde(default)]
            used_heap: Option<i64>,
            #[serde(default)]
            delta_seq: Option<i64>,
            #[serde(default)]
            size_raw: Option<i64>,
            #[serde(default)]
            size_gz: Option<i64>,
            #[serde(default)]
            final_crc: Option<i64>,
            #[serde(default)]
            snap_codec: Option<String>,
        }
        let rows: Vec<Row> = store
            .query_typed(
                "SELECT cell,epoch,n_chunks,engine_hash,clock_calls,rng_calls,store,r2_key,kv_json,used_heap,delta_seq,size_raw,size_gz,final_crc,snap_codec
                 FROM snap_manifest WHERE id=1 LIMIT 1;",
                None,
            )
            .ok()?;
        rows.into_iter().next().map(|r| Manifest {
            cell: r.cell,
            epoch: r.epoch,
            n_chunks: r.n_chunks,
            engine_hash: r.engine_hash,
            clock_calls: r.clock_calls,
            rng_calls: r.rng_calls,
            store: r.store,
            r2_key: r.r2_key,
            kv_json: r.kv_json.unwrap_or_else(|| "{}".into()),
            used_heap: r.used_heap.unwrap_or(0),
            delta_seq: r.delta_seq.unwrap_or(0),
            size_raw: r.size_raw.unwrap_or(0),
            size_gz: r.size_gz.unwrap_or(0),
            final_crc: r.final_crc.unwrap_or(0),
            // back-compat: NULL/absent => "gzip" (pre-issue-#9 bases were gzip).
            snap_codec: r
                .snap_codec
                .filter(|c| !c.is_empty())
                .unwrap_or_else(|| "gzip".into()),
        })
    }

    // W4: read the committed delta chain as a JS Array of {gz, indicesGz, grain}.
    fn read_delta_chain(&self, delta_seq: i64) -> Result<js_sys::Array> {
        let out = js_sys::Array::new();
        if delta_seq <= 0 {
            return Ok(out);
        }
        let rows = self
            .store()
            .query_raw(
                "SELECT payload, indices, grain, codec FROM delta_chunks ORDER BY seq ASC;",
                None,
            )
            .map_err(|e| worker::Error::RustError(format!("read deltas: {e:?}")))?;
        let mut count = 0i64;
        for row in rows {
            let mut payload: Option<Vec<u8>> = None;
            let mut indices: Option<Vec<u8>> = None;
            let mut grain: i64 = 256;
            // issue #9: per-delta codec; NULL/absent => "gzip" (back-compat).
            let mut codec: String = "gzip".to_string();
            for (i, val) in row.into_iter().enumerate() {
                match (i, val) {
                    (0, store::StoreValue::Blob(b)) => payload = Some(b),
                    (1, store::StoreValue::Blob(b)) => indices = Some(b),
                    (2, store::StoreValue::Integer(n)) => grain = n,
                    (3, store::StoreValue::String(t)) => {
                        if !t.is_empty() {
                            codec = t;
                        }
                    }
                    _ => {}
                }
            }
            let p = payload.unwrap_or_default();
            let idx = indices.unwrap_or_default();
            let p_arr = Uint8Array::new_with_length(p.len() as u32);
            p_arr.copy_from(&p);
            let i_arr = Uint8Array::new_with_length(idx.len() as u32);
            i_arr.copy_from(&idx);
            let obj = js_sys::Object::new();
            Reflect::set(&obj, &JsValue::from_str("gz"), &p_arr).ok();
            Reflect::set(&obj, &JsValue::from_str("indicesGz"), &i_arr).ok();
            Reflect::set(
                &obj,
                &JsValue::from_str("grain"),
                &JsValue::from_f64(grain as f64),
            )
            .ok();
            Reflect::set(
                &obj,
                &JsValue::from_str("codec"),
                &JsValue::from_str(&codec),
            )
            .ok();
            out.push(&obj);
            count += 1;
        }
        if count != delta_seq {
            return Err(worker::Error::RustError(format!(
                "CorruptDeltaError: delta count mismatch (manifest delta_seq={delta_seq}, read {count})"
            )));
        }
        Ok(out)
    }

    // E6: read the committed oplog tail (cells since the last full base) as a JS Array of
    // {src, hostResults}. Used for engine-migration replay on an engine-hash mismatch.
    fn read_oplog(&self) -> js_sys::Array {
        let out = js_sys::Array::new();
        #[derive(serde::Deserialize)]
        struct Row {
            src: String,
            host_results: String,
        }
        let rows: Vec<Row> = self
            .store()
            .query_typed(
                "SELECT src, host_results FROM oplog ORDER BY seq ASC;",
                None,
            )
            .unwrap_or_default();
        for r in rows {
            let obj = js_sys::Object::new();
            Reflect::set(&obj, &JsValue::from_str("src"), &JsValue::from_str(&r.src)).ok();
            let hr: JsValue = js_sys::JSON::parse(&r.host_results).unwrap_or(JsValue::NULL);
            Reflect::set(&obj, &JsValue::from_str("hostResults"), &hr).ok();
            out.push(&obj);
        }
        out
    }

    fn read_chunks(&self, n_chunks: i64) -> Result<Uint8Array> {
        let rows = self
            .store()
            .query_raw("SELECT data FROM snap_chunks ORDER BY seq ASC;", None)
            .map_err(|e| worker::Error::RustError(format!("read chunks: {e:?}")))?;
        let mut blobs: Vec<Vec<u8>> = Vec::new();
        for row in rows {
            for val in row {
                if let store::StoreValue::Blob(b) = val {
                    blobs.push(b);
                }
            }
        }
        if blobs.len() as i64 != n_chunks {
            return Err(worker::Error::RustError(format!(
                "CorruptSnapshotError: chunk count mismatch (manifest={n_chunks}, read {})",
                blobs.len()
            )));
        }
        let total: usize = blobs.iter().map(|b| b.len()).sum();
        let out = Uint8Array::new_with_length(total as u32);
        let mut off = 0u32;
        for b in &blobs {
            let arr = Uint8Array::new_with_length(b.len() as u32);
            arr.copy_from(b);
            out.set(&arr, off);
            off += b.len() as u32;
        }
        Ok(out)
    }
}

struct RestoreInfo {
    source: String,
    read_ms: f64,
    total_server_ms: f64,
    timings_json: String,
}
impl RestoreInfo {
    fn warm() -> Self {
        RestoreInfo {
            source: "warm".into(),
            read_ms: 0.0,
            total_server_ms: 0.0,
            timings_json: "{}".into(),
        }
    }
}

struct Manifest {
    #[allow(dead_code)]
    cell: i64,
    epoch: i64,
    n_chunks: i64,
    engine_hash: String,
    clock_calls: i64,
    rng_calls: i64,
    store: String,
    r2_key: String,
    kv_json: String,
    used_heap: i64,
    delta_seq: i64,
    size_raw: i64,
    size_gz: i64,
    final_crc: i64,
    snap_codec: String,
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    let url = req.url()?;
    if url.path() == "/health" {
        return Response::ok("ok");
    }
    // Cheap pre-DO auth fast-path: if a credential IS present and invalid, 401 before spinning a
    // DO (garbage never mints a session). A credential-LESS request must still reach the DO so the
    // browser first-message {t:auth} path works. Only enforces when ENGRAM_AUTH_ENFORCE=1.
    {
        let keys: Vec<String> = env
            .secret("ENGRAM_KERNEL_KEY")
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // FAIL-CLOSED: enforce whenever a key is configured unless ENGRAM_AUTH_ENFORCE=="0".
        let enforce = !keys.is_empty()
            && env
                .var("ENGRAM_AUTH_ENFORCE")
                .ok()
                .map(|v| v.to_string())
                .map(|s| s.trim() != "0")
                .unwrap_or(true);
        if enforce {
            let (token, _) = extract_token(&req);
            if !token.is_empty() && !token_valid(&token, &keys) {
                return Response::error("unauthorized", 401);
            }
        }
    }
    let session = url
        .query_pairs()
        .find(|(k, _)| k == "id")
        .map(|(_, v)| v.to_string())
        .unwrap_or_else(|| "default".into());
    let ns = env.durable_object("KERNEL_DO")?;
    let stub = ns.id_from_name(&session)?.get_stub()?;
    stub.fetch_with_request(req).await
}

// ---- helpers ----
fn restore_timings_value(info: &RestoreInfo) -> serde_json::Value {
    let glue: serde_json::Value =
        serde_json::from_str(&info.timings_json).unwrap_or_else(|_| json!({}));
    json!({ "readMs": info.read_ms, "totalServerMs": info.total_server_ms, "glue": glue })
}

/// EXTENSIBILITY API — REDUCED Phase 1 (#32): validate config.extensions at create. Returns
/// Some(typed-error-json) on the FIRST violation, else None. Enforces: client-backend only
/// (http/worker rejected); each ext.name is a non-empty string, NOT __-prefixed, and does NOT
/// shadow a built-in host effect / namespace (fetch/ws/stream/__fs/host/...); manifest total size
/// capped. CUT (rejected): http/worker backends, binding:NAME credential injection.
fn validate_extensions(cfg: &serde_json::Value) -> Option<serde_json::Value> {
    const MAX_MANIFEST_BYTES: usize = 64 * 1024; // 64KB total manifest cap
    const MAX_EXTENSIONS: usize = 32;
    const RESERVED: &[&str] = &[
        "fetch",
        "fetchStream",
        "ws",
        "stream",
        "streamRead",
        "streamWrite",
        "streamCancel",
        "__fs",
        "fs",
        "host",
        "subLM",
        "ctx",
        "final",
        "kv",
    ];
    let exts = match cfg.get("extensions") {
        None => return None,
        Some(serde_json::Value::Null) => return None,
        Some(serde_json::Value::Array(a)) => a,
        Some(_) => {
            return Some(json!({
                "name": "ExtConfigError",
                "message": "config.extensions must be an array of manifests"
            }))
        }
    };
    // total manifest size cap (defends the persisted config + snapshot).
    let sz = serde_json::to_string(&serde_json::Value::Array(exts.clone()))
        .map(|s| s.len())
        .unwrap_or(usize::MAX);
    if sz > MAX_MANIFEST_BYTES {
        return Some(json!({
            "name": "ExtConfigError",
            "message": format!("config.extensions manifest {}B exceeds cap {}B", sz, MAX_MANIFEST_BYTES)
        }));
    }
    if exts.len() > MAX_EXTENSIONS {
        return Some(json!({
            "name": "ExtConfigError",
            "message": format!("too many extensions ({} > {})", exts.len(), MAX_EXTENSIONS)
        }));
    }
    let mut seen: Vec<String> = Vec::new();
    for e in exts {
        let obj = match e.as_object() {
            Some(o) => o,
            None => {
                return Some(json!({
                    "name": "ExtConfigError",
                    "message": "each extension must be an object {name, backend, tools, limits}"
                }))
            }
        };
        let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name.is_empty() {
            return Some(json!({
                "name": "ExtConfigError",
                "message": "extension.name must be a non-empty string"
            }));
        }
        if name.starts_with("__") {
            return Some(json!({
                "name": "ExtConfigError",
                "message": format!("extension.name '{}' may not be __-prefixed (reserved)", name)
            }));
        }
        if RESERVED.contains(&name) {
            return Some(json!({
                "name": "ExtConfigError",
                "message": format!("extension.name '{}' shadows a built-in host effect/namespace", name)
            }));
        }
        // valid identifier-ish (so host.<name>.<fn> is reachable and cannot inject arbitrary JS).
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        {
            return Some(json!({
                "name": "ExtConfigError",
                "message": format!("extension.name '{}' must be alphanumeric/_/$ only", name)
            }));
        }
        if seen.iter().any(|n| n == name) {
            return Some(json!({
                "name": "ExtConfigError",
                "message": format!("duplicate extension.name '{}'", name)
            }));
        }
        seen.push(name.to_string());
        // backend.kind MUST be "client" (default). http/worker are CUT in this phase.
        let kind = obj
            .get("backend")
            .and_then(|b| b.get("kind"))
            .and_then(|v| v.as_str())
            .unwrap_or("client");
        if kind != "client" {
            return Some(json!({
                "name": "ExtBackendError",
                "message": format!("backend.kind '{}' is not supported (only 'client' in this phase; http/worker are cut)", kind)
            }));
        }
        // tools[].fn must be valid identifiers (reachable as host.<name>.<fn>).
        if let Some(tools) = obj.get("tools").and_then(|t| t.as_array()) {
            for t in tools {
                let fnname = t.get("fn").and_then(|v| v.as_str()).unwrap_or("");
                if fnname.is_empty()
                    || !fnname
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
                {
                    return Some(json!({
                        "name": "ExtConfigError",
                        "message": format!("extension '{}' tool.fn '{}' must be a non-empty alphanumeric/_/$ identifier", name, fnname)
                    }));
                }
            }
        }
        // limits caps (defense-in-depth; glue also clamps).
        if let Some(limits) = obj.get("limits").and_then(|l| l.as_object()) {
            if let Some(cpc) = limits.get("callsPerCell").and_then(|v| v.as_i64()) {
                if cpc < 1 || cpc > 64 {
                    return Some(json!({
                        "name": "ExtConfigError",
                        "message": format!("extension '{}' limits.callsPerCell {} out of range [1,64]", name, cpc)
                    }));
                }
            }
            if let Some(mrb) = limits.get("maxResultBytes").and_then(|v| v.as_i64()) {
                if mrb < 1 || mrb > 65536 {
                    return Some(json!({
                        "name": "ExtConfigError",
                        "message": format!("extension '{}' limits.maxResultBytes {} out of range [1,65536]", name, mrb)
                    }));
                }
            }
        }
    }
    None
}

/// EXTENSIBILITY API — REDUCED Phase 1 (#32): emit ONE AE datapoint per ext call (op="ext:<name>")
/// from the glue-installed sink. Standalone (the sink closure is 'static, can't borrow &self), so it
/// captures env+do_id+generation and mirrors KernelDO::write_ae's blob/double layout. Best-effort.
fn write_ext_ae(env: &Env, do_id: &str, generation: i64, op: &str) {
    let envj: &JsValue = env.as_ref();
    let ae = match Reflect::get(envj, &JsValue::from_str("AE")) {
        Ok(v) => v,
        Err(_) => return,
    };
    if ae.is_undefined() || ae.is_null() {
        return;
    }
    let write_fn: js_sys::Function =
        match Reflect::get(&ae, &JsValue::from_str("writeDataPoint")).and_then(|f| f.dyn_into()) {
            Ok(f) => f,
            Err(_) => return,
        };
    let indexes = js_sys::Array::new();
    indexes.push(&JsValue::from_str(do_id));
    let blobs = js_sys::Array::new();
    blobs.push(&JsValue::from_str(op));
    blobs.push(&JsValue::from_str("")); // restore_source
    blobs.push(&JsValue::from_str("")); // store
    blobs.push(&JsValue::from_str("")); // error_name
    blobs.push(&JsValue::from_str("ext")); // value_type
    blobs.push(&JsValue::from_str("")); // label
    let version = version_meta_from_env(env);
    blobs.push(&JsValue::from_str(&version.id));
    blobs.push(&JsValue::from_str(&version.tag));
    blobs.push(&JsValue::from_str(&version.timestamp));
    let doubles = js_sys::Array::new();
    for _ in 0..6 {
        doubles.push(&JsValue::from_f64(0.0));
    }
    doubles.push(&JsValue::from_f64(generation as f64));
    doubles.push(&JsValue::from_f64(1.0)); // ok
    let point = js_sys::Object::new();
    let _ = Reflect::set(&point, &JsValue::from_str("indexes"), &indexes);
    let _ = Reflect::set(&point, &JsValue::from_str("blobs"), &blobs);
    let _ = Reflect::set(&point, &JsValue::from_str("doubles"), &doubles);
    let _ = write_fn.call1(&ae, &point);
    let log = json!({ "ev": "op", "op": op, "doId": do_id, "valueType": "ext",
        "generation": generation, "ok": true,
        "versionId": version.id, "versionTag": version.tag, "versionTimestamp": version.timestamp });
    console_log(&log.to_string());
}

fn version_meta_from_env(env: &Env) -> VersionMeta {
    let envj: &JsValue = env.as_ref();
    let meta = match Reflect::get(envj, &JsValue::from_str("CF_VERSION_METADATA")) {
        Ok(v) if !v.is_undefined() && !v.is_null() => v,
        _ => return VersionMeta::default(),
    };
    VersionMeta {
        id: js_prop_string(&meta, "id"),
        tag: js_prop_string(&meta, "tag"),
        timestamp: js_prop_string(&meta, "timestamp"),
    }
}

fn js_prop_string(obj: &JsValue, key: &str) -> String {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| {
            if v.is_undefined() || v.is_null() {
                None
            } else if let Some(s) = v.as_string() {
                Some(s)
            } else {
                js_sys::JSON::stringify(&v)
                    .ok()
                    .and_then(|s| s.as_string())
                    .map(|s| s.trim_matches('"').to_string())
            }
        })
        .unwrap_or_default()
}

fn read_int<S: KernelStore>(store: &S, k: &str, dflt: i64) -> i64 {
    #[derive(serde::Deserialize)]
    struct Row {
        v: String,
    }
    let rows: Vec<Row> = store
        .query_typed(
            "SELECT v FROM meta WHERE k=? LIMIT 1;",
            Some(vec![k.into()]),
        )
        .unwrap_or_default();
    rows.first().and_then(|r| r.v.parse().ok()).unwrap_or(dflt)
}

fn read_str<S: KernelStore>(store: &S, k: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Row {
        v: String,
    }
    let rows: Vec<Row> = store
        .query_typed(
            "SELECT v FROM meta WHERE k=? LIMIT 1;",
            Some(vec![k.into()]),
        )
        .unwrap_or_default();
    rows.into_iter().next().map(|r| r.v)
}

fn parse_artifact_handle(handle: &str) -> Option<(i64, String)> {
    let rest = handle.strip_prefix("cell:")?;
    if let Some(cell) = rest.strip_suffix(":text") {
        return Some((cell.parse().ok()?, "text".to_string()));
    }
    let (cell, id) = rest.split_once(":artifact:")?;
    if id.is_empty() || id.len() > 64 {
        return None;
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return None;
    }
    Some((cell.parse().ok()?, id.to_string()))
}

fn stamp_artifact_handles(v: &mut serde_json::Value, cell: i64) -> bool {
    match v {
        serde_json::Value::Array(items) => {
            let mut any = false;
            for item in items {
                any |= stamp_artifact_handles(item, cell);
            }
            any
        }
        serde_json::Value::Object(obj) => {
            let mut any = false;
            let is_artifact = obj
                .get("kind")
                .and_then(|v| v.as_str())
                .map(|s| s == "artifact")
                .unwrap_or(false);
            if is_artifact {
                if let Some(handle) = obj.get("handle").and_then(|v| v.as_str()) {
                    if let Some(id) = handle.strip_prefix("pending:") {
                        let stamped = if id == "text" {
                            format!("cell:{cell}:text")
                        } else {
                            format!("cell:{cell}:artifact:{id}")
                        };
                        obj.insert("handle".to_string(), json!(stamped));
                        any = true;
                    }
                }
            }
            for value in obj.values_mut() {
                any |= stamp_artifact_handles(value, cell);
            }
            any
        }
        _ => false,
    }
}

/// Persist the session config to meta WITHOUT panicking on an oversize value. SQLite's row/blob
/// ceiling (SQLITE_TOOBIG, ~1GB but workerd caps far lower) would make the raw `write_meta` `.expect`
/// panic -> WS-1006 DoS. We pre-reject any config above a generous ceiling with a typed error so the
/// socket stays alive. Returns Some(error-json) on rejection, None on success.
fn persist_config<S: KernelStore>(store: &S, cfg_str: &str) -> Option<serde_json::Value> {
    // 512KB ceiling — far above any legitimate config (the extensions manifest alone caps at 64KB),
    // far below SQLite's hard limit, so this only catches abusive/oversize payloads.
    const MAX_CONFIG_BYTES: usize = 512 * 1024;
    if cfg_str.len() > MAX_CONFIG_BYTES {
        return Some(json!({
            "name": "ConfigTooBigError",
            "message": format!("config {}B exceeds cap {}B", cfg_str.len(), MAX_CONFIG_BYTES)
        }));
    }
    if store
        .exec(
            "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;",
            Some(vec!["config".into(), cfg_str.into()]),
        )
        .is_err()
    {
        return Some(json!({
            "name": "ConfigPersistError",
            "message": "failed to persist config (storage rejected the write)"
        }));
    }
    None
}

fn write_meta<S: KernelStore>(store: &S, k: &str, v: &str) {
    store
        .exec(
            "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;",
            Some(vec![k.into(), v.into()]),
        )
        .expect("write meta");
}

/// UNIFIED-FS MERGE: the session-monotonic `fsVersion` counter (the Rust twin of @engram/fs's
/// ManifestStore.bumpVersion). Bumped on EVERY durable fs mutation (cell flush, vfs-write, reconcile)
/// so an external reader / the exported manifest can detect a stale view. Stored as a plain `meta`
/// row — when called inside flush_staged_fs it lands in the SAME coalesced SQLite flush as the heap
/// manifest, preserving the staged-commit coherence invariant. Returns the new value. Best-effort:
/// a read failure restarts the counter from 0 (the counter is advisory, never a coherence authority).
fn bump_fs_version<S: KernelStore>(store: &S) -> i64 {
    let next = read_int(store, "fsVersion", 0) + 1;
    write_meta(store, "fsVersion", &next.to_string());
    next
}

fn read_fs_version<S: KernelStore>(store: &S) -> i64 {
    read_int(store, "fsVersion", 0)
}

// ── R2 RESTORE RESILIENCE (issue #10: transient R2 hiccup on the >SQLITE_HOT_MAX overflow
//    restore path used to bubble a 500 → client reconnect+{t:create} → fresh-over-durable WIPE).
//    Constants govern the retry/timeout/circuit-breaker wrapped around the single R2 GET. ───────
const R2_GET_ATTEMPTS: usize = 4;
// Deterministic (jitter-free — determinism invariant) exponential-ish backoff between attempts.
const R2_GET_BACKOFF_MS: [u64; 3] = [50, 150, 400];
// Per-attempt wall-clock ceiling: a single hung GET must not eat the whole DO request budget.
const R2_GET_TIMEOUT_MS: u64 = 4000;
// Circuit breaker: after this many CONSECUTIVE exhausted R2 reads, skip R2 entirely (go straight to
// oplog-replay) until the cooldown elapses — avoids stacking multi-second timeouts on a degraded R2.
const R2_BREAKER_TRIP_AT: i64 = 3;
const R2_BREAKER_COOLDOWN_MS: f64 = 30_000.0;
const R2_BREAKER_FAILS_KEY: &str = "r2BreakerFails";
const R2_BREAKER_OPEN_UNTIL_KEY: &str = "r2BreakerOpenUntil";

/// Outcome of a resilient R2 read attempt set.
enum R2Read {
    /// Body bytes obtained.
    Got(Uint8Array),
    /// The key is durably absent (404-class) — NOT a transient failure; recover via oplog replay.
    Missing,
    /// All retries exhausted on transient errors/timeouts — session is INTACT (do NOT wipe);
    /// caller must route to oplog-replay / typed-retryable, never to create-fresh.
    Exhausted,
}

/// A 2-future select: resolves with the first branch to complete. Used to race an R2 GET against a
/// `Delay` timeout without pulling in the `futures` crate. Left = the work, Right = the timeout.
struct Select2<A, B> {
    a: Pin<Box<A>>,
    b: Pin<Box<B>>,
}
enum Either<TA, TB> {
    Left(TA),
    Right(TB),
}
impl<A: Future, B: Future> Future for Select2<A, B> {
    type Output = Either<A::Output, B::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = self.a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = self.b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    }
}
async fn race_timeout<F: Future>(work: F, timeout_ms: u64) -> Option<F::Output> {
    let sel = Select2 {
        a: Box::pin(work),
        b: Box::pin(Delay::from(Duration::from_millis(timeout_ms))),
    };
    match sel.await {
        Either::Left(v) => Some(v),
        Either::Right(_) => None,
    }
}

fn now_ms() -> f64 {
    js_sys::Date::now()
}

/// console.log one line via the global console (no web-sys dependency). Best-effort.
fn console_log(s: &str) {
    if let Some(global) = js_sys::global().dyn_ref::<js_sys::Object>() {
        if let Ok(console) = Reflect::get(global, &JsValue::from_str("console")) {
            if let Ok(log) = Reflect::get(&console, &JsValue::from_str("log")) {
                if let Ok(f) = log.dyn_into::<js_sys::Function>() {
                    let _ = f.call1(&console, &JsValue::from_str(s));
                }
            }
        }
    }
}

fn clone_glue(g: &GlueKernel) -> GlueKernel {
    let v: &JsValue = g.as_ref();
    v.clone().unchecked_into()
}

fn to_err(e: JsValue) -> worker::Error {
    worker::Error::RustError(format!("{:?}", e))
}

/// Extract `(name, message)` from a JsValue error thrown by the registry glue. A thrown JS Error
/// carries `.name`/`.message`; anything else stringifies. Used to surface a typed worker-invoke
/// error (e.g. RegistryUnavailableError) without leaking a stack beyond the message.
fn js_error_parts(e: &JsValue) -> (String, String) {
    let name = Reflect::get(e, &JsValue::from_str("name"))
        .ok()
        .and_then(|v| v.as_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "WorkerRuntimeError".to_string());
    let message = Reflect::get(e, &JsValue::from_str("message"))
        .ok()
        .and_then(|v| v.as_string())
        .filter(|s| !s.is_empty())
        .or_else(|| e.as_string())
        .unwrap_or_else(|| "registry invoke failed".to_string());
    (name, message)
}

// ── SHA-256 (pure Rust, wasm32-safe, sync; no crate dep) ────────────────────────────────────────
// Used to content-address a registered worker source: hash = lowercase-hex sha256(source). The
// hash is immutable (same source = same hash), drives the R2 `workers/<hash>.js` key, and is the
// Worker-Loader warm-cache discriminant. FIPS-180-4 reference implementation.
const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

fn sha256_hex(data: &[u8]) -> String {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    // Pre-process: append 0x80, pad to 56 mod 64, append the 64-bit big-endian bit length.
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    let mut w = [0u32; 64];
    for block in msg.chunks_exact(64) {
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = String::with_capacity(64);
    for word in h.iter() {
        out.push_str(&format!("{:08x}", word));
    }
    out
}

/// Standard base64 alphabet for the vfs-* wire (dataB64). Self-contained (no base64 crate dep) —
/// the host.fs glue uses btoa/atob at the engine edge; vfs-* crosses bytes the same way DO-side.
const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode raw bytes to a standard (padded) base64 string.
fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(B64_ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            B64_ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64_ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Decode a standard base64 string (padding optional; whitespace ignored). Returns None on a
/// non-base64 byte so a malformed upload is a clean typed error, never a panic/garbage write.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let dec = |c: u8| -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let mut acc: u32 = 0;
    let mut nbits: u32 = 0;
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for &c in s.as_bytes() {
        if c == b'=' || c.is_ascii_whitespace() {
            continue;
        }
        let v = dec(c)? as u32;
        acc = (acc << 6) | v;
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
        }
    }
    Some(out)
}

/// THE /workspace CANONICAL ROOT — the ONE filesystem root every path resolves under (owner's
/// invariant; mirrors @engram/fs WORKSPACE_ROOT). A bare-absolute "/x" is ROOT-relative ("/x" ->
/// /workspace/x), a "/workspace/x" re-roots at /workspace, a relative "x" resolves against the CWD.
const WORKSPACE_ROOT: &str = "/workspace";

/// Normalize a guest fs path to a /workspace-rooted R2-RELATIVE path (leading-slash `<rel>` form,
/// e.g. "/a/b"), honoring the session `cwd` (default /workspace) for relative paths. THE RULE:
///   - "/workspace/a/b" / "/a/b" -> ROOT-relative -> rel "/a/b"   (back-compat: bare-/ unchanged)
///   - "a/b" / "./a" / "../a"    -> CWD-relative  -> join(cwd, p), then normalize
/// `.`/`..` are normalized and CLAMPED under /workspace; a `..` that escapes /workspace throws
/// EACCES (parity with @engram/fs). NUL throws EINVAL. The returned LEADING-slash form keeps the
/// existing key derivation (`format!("{}{}", prefix, path.trim_start_matches('/'))`) UNCHANGED, so
/// every previously-written fs/<doId>/a/b key stays reachable — NO data migration.
fn norm_fs_path_cwd(p: &str, cwd: &str) -> std::result::Result<String, JsValue> {
    if p.contains('\u{0}') {
        return Err(JsValue::from_str("EINVAL: path contains NUL byte"));
    }
    if cwd.contains('\u{0}') {
        return Err(JsValue::from_str("EINVAL: cwd contains NUL byte"));
    }
    // Strip ONE leading "/workspace" so a "/workspace/..."-form input/cwd re-roots at /workspace.
    let strip_ws = |s: &str| -> String {
        if s == WORKSPACE_ROOT {
            String::new()
        } else if let Some(rest) = s.strip_prefix(&format!("{}/", WORKSPACE_ROOT)) {
            format!("/{}", rest)
        } else {
            s.to_string()
        }
    };
    // Build the raw segment chain rooted under /workspace (carry the literal "workspace" floor).
    let ws_floor = 1usize; // ["workspace"]
    let mut chain: Vec<String> = vec!["workspace".to_string()];
    if p.starts_with('/') {
        // ROOT-relative (ignores cwd).
        let rooted = strip_ws(p);
        chain.extend(rooted.split('/').map(|s| s.to_string()));
    } else {
        // CWD-relative: normalize cwd (root-relative under /workspace) then append p.
        let cwd_norm = strip_ws(cwd);
        for seg in cwd_norm.split('/') {
            chain.push(seg.to_string());
        }
        for seg in p.split('/') {
            chain.push(seg.to_string());
        }
    }
    // Normalize ./.. with a clamp at the /workspace floor (escape -> EACCES).
    let mut out: Vec<&str> = Vec::new();
    for seg in chain.iter() {
        match seg.as_str() {
            "" | "." => continue,
            ".." => {
                if out.len() <= ws_floor {
                    return Err(JsValue::from_str("EACCES: path escapes /workspace"));
                }
                out.pop();
            }
            s => out.push(s),
        }
    }
    // Strip the /workspace floor -> leading-slash <rel> form ("/a/b"; root -> "/").
    Ok(format!("/{}", out[ws_floor..].join("/")))
}

/// Back-compat shim: resolve a path against the DEFAULT /workspace CWD. Used by the out-of-band
/// vfs-*/reconcile paths (a hash-worker/container invocation's CWD defaults to /workspace).
fn norm_fs_path(p: &str) -> std::result::Result<String, JsValue> {
    norm_fs_path_cwd(p, WORKSPACE_ROOT)
}

/// DO-side R2 servicer for the in-VM `fs` host-backed provider (config.fs.provider == "r2").
/// `payload` is the JS object the glue forwards: { op, path, bytes?:Uint8Array }. Returns a JS
/// object the glue marshals back to the engine: { ok, bytes?:Uint8Array, names?, size?, isFile?,
/// isDirectory?, error? }. Binary crosses as Uint8Array (glue does the base64 at the engine edge).
///
/// COHERENCE: write/delete are STAGED in `staged` (DO memory) and flushed at checkpoint (see
/// flush_staged_fs) — they do NOT touch R2 here. Reads/list/stat overlay `staged` (this cell's
/// pending mutations) on `committed` (the path namespace as of eval start) and fall through to R2
/// for the actual body bytes of already-committed files. This makes the fs namespace commit in the
/// SAME version as the heap (a cold restore never sees a file from an uncommitted cell, nor a heap
/// referencing a not-yet-durable file).
///
/// ISOLATION: `prefix` is always `fs/<doId>/` (DO-id-bound, never user config) and `path` is
/// normalized `..`-free, so the R2 key can never address another session's namespace.
async fn r2_fs_op(
    env: &Env,
    binding: &str,
    prefix: &str,
    staged: &StagedFs,
    committed: &FsCommitted,
    payload: JsValue,
) -> std::result::Result<JsValue, JsValue> {
    let getp = |k: &str| Reflect::get(&payload, &JsValue::from_str(k)).ok();
    let op = getp("op").and_then(|v| v.as_string()).unwrap_or_default();
    let raw_path = getp("path").and_then(|v| v.as_string()).unwrap_or_default();
    let path = match norm_fs_path(&raw_path) {
        Ok(p) => p,
        Err(e) => {
            let out = js_sys::Object::new();
            let _ = Reflect::set(&out, &JsValue::from_str("error"), &e);
            return Ok(out.into());
        }
    };
    let key = format!("{}{}", prefix, path.trim_start_matches('/'));
    let out = js_sys::Object::new();
    let set = |k: &str, v: &JsValue| {
        let _ = Reflect::set(&out, &JsValue::from_str(k), v);
    };
    let enoent = |p: &str| format!("ENOENT: no such file or directory, '{}'", p);

    // Resolve the effective view of `path`: this cell's staged op wins over the committed namespace.
    // Returns: Some(Some((bytes_opt, size))) = exists (bytes_opt None => committed-only, read from R2);
    //          Some(None) = staged-deleted (treat as absent); None = not staged (check committed).
    let staged_view = |p: &str| -> Option<Option<(Option<Vec<u8>>, i64)>> {
        let st = staged.borrow();
        // last staged op for this path wins
        for s in st.iter().rev() {
            if s.path == p {
                return Some(match &s.op {
                    FsStageOp::Write(b) => Some((Some(b.clone()), b.len() as i64)),
                    FsStageOp::Delete => None,
                });
            }
        }
        None
    };

    // Ranged read window (engine chunks a large body across calls): off/len in bytes. Absent => whole.
    let off = getp("off").and_then(|v| v.as_f64()).unwrap_or(0.0) as usize;
    let len = getp("len").and_then(|v| v.as_f64());

    match op.as_str() {
        "read" => {
            // Resolve the body source: staged-this-cell wins over committed. Returns the TOTAL size
            // (so the engine knows how many chunks to pull) + the requested [off, off+len) slice.
            let slice_out = |full: &[u8]| -> (i64, Uint8Array) {
                let total = full.len();
                let start = off.min(total);
                let end = match len {
                    Some(l) => (start + (l as usize)).min(total),
                    None => total,
                };
                let part = &full[start..end];
                let arr = Uint8Array::new_with_length(part.len() as u32);
                arr.copy_from(part);
                (total as i64, arr)
            };
            match staged_view(&path) {
                Some(None) => set("error", &JsValue::from_str(&enoent(&raw_path))), // staged-deleted
                Some(Some((Some(bytes), _))) => {
                    let (total, arr) = slice_out(&bytes);
                    set("ok", &JsValue::TRUE);
                    set("size", &JsValue::from_f64(total as f64));
                    set("bytes", &arr.into());
                }
                _ => {
                    // not staged: read committed body from R2 (ranged when off/len given).
                    match committed.get(&path) {
                        // No committed fs_files row. The engram-sandbox CONTAINER writes bodies
                        // DIRECTLY to R2 (s3fs mount) at the identical canonical key fs/<doId>/<rel>
                        // WITHOUT a committed meta row — so a container-written file is invisible to
                        // committed.get(). Fall back to a direct R2 GET at the same key: this is the
                        // transparent container->cell half of the shared VFS (a cell's
                        // fs.promises.readFile('/workspace/x') reads the SAME bytes the container
                        // wrote at /workspace/x). The TOTAL size comes from the R2 object itself; a
                        // genuinely-absent key still yields ENOENT. Reads only this session's
                        // DO-id-bound prefix, so isolation is preserved.
                        None => {
                            let bucket = env.bucket(binding).map_err(|e| {
                                JsValue::from_str(&format!("FsError: bucket '{}' — {e}", binding))
                            })?;
                            // HEAD first for the authoritative total size, then a ranged/whole GET.
                            let total_size: Option<u64> = match bucket.head(&key).await {
                                Ok(Some(obj)) => Some(obj.size() as u64),
                                _ => None,
                            };
                            match total_size {
                                None => set("error", &JsValue::from_str(&enoent(&raw_path))),
                                Some(total) => {
                                    let mut gb = bucket.get(&key);
                                    if off > 0 || len.is_some() {
                                        let length = len
                                            .map(|l| l as u64)
                                            .unwrap_or(total.saturating_sub(off as u64));
                                        gb = gb.range(worker::Range::OffsetWithLength {
                                            offset: off as u64,
                                            length,
                                        });
                                    }
                                    match gb.execute().await {
                                        Ok(Some(obj)) => {
                                            let body = obj.body().ok_or_else(|| {
                                                JsValue::from_str("FsError: empty body")
                                            })?;
                                            let bytes = body
                                                .bytes()
                                                .await
                                                .map_err(|e| JsValue::from_str(&format!("{e}")))?;
                                            let arr =
                                                Uint8Array::new_with_length(bytes.len() as u32);
                                            arr.copy_from(&bytes);
                                            set("ok", &JsValue::TRUE);
                                            set("size", &JsValue::from_f64(total as f64));
                                            set("bytes", &arr.into());
                                        }
                                        // Raced delete between HEAD and GET => treat as absent.
                                        _ => set("error", &JsValue::from_str(&enoent(&raw_path))),
                                    }
                                }
                            }
                        }
                        Some(meta) => {
                            let bucket = env.bucket(binding).map_err(|e| {
                                JsValue::from_str(&format!("FsError: bucket '{}' — {e}", binding))
                            })?;
                            let mut gb = bucket.get(&key);
                            if off > 0 || len.is_some() {
                                let length = len
                                    .map(|l| l as u64)
                                    .unwrap_or((meta.size as u64).saturating_sub(off as u64));
                                gb = gb.range(worker::Range::OffsetWithLength {
                                    offset: off as u64,
                                    length,
                                });
                            }
                            match gb.execute().await {
                                Ok(Some(obj)) => {
                                    let body = obj
                                        .body()
                                        .ok_or_else(|| JsValue::from_str("FsError: empty body"))?;
                                    let bytes = body
                                        .bytes()
                                        .await
                                        .map_err(|e| JsValue::from_str(&format!("{e}")))?;
                                    let arr = Uint8Array::new_with_length(bytes.len() as u32);
                                    arr.copy_from(&bytes);
                                    set("ok", &JsValue::TRUE);
                                    // size = the TOTAL committed file size (from meta), not the slice.
                                    set("size", &JsValue::from_f64(meta.size as f64));
                                    set("bytes", &arr.into());
                                }
                                // Committed meta but missing body = a torn write (per SANDBOX-API):
                                // report it distinctly so callers can detect, never return garbage.
                                _ => set(
                                    "error",
                                    &JsValue::from_str(&format!(
                                        "ENOENT: torn file (committed meta, body absent), '{}'",
                                        raw_path
                                    )),
                                ),
                            }
                        }
                    }
                }
            }
        }
        "write" => {
            let bytes_val = getp("bytes").unwrap_or(JsValue::NULL);
            let arr: Uint8Array = bytes_val
                .dyn_into()
                .map_err(|_| JsValue::from_str("FsError: write bytes missing"))?;
            let chunk = getp("chunk").and_then(|v| v.as_f64()).unwrap_or(0.0) as i64;
            let new_bytes = arr.to_vec();
            // STAGE (no R2 write until checkpoint). A chunked write (body > 64KB) arrives as several
            // host calls: chunk 0 REPLACES (start a fresh staged Write for this path), chunk>0 APPENDS
            // to the last staged Write for the same path so the host assembles the whole body in DO
            // memory before the checkpoint flush.
            let mut st = staged.borrow_mut();
            if chunk > 0 {
                // find the last staged Write for this path and extend it
                if let Some(s) = st.iter_mut().rev().find(|s| s.path == path) {
                    if let FsStageOp::Write(ref mut buf) = s.op {
                        buf.extend_from_slice(&new_bytes);
                    } else {
                        // last op was a delete: start a fresh write with just this chunk
                        st.push(FsStage {
                            path: path.clone(),
                            op: FsStageOp::Write(new_bytes),
                        });
                    }
                } else {
                    st.push(FsStage {
                        path: path.clone(),
                        op: FsStageOp::Write(new_bytes),
                    });
                }
            } else {
                st.push(FsStage {
                    path: path.clone(),
                    op: FsStageOp::Write(new_bytes),
                });
            }
            set("ok", &JsValue::TRUE);
        }
        "delete" => {
            // STAGE a tombstone (flushed at checkpoint: DELETE fs_files row + best-effort R2 delete).
            staged.borrow_mut().push(FsStage {
                path: path.clone(),
                op: FsStageOp::Delete,
            });
            set("ok", &JsValue::TRUE);
        }
        "stat" => {
            let size = match staged_view(&path) {
                Some(None) => None,                           // staged-deleted => absent
                Some(Some((_, sz))) => Some(sz),              // staged write => its size
                None => committed.get(&path).map(|m| m.size), // committed size
            };
            match size {
                Some(sz) => {
                    set("ok", &JsValue::TRUE);
                    set("isFile", &JsValue::TRUE);
                    set("isDirectory", &JsValue::FALSE);
                    set("size", &JsValue::from_f64(sz as f64));
                }
                None => set("error", &JsValue::from_str(&enoent(&raw_path))),
            }
        }
        "list" => {
            // The committed namespace overlaid with this cell's staged writes/deletes, scoped to the
            // requested dir prefix. Returns the immediate child names (files + synthetic subdir names).
            let dir = if path == "/" {
                "/".to_string()
            } else {
                format!("{}/", path)
            };
            let pre = if path == "/" { "/" } else { dir.as_str() };
            // effective path set = committed - staged-deletes + staged-writes
            let mut eff: std::collections::BTreeSet<String> = committed.keys().cloned().collect();
            for s in staged.borrow().iter() {
                match s.op {
                    FsStageOp::Write(_) => {
                        eff.insert(s.path.clone());
                    }
                    FsStageOp::Delete => {
                        eff.remove(&s.path);
                    }
                }
            }
            let names = js_sys::Array::new();
            let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for full in eff.iter() {
                if let Some(rest) = full.strip_prefix(pre) {
                    let name = rest.split('/').next().unwrap_or("");
                    if !name.is_empty() && seen.insert(name.to_string()) {
                        names.push(&JsValue::from_str(name));
                    }
                }
            }
            set("ok", &JsValue::TRUE);
            set("names", &names.into());
        }
        other => set(
            "error",
            &JsValue::from_str(&format!("FsError: unknown op '{}'", other)),
        ),
    }
    Ok(out.into())
}

fn num_field(obj: &JsValue, k: &str) -> Option<f64> {
    Reflect::get(obj, &JsValue::from_str(k))
        .ok()
        .and_then(|v| v.as_f64())
}

fn str_field(obj: &JsValue, k: &str) -> Option<String> {
    Reflect::get(obj, &JsValue::from_str(k))
        .ok()
        .and_then(|v| v.as_string())
}
