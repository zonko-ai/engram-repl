# Engram Durability Roadmap — Index + Remaining Rungs

> The proven, sequenced plan to harden Engram's durability + cold-start, all snapshot-thesis-safe,
> Cloudflare-only, no brain rewrite. Each rung was built + measured in the expeditions
> (`docs/WASM-EXPEDITIONS.md`, `docs/WASM-EXPEDITIONS-2.md`). Detailed plans linked; the three smaller
> remaining rungs (E6/W3/E4) are specced inline here.
>
> **Build discipline:** experiments/specs only until owner OK; every rung ships behind its verify gate
> (evict→cold-restore regression + seeded-determinism byte-identity) measured on **real workerd**, with
> a deployed-version rollback anchor.

---

## 2026-06-15 chaos fault-injection closure

Fault-test hooks and local chaos gates now live under `tests/chaos/fault-injection/`; see
`docs/CHAOS-FAULT-INJECTION-VERDICT.md`.

- BUG-1: raw restore admission is now gated by `MEASURED_RESTORE_RAW_CEILING_BYTES = 60MiB`
  (`SAFE_SERIALIZE_BUFFER_BYTES` and `MAX_RESTORE_RAW_BYTES` lockstep), while the stricter 24MiB
  incompressible-live-heap guard remains in place.
- BUG-2: verify-before-truncate remains the code fix; the local harness includes the one-shot
  `r2_get_resilient` durable-miss hook, but local safe payloads stayed in SQLite, so the real R2
  overflow miss leg still needs a throwaway Cloudflare run if we want black-box R2 timing evidence.
- BUG-3: parked continuation vs mutex-free `vfs-write` is locally chaos-verified: a frame-origin write
  committed during `host.pause` survives the resumed cell's staged flush.

## The sequence

```
   ┌ W5 compaction ┐─▶┌ W4 byte-delta ┐─▶┌ E6 oplog tail ┐─▶┌ W3 asyncify ┐─▶┌ E4 wizer-bake ┐
   │ un-wedge P0   │  │ ~3× less write │  │ crash tail +   │  │ mid-cell    │  │ faster cold   │
   │ 96.9% reclaim │  │ zero re-fire   │  │ engine-migrate │  │ preempt     │  │ create        │
   └───────────────┘  └────────────────┘  └────────────────┘  └─────────────┘  └───────────────┘
        P0 fix            efficiency           recovery net        opt-in           cold-start
   docs/W5-COMPACTION  docs/W4-BYTEDELTA       (below)             (below)          (below)
   -PLAN.md            -PLAN.md
```

Rationale for order: W5 first — it closes a **years-open correctness bug** (a spiked-then-freed session
literally *cannot recover* today). W4 next — a compacted base makes deltas smallest. E6 — the
sub-checkpoint crash tail + the engine-migration escape hatch. W3/E4 — opt-in latency wins.

---

## E6 — oplog tail + engine-migration (recovery net)

**Proven:** `docs/WASM-EXPEDITIONS.md` E6 — oplog-compaction hybrid cut durable bytes 80–96.5%,
write-amp 39.5×→1.4–7.9×, byte-identical restore, sub-15ms replaying 24 cells.

**Role after W4 lands:** W4 byte-delta becomes the primary per-cell durability (zero re-fire), so E6's
oplog is **narrowed to two jobs**, not the main path:

```
   1. CRASH TAIL — host calls + cell sources since the last committed delta/snapshot.
      On crash recovery: restore last checkpoint + replay only the uncommitted tail.
   2. ENGINE MIGRATION — when quickjs.wasm hash changes, the heap image is version-locked
      and won't restore. Replay the retained source-cell oplog into the NEW engine instead
      of bricking. (Engram already has a partial journal; E6 generalizes it.)
```

```
   oplog entry = { seq, kind: cell|hostcall, source|result, seededClockTick, rngCounter }
   replay = deterministic (seeded entropy recorded) ⇒ byte-identical re-derivation
```

**Tradeoff (state honestly):** replay within the oplog window relaxes the pure no-replay guarantee —
effectful host calls are replayed from *recorded results* (not re-fired), so side effects do NOT
re-execute; only pure recomputation happens. Pure cells faithful; the recorded-result design keeps
effects from double-firing.

**Verify gate:** crash mid-cell → recover = last checkpoint + tail replay, exactly-once effects;
engine-hash bump → migrate via replay, state intact; determinism byte-identical.

---

## W3 — Asyncify mid-cell preemption (opt-in)

**Proven:** `docs/WASM-EXPEDITIONS-2.md` W3 — Binaryen Asyncify gives true arbitrary-point
pause/resume (demonstrated mid-loop), **1.36× size** when safely instrumented. Closes the gap chiwawa
couldn't adopt (chiwawa interprets the guest; Asyncify rewrites our actual QuickJS wasm).

**Role:** today Engram can only snapshot at **cell / interrupt boundaries** — a single long-running
cell can't be checkpointed mid-execution (and a tight loop is killed by the tick-budget). Asyncify lets
a cell be **unwound to the host mid-execution**, snapshotted at *any* instruction, and rewound after
restore.

```
   guest: long cell running ──tick budget hit──▶ ASYNCIFY UNWIND (stack → linear memory)
                                                        │  snapshot now (mid-cell!)
                                                        ▼  evict / hibernate
                                              cold restore ──▶ ASYNCIFY REWIND ──▶ cell continues
```

**Cost + decision:** Asyncify bloats size (1.36×) and slows execution. So make it an **opt-in
"preemptible session" mode** (config flag), NOT the default. Never use `--ignore-indirect` (unsafe —
W3 finding). Default sessions keep the cheap cell-boundary model; latency/long-job sessions opt in.

**Verify gate:** long cell unwinds mid-execution → snapshot → cold-restore → rewind → cell completes
correctly + exactly-once; size delta measured on real workerd; non-preemptible sessions unaffected.

---

## E4 — Wizer-bake cold-create (latency)

**Proven:** `docs/WASM-EXPEDITIONS.md` E4 — wizer pre-initializes a booted QuickJS + injected stdlib
heap at **deploy time**, moving the ~143 ms/MB stdlib-inject cost off the per-session cold-create path.
Baked module instantiates from a precompiled `WebAssembly.Module` → **CompiledWasm-compatible** (no
runtime raw-byte compile, workerd-legal).

**Role:** v0.4/v0.6 found stdlib injection costs ~80–140 ms per session at create. Wizer-bake pays it
**once at build**:

```
   today:  cold create ─▶ boot QuickJS ─▶ inject stdlib (~80-140ms) ─▶ ready
   E4:     deploy-time wizer-bake the booted+injected heap into quickjs-stdlib-baked.wasm
           cold create ─▶ instantiate baked module (already has stdlib) ─▶ ready   (injection gone)
```

**Composes with snapshots:** baking is the *cold-create* path (no live user state); per-session
snapshots are unchanged. Split the two concerns cleanly (matches the v0.4 finding).

**Build note:** E4's prototype hit an esbuild-target parse blocker — fix that first. Bake only the
*default* stdlib set; opt-in modules still inject at runtime.

**Verify gate:** baked module instantiates under CompiledWasm; cold-create latency drop measured on
real workerd; baked stdlib identical to runtime-injected; snapshots from baked sessions restore.

---

## R2-tail mitigation — cold-restore latency for big incompressible heaps (real-CF-exposed)

**Real-CF finding:** >2MB-gz heaps route to R2; R2 GET = ~300ms warm / ~900ms cold / ~1.8s p95 — the
single biggest owned restore cost. **NOW REAL-CF-MEASURED** (engram-bench, `docs/R2TAIL-REALCF.md`).
The round-4 local sim was **WRONG on 2 of 3** — real numbers below supersede it:

```
   mitigation                         REAL-CF result (sim claim → actual)
   ──────────                         ──────
   chunked-parallel, multi-object     SIM 3.9×/7.65×  →  REAL 0.44-0.98× REGRESSION + k≥16 hits
                                      workerd connection cap. R2 cold GET is connection-bound,
                                      not bandwidth-bound. DROP.
   prefer-SQLite via gz-9             SIM flips tier  →  REAL <1% (0.67-0.80%) smaller, NEVER flips
                                      an incompressible image under the 2MB line. DROP.
   ★ HOT-TIER: gz image in DO-SQLite  SIM ~0ms        →  REAL readMs=0 (no GET), confirmed. Restore
     64KB rows                        ≤66ms @5MB, ~2s @20MB (pure gunzip). SHIP.
```

**Recommendation (updated, real-CF):** **HOT-TIER is the decisive and only real win.** Keep the gz
image in DO-SQLite 64KB rows (above the 2MB-gz R2-overflow line) for latency-sensitive sessions whose
gz is still ≲ a few MB → restore is in-turn ~0ms read + gunzip CPU, eliminating the 0.9–3.9s R2 GET
tail. **Drop chunked-parallel and gz-9** — both fail on real CF (regression / <1%). Gunzip cost grows
with raw size (~2s @20MB), so hot-tier wins clearest for smaller gz. Build as a routing-policy tweak.

## Parked (door open, no parity reason now)

```
   W1 Rust-QuickJS brain   VIABLE (wasip1 runs on CF) but zero capability gain vs current — park
   W2 Boa brain            snapshottable, ~2× size, unproven at parity — park
   chiwawa mid-stack       not adoptable (interprets guest) — W3 Asyncify covers the need
   JS_WriteObject snapshot  DEAD (loses 7/16 value kinds)
   non-CF substrates        none beat the heap-image primitive on CF (only off-CF Firecracker)
```

Revive conditions tracked in `docs/WASM-EXPEDITIONS-2.md` autopsy (W6).

---

## Cross-cutting: the coherence invariant

All host-mediated state (fs/kv/ctx/timers — see `docs/SANDBOX-API.md`) and all durability rungs share
one rule, mirroring the existing crash-atomic checkpoint:

> **Every host mutation in a cell must durably commit before that cell's checkpoint commits.** The
> checkpoint is the single commit point; host writes are staged and flushed atomically with it. A crash
> before commit rolls back to the previous good checkpoint — no torn state.

This is what makes W4/W5/E6 + the sandbox APIs safe to compose.
