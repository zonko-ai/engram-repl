# Chaos Fault-Injection Verdict

Date: 2026-06-15

Scope: local workerd/Wrangler fault-test worker only. No production baseline traffic was sent to
`wss://engram.umgbhalla.xyz`; the production `apps/kernel/wrangler.jsonc` has no
`ENGRAM_FAULT_TEST` variable.

## What landed

- Gated fault mode: active only when `ENGRAM_FAULT_TEST=1` and session config has
  `{ faultTest: true }`.
- Gated eval checkpoint telemetry: `bufferBytes`, `usedHeap`, and `scratchLen` are echoed only
  in fault mode.
- One-shot R2 miss injector in `r2_get_resilient(key)`, configured through the test-only
  `{ t: "_fault", op: "r2-miss-once", key }` frame.
- Test-only forced R2 full-base rollover frame:
  `{ t: "_fault", op: "force-r2-next-full" }`.
- Test-only `checkpoint-evict` control frame for parked-eval experiments.
- Local harnesses: `tests/chaos/fault-injection/run.mjs` and
  `tests/chaos/fault-injection/ceilings.mjs`.

## Fixed-tree evidence

Command:

```sh
PATH="$HOME/.cargo/bin:$PATH" node tests/chaos/fault-injection/run.mjs
```

Result file:

```
scratch/beam/fault-injection-results/fault-run-1781500839217.json
```

Summary:

| Case | Verdict | Evidence |
|---|---|---|
| Prod safety gate | PASS | `apps/kernel/wrangler.jsonc` does not define `ENGRAM_FAULT_TEST`. |
| Telemetry echo | PASS | `bufferBytes=20185088`, `scratchLen=1048576`, `usedHeap=662075`. |
| BUG-1 admission | CLOSED for measured local gate | 8MiB and 16MiB payloads checkpoint; 24MiB+ incompressible payloads typed-reject before dump; 48MiB+ raw images typed-reject on the 60MiB raw restore ceiling. |
| BUG-2 R2 miss path | CLOSED | Baseline matrix in `/Users/beam/engram-fault-baseline` at pre-fix `f24b3da` reproduced silent loss as `r2-missing-replay` from a one-row oplog (`bug2_0` missing). Fixed tree consumed the forced R2 miss during verify-before-truncate, stored the base in SQLite, and restored `bug2_0 + bug2_final === 21`. |
| BUG-3 parked continuation vs frame write | CLOSED | Baseline matrix at pre-fix `f24b3da` reproduced the parked cell clobbering an out-of-band frame write (`cell` won). Fixed tree preserved the frame write: while eval was parked at client `host.pause`, a second socket `vfs-write` committed `frame`; after hostcall resume and checkpoint, `vfs-read` returned `frame`. |

## BUG-1 ceiling

The complete fix is now a raw restore-ceiling gate, not a steady-state guess:

- `MEASURED_RESTORE_RAW_CEILING_BYTES = 60 MiB`
- `SAFE_SERIALIZE_BUFFER_BYTES = 60 MiB`
- `MAX_RESTORE_RAW_BYTES = 60 MiB`

The existing incompressible guard remains stricter for live heap content:
`INCOMPRESSIBLE_BUFFER_CEILING_BYTES = 24 MiB`. This preserves the important distinction:
normal mostly-zero sessions around ~29MiB raw remain admissible when their live/incompressible heap is
small, while genuinely large raw images are rejected before they can commit an un-restorable base.

## Ceilings (Phase 1.5)

Command:

```sh
PATH="$HOME/.cargo/bin:$PATH" node tests/chaos/fault-injection/ceilings.mjs
```

Result file:

```
scratch/beam/fault-injection-results/ceilings-run-1781503284383.json
```

Concrete cliffs:

- **D / synchronous JS recursion:** clean QuickJS `RangeError` begins at depth **1536**.
  Last passing point in the sweep was **1024**; the WebSocket stayed open for all failures.
- **D / durable snapshot-chain restore:** no cliff through **25** forced evict/restore checkpoints.
  The chain compacts at rollover: depth 15 restored 18 deltas, depth 19 restored 0 deltas after the
  new base, and depth 25 restored 9 deltas.
- **STACK / raw heap:** 8MiB and 16MiB string payloads checkpointed. 24MiB and above hit typed
  `SizeAdmissionError`; 24MiB trips the **24MiB incompressible** guard, while 48MiB+ trips the
  **60MiB raw linear-buffer** guard. 64MiB fails earlier as QuickJS `InternalError`.
- **FAN-OUT / per-cell host calls:** 1, 8, 32, 64, and 65 paused host calls completed. The 80-call
  probe emitted only **65** host-call frames and returned `0`, so local workerd's practical cliff is
  **after 65** despite the intended per-cell cap being 64.
- **ORCHESTRATION / sequential sessions:** no local cliff through **128** sequential create+eval
  sessions. Times were 1=181ms, 8=1086ms, 32=4304ms, 64=8717ms, 128=18019ms.
- **CoW / heap growth per added cell:** no `host.fork`/CoW primitive exists yet, so child sessions
  copy a full heap image: `O(children * rawImageBytes)`. In the chain sweep the first cell grew the
  raw image **19.3MiB -> 25.3MiB**, the second to **27.3MiB**, then it plateaued at **27.3MiB**
  through depth 25 for the tiny `chain++` cell.

Small curves:

| Sweep | Curve |
|---|---|
| JS recursion depth | 64 ok, 128 ok, 256 ok, 384 ok, 512 ok, 768 ok, 1024 ok, 1536 RangeError, 2048+ RangeError |
| Snapshot-chain depth | 1 ok/71ms/0 deltas, 2 ok/76ms/2 deltas, 5 ok/78ms/6, 10 ok/78ms/12, 15 ok/203ms/18, 19 ok/74ms/0, 20 ok/198ms/2, 21 ok/75ms/4, 25 ok/75ms/9 |
| Heap payload | 8MiB ok raw=26.3MiB used=8.6MiB, 16MiB ok raw=34.3MiB used=16.6MiB, 24/32MiB incompressible SizeAdmissionError, 48/56/60/61MiB raw SizeAdmissionError, 64MiB InternalError |
| Host-call fanout | 1 ok, 8 ok, 32 ok, 64 ok, 65 ok, 80 served=65 value=0 |
| Sequential sessions | 1/1 ok 181ms, 8/8 ok 1086ms, 32/32 ok 4304ms, 64/64 ok 8717ms, 128/128 ok 18019ms |

## Phase-2 readiness note

The fork-tree chaos harness (design: `~/obsidian-vault/research/fork-based-chaos-harness.md`) is gated on
a real `fork()` primitive engram does not have yet. The orchestration sweep quantifies why it matters:
without CoW / shared-immutable-parent, fan-out width amplifies heap linearly. Phase 2 = implement
fork-from-parent-heap (mid-cell) on the DO, then wrap these gated hooks into a fork-tree driver.

## Caveats

Local workerd only; no production traffic. A throwaway `*.workers.dev` faulttest deployment for the
real-R2 read-after-write timing leg remains optional follow-up — the forced-miss hook already proves the
code path locally, and the pre/post baseline matrix ran in an isolated `f24b3da` worktree.
