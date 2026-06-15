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

## Ceilings (Phase 1.5) — local workerd sweeps

Measured on the local fault-test kernel (`tests/chaos/fault-injection/ceilings.mjs`):

- **JS call-stack depth** — synchronous recursion OK through **depth 1024**; **RangeError at 1536+**. Safe synchronous-recursion budget ≈ 1024 frames; cliff between 1024 and 1536.
- **Snapshot / recursion-tree chain depth** — durable chain restores cleanly from depth 1 upward via `sqlite-restore`; restore cost grows with chain length (gunzip + page-grow per link). Limit is cost, not correctness.
- **Heap per node** — 8MiB & 16MiB raw checkpoint fine (mostly-zero, ~477KB gz, SQLite store). BUG-1 gate rejects ≥24MiB *incompressible* / ≥60MiB raw before committing an un-restorable base.
- **Host-call fan-out per cell** — served cleanly to **65**; at 80 requested only **65 served** (rest truncated) — confirms the **64 host-call/cell cap**.
- **Sequential session density** — 1→64 linear (~136ms/session); **128 sessions all OK** at ~18.0s (~141ms/session). No correctness cliff; throughput-bound.
- **Static orchestration envelope** — **128 facets/shard × 64 shards = 8192 theoretical facet slots**; **CoW amplification = O(children × rawImageBytes)** because the substrate has **no fork primitive yet** (children copy the full heap image) — exactly the Phase-2 fork-from-parent-heap target.

Result file: `scratch/beam/fault-injection-results/ceilings-run-1781503284383.json`.

## Phase-2 readiness note

The fork-tree chaos harness (design: `~/obsidian-vault/research/fork-based-chaos-harness.md`) is gated on
a real `fork()` primitive engram does not have yet. The orchestration sweep quantifies why it matters:
without CoW / shared-immutable-parent, fan-out width amplifies heap linearly. Phase 2 = implement
fork-from-parent-heap (mid-cell) on the DO, then wrap these gated hooks into a fork-tree driver.

## Caveats

Local workerd only; no production traffic. A throwaway `*.workers.dev` faulttest deployment for the
real-R2 read-after-write timing leg remains optional follow-up — the forced-miss hook already proves the
code path locally, and the pre/post baseline matrix ran in an isolated `f24b3da` worktree.
