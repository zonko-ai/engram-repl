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
- Test-only `checkpoint-evict` control frame for parked-eval experiments.
- Local harness: `tests/chaos/fault-injection/run.mjs`.

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
| BUG-2 R2 miss path | PARTIAL LOCAL CLOSE | Local safe payload stayed in SQLite, so no real R2 overflow miss was fired. The one-shot miss hook is wired in `r2_get_resilient`; local cold restore preserved state and did not silently resurrect a truncated namespace. |
| BUG-3 parked continuation vs frame write | CLOSED | While eval was parked at client `host.pause`, a second socket `vfs-write` committed `frame`; after hostcall resume and checkpoint, `vfs-read` returned `frame`, not the staged cell write. |

## BUG-1 ceiling

The complete fix is now a raw restore-ceiling gate, not a steady-state guess:

- `MEASURED_RESTORE_RAW_CEILING_BYTES = 60 MiB`
- `SAFE_SERIALIZE_BUFFER_BYTES = 60 MiB`
- `MAX_RESTORE_RAW_BYTES = 60 MiB`

The existing incompressible guard remains stricter for live heap content:
`INCOMPRESSIBLE_BUFFER_CEILING_BYTES = 24 MiB`. This preserves the important distinction:
normal mostly-zero sessions around ~29MiB raw remain admissible when their live/incompressible heap is
small, while genuinely large raw images are rejected before they can commit an un-restorable base.

## Caveats

The requested pre-fix baseline matrix was not run in this pass. The fault hooks did not exist in the
pre-fix tree, and no Cloudflare throwaway deployment was used. The harness is structured so a
backported-hook baseline or a throwaway `engram-kernel-faulttest` deployment can run the same cases
without touching prod.
