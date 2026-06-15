## 2026-06-15T05:11:34.282Z
Started fault-injection verification task. Read brief; next mapping repo build/test paths and existing local workerd/miniflare harness.

## 2026-06-15 10:42:00 IST
Mapped repo scripts. Active kernel is apps/kernel; normal prod wrangler.jsonc currently has no fault-test var. Inspecting restore/checkpoint/R2/VFS callsites before adding gated hooks.

## 2026-06-15 10:46:45 IST
Implemented gated kernel fault hooks: env+config faultTest guard, one-shot R2 miss injector, gated checkpoint telemetry, and a test-only checkpoint-evict op. Added separate apps/kernel/wrangler.faulttest.jsonc and tests/chaos/fault-injection/run.mjs harness. Next: build/typecheck and run local fault harness.

## 2026-06-15 10:47:37 IST
Verification: build:worker PASS using rustup cargo in PATH. Homebrew cargo lacked wasm32-wasip1 std; rustup target was installed and PATH fixed. Starting local Wrangler fault-test harness next.

## 2026-06-15 10:50:50 IST
Local fault harness PASS on fixed tree. Results file: scratch/beam/fault-injection-results/fault-run-1781500839217.json. Evidence: prod fault flag absent; telemetry echoed bufferBytes=20185088 scratchLen=1048576 usedHeap=662075; BUG-1 sweep admits 16MiB payload / 35.9MB raw and rejects 24MiB+ incompressible plus 60MiB raw cap; BUG-2 no silent loss on cold restore (local path remained sqlite); BUG-3 parked hostcall + out-of-band vfs-write preserved frame body. Next: docs, full checks, commit.

## 2026-06-15 10:52:18 IST
Final verification PASS: local chaos harness PASS; bun run check PASS; bun run --filter engram-rust1b build:worker PASS using PATH=$HOME/.cargo/bin:$PATH. Preparing local commit; no push.


## 2026-06-15T05:41:43.034Z
Continuing verification from a3a541c. Fixed-tree harness exists, but BUG-2 local run did not force an R2 base. Next: create isolated f24b3da worktree, transplant gated hooks/harness for baseline-only testing, extend current harness for forced R2 and Phase 1.5 ceiling sweeps, then update verdict and progress.

## 2026-06-15T05:45:13.938Z
Prepared isolated baseline worktree at /Users/beam/engram-fault-baseline with fault hooks plus pre-fix BUG-1/2/3 behavior restored. Updated main harness for --expect baseline/fixed and added Phase 1.5 ceiling runner. Starting baseline build.

## 2026-06-15T05:49:58.317Z
Baseline matrix PASS on isolated f24b3da worktree with fault hooks: BUG-2 reproduced as r2-missing-replay from one-row oplog (bug2_0 missing) and BUG-3 reproduced as parked cell clobbering frame write. Fixed matrix PASS: BUG-2 verification consumed forced R2 miss and stored base in SQLite; BUG-3 preserved frame body. Next: throwaway Cloudflare faulttest deploy and Phase 1.5 ceilings.
Phase 1.5 ceiling sweeps complete on local workerd fault-test kernel. Result: scratch/beam/fault-injection-results/ceilings-run-1781503284383.json. Cliffs: JS recursion RangeError at 1536 after 1024 ok; snapshot chain ok through depth 25; heap gate rejects from 24MiB incompressible / 60MiB raw; host-call fanout practical cliff after 65; sequential sessions ok through 128. Verdict doc updated; ready to commit.
