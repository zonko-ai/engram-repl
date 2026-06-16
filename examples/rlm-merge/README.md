# RLM merge demo — typed combinator planner × creative codegen leaf, on engram

A runnable example of a **two-tier control plane** that merges three ideas:

- **λ-RLM** (typed combinator planner): a planner sees only the *shape* of the data and emits
  a typed plan (`SPLIT → MAP → FILTER → REDUCE`) — structure decided up front, not model-written
  control code.
- **Dynamic-Workflows** (out-of-context execution): the plan runs deterministically with
  intermediate state held outside the model context.
- **engram** (durable, forkable heap): the **creative leaf's JS executes inside the engram
  durable kernel** (QuickJS-WASM in a Cloudflare DO) via `session.eval`, and the recursion's
  working state lives in the engram REPL namespace — so it survives kernel hibernation.

Each leaf is a typed slot that resolves to **either** a bounded ORACLE call (cheap, provable —
semantic sentiment judgement here) **or** a CREATIVE leaf that writes and runs real JavaScript
(exact arithmetic / large numbers). The boundary — structure where you want guarantees,
free-form code where you want creativity/exactness — is chosen *per leaf*, not as a global prompt.

The task is a synthetic long-context aggregation: judge per-ticket sentiment, then compute
`N + S * P` where `N`/`S` are over negative tickets and `P` is the 2000th prime. Ground truth is
computed independently, so the run self-verifies (**59,522,552**).

## Files

| File | What it is |
|---|---|
| `run-engram.mjs` | **The engram version.** Creative leaf runs in an engram session; mid-run the kernel is **evicted then cold-restored** to prove the MAP state survives hibernation with no oracle replay. |
| `run.mjs` | Standalone baseline — same plan, but the creative leaf runs in a local node sandbox (no engram). Useful as the local-vs-engram contrast. |
| `kimi.mjs` | Minimal Kimi-K2.7 (`@cf/moonshotai/kimi-k2.7-code`, high-thinking) client over Cloudflare Workers AI. |
| `task.mjs` | Builds the synthetic task + independent ground truth. |
| `build_html*.py` | Render the run trace (`result*.json`) into a static report page. |

## The engram differentiator (what `run-engram.mjs` proves)

1. **In-run:** after MAP, `mapped[]` is stored in the engram namespace → the live kernel is
   **evicted** → **cold-restored** → the creative REDUCE reads the recovered state and computes
   the exact answer, with **no replay of the oracle calls**.
2. The recursion's working memory is a **durable, resumable artifact** — you can hibernate a node
   mid-tree and wake it later. A local subprocess (`run.mjs`) structurally cannot do this.

## Run

```bash
# creds: CLOUDFLARE_ACCOUNT_ID + CLOUDFLARE_API_TOKEN (Workers AI),
# and ENGRAM_KERNEL_KEY for the engram session — picked up from env or repo-root .env.

# engram version (durable creative leaf + evict/cold-restore):
ENGRAM_ENDPOINT=wss://engram.<your-acct>.workers.dev node examples/rlm-merge/run-engram.mjs

# local baseline (no engram):
node examples/rlm-merge/run.mjs
```

Run outputs (`result*.json`, `*.log`, generated `*.html`) are git-ignored — regenerate by running.
