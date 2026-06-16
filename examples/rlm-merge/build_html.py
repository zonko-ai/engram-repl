#!/usr/bin/env python3
import json, html, pathlib
B = pathlib.Path("/Users/beam/rlm-merge-demo")
d = json.load(open(B/"result.json"))
esc = lambda s: html.escape(str(s))

trace = d["trace"]
def find(kind): return [t for t in trace if t["kind"]==kind]
plan = d["plan"]
oracles = find("ORACLE")
creative = find("CREATIVE")[-1] if find("CREATIVE") else {}
cost = d["cost"]; gt = d["task"]["gtParts"]

# plan steps as pipeline chips
steps = " → ".join(f'<span class=chip>{esc(s.get("op"))}</span>' for s in plan.get("steps",[]))

oracle_rows = ""
for o in oracles:
    cidx = o.get("chunk","?")
    body = esc(o.get("msg","").split(": ",1)[-1])
    ok = o.get("ok")
    oracle_rows += f'<tr><td>chunk {cidx}</td><td>{"✅" if ok else "⚠️"}</td><td><code>{body}</code></td></tr>'

cc = creative.get("code","")
cstdout = creative.get("stdout","")

H = f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Merged RLM demo — λ-combinators × dynamic workflow × Kimi-K2.7</title>
<style>
:root{{--bg:#0c1016;--pan:#141a22;--pan2:#1a212b;--bd:#26303d;--fg:#cdd6e0;--mut:#8b97a6;--ac:#3fd0d8;--ac2:#e0825e;--ok:#3fb950;--bad:#cf444c}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif}}
.wrap{{max-width:1000px;margin:0 auto;padding:36px 22px 100px}}
a{{color:#6cb6ff}} h1{{font-size:27px;margin:0 0 4px;letter-spacing:-.5px}} h2{{font-size:19px;margin:34px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--bd)}}
.sub{{color:var(--mut);font-size:13.5px;margin:2px 0 0}}
.verdict{{display:flex;align-items:center;gap:16px;background:var(--pan);border:1px solid var(--bd);border-left:4px solid var(--ok);border-radius:10px;padding:16px 18px;margin:20px 0}}
.verdict .big{{font-size:30px;font-weight:800;color:var(--ok)}}
.verdict.bad{{border-left-color:var(--bad)}} .verdict.bad .big{{color:var(--bad)}}
.kv{{display:flex;gap:24px;flex-wrap:wrap;margin:6px 0}} .kv div b{{display:block;font-size:20px;color:var(--ac)}} .kv div span{{font-size:12px;color:var(--mut)}}
.card{{background:var(--pan);border:1px solid var(--bd);border-radius:10px;padding:15px 18px;margin:14px 0}}
.chip{{display:inline-block;background:var(--pan2);border:1px solid var(--bd);border-radius:7px;padding:3px 11px;font:13px ui-monospace,monospace;color:var(--ac)}}
.flow{{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:8px 0}}
code{{background:var(--pan2);padding:1.5px 5px;border-radius:4px;font:12.5px ui-monospace,Menlo,monospace;color:#e6c07b}}
pre{{background:#0a0e13;border:1px solid var(--bd);border-radius:8px;padding:13px 15px;overflow-x:auto}} pre code{{background:none;color:#c9d6e3;padding:0;font-size:12.5px;line-height:1.5}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin:8px 0;display:block;overflow-x:auto}} th,td{{border:1px solid var(--bd);padding:6px 9px;text-align:left;vertical-align:top}} th{{background:var(--pan2);color:#e8eef5}}
.lab{{color:var(--ac2);font-weight:700}} .mut{{color:var(--mut)}} blockquote{{border-left:3px solid var(--ac2);margin:10px 0;padding:6px 14px;background:var(--pan);border-radius:0 8px 8px 0}}
.arch{{font:12.5px ui-monospace,monospace;white-space:pre;color:#bcc7d4;background:#0a0e13;border:1px solid var(--bd);border-radius:8px;padding:14px;overflow-x:auto}}
</style></head><body><div class=wrap>

<h1>Merged RLM — typed λ-combinators × dynamic-workflow execution × creative leaves</h1>
<p class=sub>A standalone local demo (no engram). Kimi-K2.7-Code via Cloudflare Workers AI, reasoning effort = <b>high</b>. Generated {esc(d['generatedAt'])}.</p>

<div class="verdict {'' if d['correct'] else 'bad'}">
<div class=big>{'✅ CORRECT' if d['correct'] else '❌ WRONG'}</div>
<div>final answer <code>{esc(d['answer'])}</code> · ground truth <code>{esc(d['task']['groundTruth'])}</code><br>
<span class=mut>independently-computed GT: N={gt['negCount']} negative tickets, S={gt['negAmountSum']} sum, ×{gt['prime']} (2000th prime) = {gt['answer']}</span></div>
</div>

<div class=kv>
<div><b>{cost['calls']}</b><span>Kimi calls</span></div>
<div><b>{cost['by_role'].get('oracle',0)}</b><span>oracle leaves</span></div>
<div><b>{cost['by_role'].get('creative',0)}</b><span>creative leaves</span></div>
<div><b>{cost['prompt_tokens']+cost['completion_tokens']:,}</b><span>tokens</span></div>
<div><b>{cost['reasoning_chars']:,}</b><span>reasoning chars (thinking)</span></div>
<div><b>{cost['wallclock_s']}s</b><span>wallclock</span></div>
</div>

<h2>The architecture (what this demo realizes)</h2>
<div class=card><div class=arch>NL question + context SHAPE
      │
      ▼
[ PLANNER ]  Kimi high-thinking → a TYPED COMBINATOR PLAN          ← λ-RLM: structure up front,
      │       (not free-form code; verified op vocabulary)             not model-written control code
      ▼
[ EXECUTOR ] deterministic, state in local variables              ← Dynamic-Workflow: out-of-context,
      │       SPLIT → MAP(leaf) → FILTER → REDUCE(leaf)                 the plan moves to code
      ▼
   leaves:
   ├─ ORACLE  → bounded Kimi call, SEMANTIC judgement (sentiment)  ← used where language understanding is needed
   └─ CREATIVE→ Kimi WRITES JS, run in a local node sandbox        ← the free-form escape hatch, EXACT compute
</div>
<p class=mut style=margin-bottom:0>The merge in one line: a <b>verified combinator skeleton</b> decides the shape; each leaf is a typed slot that resolves to a <b>bounded oracle</b> (cheap, semantic) or a <b>creative code subagent</b> (exact, deterministic) — structure where you want guarantees, freedom where you want creativity.</p></div>

<h2>The task (real, verifiable)</h2>
<div class=card>
<blockquote>{esc(d['task']['question'])}</blockquote>
<p class=mut>Context: {d['task']['records']} support tickets ({d['task']['contextChars']:,} chars) — too long to trust a single flat call, and notes are free-text (needs semantic sentiment) while the final value needs exact big arithmetic (needs code). That combination is what forces the merged plan.</p>
</div>

<h2>Tier 1 — the typed plan the planner emitted</h2>
<div class=card>
<div class=flow><span class=lab>chunks = {plan.get('chunks')}</span> &nbsp; {steps}</div>
<pre><code>{esc(json.dumps(plan, indent=2))}</code></pre>
</div>

<h2>Tier 2 — ORACLE leaves (semantic extraction, per chunk)</h2>
<div class=card>
<p class=mut>Each chunk → a bounded Kimi call that judges note sentiment and returns strict JSON for the scoped ids. The model does the language understanding; it is forbidden from computing the answer.</p>
<table><tr><th>leaf</th><th>ok</th><th>extracted</th></tr>{oracle_rows}</table>
</div>

<h2>The CREATIVE leaf — code Kimi wrote for the exact compute</h2>
<div class=card>
<p class=mut>The REDUCE step is a creative leaf: Kimi wrote JavaScript (run in a local node sandbox) to flatten the oracle outputs, count negatives (N), sum amounts (S), compute the 2000th prime itself, and return N + S×prime. This is the part a bounded oracle gets wrong — in an earlier run the oracle tried to do the arithmetic in-head and produced garbage; moving it to code fixes it.</p>
<pre><code>{esc(cc)}</code></pre>
<p class=mut>stdout → <code>{esc(cstdout)}</code></p>
</div>

<h2>Why this is the synthesis of everything</h2>
<div class=card><ul>
<li><b>λ-RLM</b> gives the verified structure + cost/termination bounds, but loses on creative/coding leaves — here those leaves are an <b>escape hatch to free-form code</b>.</li>
<li><b>Claude-Code Dynamic Workflows</b> give out-of-context execution with state in variables + a swarm, but no guarantees — here the swarm shape is <b>fixed by a typed plan</b>.</li>
<li>Our own Kimi experiment showed over-structured prompts thrash on off-task — here structure-vs-freedom is a <b>per-leaf typed choice</b>, not a global prompt.</li>
<li>Next: make the executor's state durable + forkable (the engram thesis) so a deep plan can hibernate mid-recursion and fan out without re-sending context.</li>
</ul></div>

<p class=mut style="margin-top:30px">Also on this host: <a href="/rlm-landscape.html">the full RLM landscape report</a> (papers · methods · benchmarks · repos). · Source: <code>~/rlm-merge-demo/</code> (kimi.mjs, task.mjs, run.mjs).</p>
</div></body></html>"""
(B/"index.html").write_text(H)
print("wrote", B/"index.html", len(H)//1024, "KB")
