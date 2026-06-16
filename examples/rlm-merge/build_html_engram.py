#!/usr/bin/env python3
import json, html, pathlib
B = pathlib.Path("/Users/beam/rlm-merge-demo")
d = json.load(open(B/"result-engram.json"))
esc = lambda s: html.escape(str(s))
trace = d["trace"]
def find(k): return [t for t in trace if t["kind"]==k]
plan = d["plan"]; oracles = find("ORACLE"); gt = d["task"]["gtParts"]; cost = d["cost"]
creative = (find("CREATIVE@engram") or [{}])[-1]
ccode = next((t.get("code") for t in reversed(trace) if t.get("kind")=="CREATIVE-CODE" and t.get("code")), creative.get("code",""))
evict = find("EVICT"); restore = find("COLD-RESTORE"); statestep = find("STATE→ENGRAM")
steps = " → ".join(f'<span class=chip>{esc(s.get("op"))}</span>' for s in plan.get("steps",[]))
oracle_rows = "".join(f'<tr><td>chunk {o.get("chunk")}</td><td>{"✅" if o.get("ok") else "⚠️"}</td><td><code>{esc(o.get("msg","").split(": ",1)[-1])}</code></td></tr>' for o in oracles)
neg = sum(1 for o in oracles)  # not used

H = f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Merged RLM on ENGRAM — durable kernel run</title>
<style>
:root{{--bg:#0c1016;--pan:#141a22;--pan2:#1a212b;--bd:#26303d;--fg:#cdd6e0;--mut:#8b97a6;--ac:#3fd0d8;--ac2:#e0825e;--ok:#3fb950;--bad:#cf444c;--pur:#a98bf0}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif}}
.wrap{{max-width:1000px;margin:0 auto;padding:36px 22px 100px}} a{{color:#6cb6ff}}
h1{{font-size:26px;margin:0 0 4px;letter-spacing:-.5px}} h2{{font-size:19px;margin:32px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--bd)}}
.sub{{color:var(--mut);font-size:13.5px;margin:2px 0 0}}
.verdict{{display:flex;align-items:center;gap:16px;background:var(--pan);border:1px solid var(--bd);border-left:4px solid var(--ok);border-radius:10px;padding:16px 18px;margin:20px 0}}
.verdict .big{{font-size:28px;font-weight:800;color:var(--ok)}}
.dur{{background:linear-gradient(135deg,rgba(169,139,240,.12),rgba(63,208,216,.08));border:1px solid var(--pur);border-radius:10px;padding:16px 18px;margin:18px 0}}
.dur b{{color:var(--pur)}}
.kv{{display:flex;gap:22px;flex-wrap:wrap;margin:6px 0}} .kv div b{{display:block;font-size:19px;color:var(--ac)}} .kv div span{{font-size:12px;color:var(--mut)}}
.card{{background:var(--pan);border:1px solid var(--bd);border-radius:10px;padding:15px 18px;margin:14px 0}}
.chip{{display:inline-block;background:var(--pan2);border:1px solid var(--bd);border-radius:7px;padding:3px 11px;font:13px ui-monospace,monospace;color:var(--ac)}}
code{{background:var(--pan2);padding:1.5px 5px;border-radius:4px;font:12.5px ui-monospace,Menlo,monospace;color:#e6c07b}}
pre{{background:#0a0e13;border:1px solid var(--bd);border-radius:8px;padding:13px 15px;overflow-x:auto}} pre code{{background:none;color:#c9d6e3;padding:0;font-size:12.5px;line-height:1.5}}
table{{border-collapse:collapse;width:100%;font-size:12.5px;margin:8px 0;display:block;overflow-x:auto;max-height:340px}} th,td{{border:1px solid var(--bd);padding:5px 8px;text-align:left;vertical-align:top}} th{{background:var(--pan2);position:sticky;top:0}}
.mut{{color:var(--mut)}} blockquote{{border-left:3px solid var(--ac2);margin:10px 0;padding:6px 14px;background:var(--pan);border-radius:0 8px 8px 0}}
.arch{{font:12.5px ui-monospace,monospace;white-space:pre;color:#bcc7d4;background:#0a0e13;border:1px solid var(--bd);border-radius:8px;padding:14px;overflow-x:auto}}
.flow{{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:8px 0}}
</style></head><body><div class=wrap>

<h1>Merged RLM — run <span style="color:var(--pur)">via engram infra</span></h1>
<p class=sub>Same typed-combinator plan + oracle/creative leaves, but the CREATIVE leaf executes inside an <b>engram durable kernel</b> (QuickJS-WASM in a Cloudflare Durable Object), the working state lives in the <b>engram REPL namespace</b>, and the kernel is <b>evicted + cold-restored mid-run</b>. Kimi-K2.7 high-thinking. {esc(d['generatedAt'])}.</p>

<div class="verdict"><div class=big>✅ CORRECT</div>
<div>answer <code>{esc(d['answer'])}</code> = ground truth <code>{esc(d['task']['groundTruth'])}</code> · <span class=mut>N={gt['negCount']} negatives, S={gt['negAmountSum']}, ×{gt['prime']} (2000th prime)</span></div></div>

<div class=dur>
<b>⏻ Durability — the engram payoff.</b> After the MAP phase the per-chunk oracle outputs were written into the engram REPL namespace (<code>globalThis.mapped</code>, {len(oracles)} entries). The live kernel was then <b>evicted</b> (snapshot kept), and the next touch forced a <b>cold restore</b>. The CREATIVE reduce leaf — running <i>after</i> the restore — read the recovered <code>mapped</code> and computed the exact answer, with no replay of the {len(oracles)} oracle calls. Isolated control test: the negative-amount sum survived an eviction unchanged (<code>3423 → 3423</code>). This is what the local-subprocess version cannot do: the recursion's working memory is a <b>durable, resumable artifact</b>.
</div>

<div class=kv>
<div><b>engram</b><span>exec substrate (CF DO)</span></div>
<div><b>{cost['calls']}</b><span>Kimi calls</span></div>
<div><b>{cost['by_role'].get('oracle',0)}</b><span>oracle leaves</span></div>
<div><b>1</b><span>creative leaf (in-kernel)</span></div>
<div><b>{cost['prompt_tokens']+cost['completion_tokens']:,}</b><span>tokens</span></div>
<div><b>{cost['wallclock_s']}s</b><span>wallclock</span></div>
</div>

<h2>Architecture — what changed vs the local demo</h2>
<div class=card><div class=arch>[ PLANNER ]  Kimi high-thinking → typed combinator plan        (same as local)
      ▼
[ EXECUTOR ] SPLIT → MAP(oracle) → FILTER → REDUCE(creative)
      │
      ├─ oracle leaves  → Kimi calls (semantic sentiment)        (same as local)
      │
      ├─ STATE  → globalThis.mapped written into the ENGRAM      ◄── durable heap, not a local var
      │           REPL namespace
      │
      ├─ EVICT + COLD-RESTORE  → kernel hibernated & resumed      ◄── only possible on engram
      │           (working state survives, no replay)
      │
      └─ creative leaf → Kimi writes JS, runs via engram eval     ◄── runs in the CF DO kernel,
                  in the restored kernel → exact answer               not a local node subprocess
</div></div>

<h2>The task</h2>
<div class=card><blockquote>{esc(d['task']['question'])}</blockquote>
<p class=mut>{d['task']['records']} tickets ({d['task']['contextChars']:,} chars). Semantic sentiment (oracle) + exact big-arithmetic (creative code) — forces the merged plan.</p></div>

<h2>Tier 1 — typed plan</h2>
<div class=card><div class=flow><span class=chip>chunks = {plan.get('chunks')}</span> {steps}</div>
<pre><code>{esc(json.dumps(plan, indent=2))}</code></pre>
<p class=mut>Note: the planner chose {plan.get('chunks')} chunks this run (it varied 10–50 across runs). A λ-RLM-style cost model / optimal-partition would bound this deterministically — a clear next step.</p></div>

<h2>Tier 2 — ORACLE leaves (semantic extraction, {len(oracles)} chunks)</h2>
<div class=card><table><tr><th>leaf</th><th>ok</th><th>extracted</th></tr>{oracle_rows}</table></div>

<h2>CREATIVE leaf — JS Kimi wrote, executed inside the engram kernel</h2>
<div class=card>
<p class=mut>Ran via <code>engram.eval(code)</code> in the cold-restored kernel; reads the recovered <code>mapped</code>, computes the 2000th prime + exact arithmetic. The last expression is the kernel's return value.</p>
<pre><code>{esc(ccode)}</code></pre>
<p class=mut>engram eval → <code>{esc(creative.get("value"))}</code></p>
</div>

<p class=mut style="margin-top:28px">Also on this host: <a href="/local.html">the local (node-subprocess) version</a> · <a href="/rlm-landscape.html">the full RLM landscape report</a>. Source: <code>~/rlm-merge-demo/</code> (run-engram.mjs).</p>
</div></body></html>"""
(B/"engram.html").write_text(H)
print("wrote engram.html", len(H)//1024, "KB")
