// Minimal Kimi-K2.7 client over Cloudflare Workers AI, with high-thinking.
// Standalone — no engram. Reads CF creds (CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN)
// from the environment, falling back to the repo-root .env (two levels up).
import fs from "node:fs";
const ENV = { ...process.env };
for (const f of [new URL("../../.env", import.meta.url), new URL("../../.dev.vars", import.meta.url)]) {
  try { for (const l of fs.readFileSync(f, "utf8").split("\n")) { const m = l.match(/^([A-Z0-9_]+)=(.*)$/); if (m && ENV[m[1]] == null) ENV[m[1]] = m[2].replace(/^["']|["']$/g, ""); } } catch {}
}
export const ACCT = ENV.CLOUDFLARE_ACCOUNT_ID, CFTOK = ENV.CLOUDFLARE_API_TOKEN;
export const MODEL = "@cf/moonshotai/kimi-k2.7-code";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const COST = { calls: 0, prompt_tokens: 0, completion_tokens: 0, reasoning_chars: 0, by_role: {} };

export async function kimi(messages, { tools, effort = "high", maxTokens = 4096, role = "misc" } = {}) {
  const body = { messages, max_tokens: maxTokens, reasoning: { effort } };
  if (tools) body.tools = tools;
  let lastErr;
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${ACCT}/ai/run/${MODEL}`, {
        method: "POST", headers: { Authorization: `Bearer ${CFTOK}`, "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      const j = await res.json();
      if (!j.success && j.errors) { lastErr = JSON.stringify(j.errors); if (/3040|capacity|429|rate|503|overload/i.test(lastErr)) { await sleep(Math.min(30000, 3000 * attempt)); continue; } throw new Error(lastErr); }
      const r = j.result || {};
      const ch = (r.choices && r.choices[0]) || {};
      const msg = ch.message || { content: r.response || "" };
      const u = r.usage || {};
      COST.calls++; COST.prompt_tokens += u.prompt_tokens || 0; COST.completion_tokens += u.completion_tokens || 0;
      COST.reasoning_chars += (msg.reasoning_content || "").length;
      COST.by_role[role] = (COST.by_role[role] || 0) + 1;
      // reasoning models can spend the whole budget on reasoning -> empty content (finish=length).
      // one auto-retry with a much larger budget before giving up.
      if (!(msg.content || "").trim() && ch.finish_reason === "length" && body.max_tokens < 24000) {
        body.max_tokens = Math.min(24000, body.max_tokens * 2); await sleep(500); continue;
      }
      return { content: msg.content || "", tool_calls: msg.tool_calls || null, reasoning: msg.reasoning_content || "", finish: ch.finish_reason };
    } catch (e) { lastErr = e.message; await sleep(Math.min(20000, 2000 * attempt)); }
  }
  throw new Error("kimi failed: " + lastErr);
}

// pull first JSON object/array out of a model reply (handles ```json fences + prose)
export function extractJSON(text) {
  let t = String(text).trim();
  const fence = t.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) t = fence[1].trim();
  const s = t.search(/[\[{]/); if (s < 0) throw new Error("no JSON in reply: " + t.slice(0, 120));
  // balance-scan from first bracket
  const open = t[s], close = open === "{" ? "}" : "]"; let d = 0, end = -1, inStr = false, esc = false;
  for (let i = s; i < t.length; i++) { const c = t[i]; if (inStr) { if (esc) esc = false; else if (c === "\\") esc = true; else if (c === '"') inStr = false; } else { if (c === '"') inStr = true; else if (c === open) d++; else if (c === close) { d--; if (d === 0) { end = i; break; } } } }
  if (end < 0) throw new Error("unbalanced JSON");
  return JSON.parse(t.slice(s, end + 1));
}
