// Real task with deterministic ground truth. Designed to FORCE the merged architecture:
//  - long context (500 records) -> must SPLIT (a single flat call rots / can't be trusted)
//  - a SEMANTIC sub-question (sentiment of a free-text note) -> ORACLE leaf (real NL judgement)
//  - an EXACT-COMPUTE sub-question (big arithmetic) -> CREATIVE code leaf (oracle would fumble)
//  - aggregation over a given id subset -> FILTER + REDUCE
// Ground truth is computed here independently of the model.

const NEG = [
  "this is completely unacceptable and I want a refund",
  "worst experience ever, nothing works",
  "I've been charged twice and nobody will help me",
  "extremely frustrated, the product broke on day one",
  "I am furious about the constant outages",
  "terrible support, I waited three hours and got nothing",
];
const POS = [
  "everything works great, very happy with the service",
  "the team resolved my issue quickly, excellent support",
  "love the new feature, smooth and reliable",
  "thanks, the upgrade went perfectly",
  "really satisfied with how fast this was handled",
  "no complaints at all, works as expected",
];

function rng(seed) { let x = seed >>> 0; return () => (x = (x * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff; }

export function buildTask() {
  const r = rng(20260615);
  const records = [];
  for (let i = 1; i <= 500; i++) {
    const neg = r() < 0.42;                       // hidden ground-truth sentiment
    const pool = neg ? NEG : POS;
    const note = pool[Math.floor(r() * pool.length)];
    const amount = 50 + Math.floor(r() * 950);    // 50..999
    records.push({ id: i, amount, note, __neg: neg });   // __neg is GT only (stripped from model view)
  }
  // a 20-id subset the question scopes to
  const subset = [];
  const r2 = rng(7);
  while (subset.length < 20) { const id = 1 + Math.floor(r2() * 500); if (!subset.includes(id)) subset.push(id); }

  // nth prime (exact-compute ingredient)
  const nthPrime = (n) => { let c = 0, k = 1; while (c < n) { k++; let p = true; for (let d = 2; d * d <= k; d++) if (k % d === 0) { p = false; break; } if (p) c++; } return k; };
  const PRIME_N = 2000;
  const prime = nthPrime(PRIME_N);

  // ground truth
  const inSub = records.filter((x) => subset.includes(x.id));
  const negCount = inSub.filter((x) => x.__neg).length;
  const negAmountSum = inSub.filter((x) => x.__neg).reduce((a, x) => a + x.amount, 0);
  const answer = negCount * 1 + negAmountSum * prime;   // composite final

  // the context the MODEL sees (note: __neg stripped)
  const lines = records.map((x) => `#${x.id} | amount=${x.amount} | note="${x.note}"`);
  const context = "Support tickets, one per line (id, amount, free-text note):\n" + lines.join("\n");

  const question =
    `Scope to ONLY the tickets whose id is in this set: [${subset.join(", ")}].\n` +
    `Among those 20 tickets, let N = how many have a NEGATIVE-sentiment note, and ` +
    `let S = the sum of 'amount' over those negative tickets.\n` +
    `Compute the final value: N + S * (the ${PRIME_N}th prime number). Return ONLY the integer.`;

  return {
    context, question, subset,
    shape: `500 lines, each "#id | amount=INT | note=\\"free text\\"". Question scopes to a 20-id subset, needs sentiment judgement on notes (semantic) + exact arithmetic with the ${PRIME_N}th prime.`,
    groundTruth: String(answer),
    gtParts: { negCount, negAmountSum, prime, answer },
    records,
  };
}
