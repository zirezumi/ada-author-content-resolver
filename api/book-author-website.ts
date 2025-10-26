// api/book-author-website.ts
/// <reference types="node" />

/* =============================
   Vercel runtime
   ============================= */
export const config = { runtime: "nodejs" } as const;

/* =============================
   Auth
   ============================= */
const AUTH_SECRETS: string[] = (process.env.AUTHOR_UPDATES_SECRET || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const SKIP_AUTH = (process.env.SKIP_AUTH || "").toLowerCase() === "true";

function headerCI(req: any, name: string): string | undefined {
  if (!req?.headers) return undefined;
  const entries = Object.entries(req.headers) as Array<[string, string]>;
  const hit = entries.find(([k]) => k.toLowerCase() === name.toLowerCase());
  return hit?.[1];
}

function requireAuth(req: any, res: any): boolean {
  if (SKIP_AUTH) return true;
  if (!AUTH_SECRETS.length) {
    res.status(500).json({ error: "server_misconfigured: missing AUTHOR_UPDATES_SECRET" });
    return false;
  }
  const provided = (headerCI(req, "x-auth") || "").trim();
  if (!provided || !AUTH_SECRETS.includes(provided)) {
    res.status(401).json({ error: "unauthorized" });
    return false;
  }
  return true;
}

/* =============================
   Google CSE
   ============================= */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_ID = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "";
const USE_SEARCH = !!(CSE_KEY && CSE_ID);

async function fetchWithTimeout(url: string, init?: RequestInit, ms = 5500) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { "User-Agent": "BookAuthorWebsite/1.0", ...(init?.headers || {}) },
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function googleCSE(query: string, num = 10): Promise<any[]> {
  if (!USE_SEARCH) return [];
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(num));

  const resp = await fetchWithTimeout(url.toString());
  if (!resp?.ok) {
    const text = await resp?.text?.();
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  return Array.isArray(data?.items) ? data.items : [];
}

/* =============================
   Utils
   ============================= */
function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
}

/* =============================
   Heuristic "name-likeness" scoring
   ============================= */

const MIDDLE_PARTICLES = new Set([
  "de","del","de la","di","da","dos","das","do","van","von","bin","ibn","al","el","le","la","du","st","saint","mc","mac","ap"
]);

const GENERATIONAL_SUFFIX = new Set(["jr","jr.","sr","sr.","ii","iii","iv","v"]);
const BAD_TAIL = new Set([
  "frankly","review","reviews","opinion","analysis","explainer","interview","podcast",
  "video","transcript","essay","profile","biography","news","guide","column","commentary",
  "editorial","update","thread","blog"
]);

function isCasedWordToken(tok: string): boolean {
  return /^[A-Z][\p{L}’'-]*[A-Za-z]$/u.test(tok);
}
function isParticle(tok: string): boolean { return MIDDLE_PARTICLES.has(tok.toLowerCase()); }
function isSuffix(tok: string): boolean { return GENERATIONAL_SUFFIX.has(tok.toLowerCase()); }
function looksLikeAdverb(tok: string): boolean { return /^[A-Za-z]{3,}ly$/u.test(tok); }

function nameLikeness(raw: string): number {
  const s = raw.trim().replace(/\s+/g, " ");
  if (!s) return 0;

  const partsOrig = s.split(" ");
  const parts: string[] = [];
  for (let i = 0; i < partsOrig.length; i++) {
    const cur = partsOrig[i];
    const next = partsOrig[i + 1]?.toLowerCase();
    if (i + 1 < partsOrig.length && (cur.toLowerCase() === "de" && next === "la")) { parts.push("de la"); i++; continue; }
    parts.push(cur);
  }

  if (parts.length < 2) return 0;
  if (parts.length > 5) return 0.25;

  let score = 1.0;

  if (!isCasedWordToken(parts[0])) score -= 0.6;

  const last = parts[parts.length - 1];
  const lastIsSuffix = isSuffix(last);
  const lastName = lastIsSuffix ? parts[parts.length - 2] : last;
  if (!isCasedWordToken(lastName)) score -= 0.6;

  for (let i = 1; i < parts.length - (lastIsSuffix ? 2 : 1); i++) {
    const p = parts[i];
    if (!isCasedWordToken(p)) score += isParticle(p) ? -0.05 : -0.35;
  }

  if (BAD_TAIL.has(last.toLowerCase())) score -= 0.7;
  if (!lastIsSuffix && looksLikeAdverb(last)) score -= 0.6;
  if (/\d/.test(s)) score -= 0.8;
  if (/[;:!?]$/.test(s)) score -= 0.2;

  if (parts.length === 2) score += 0.10;
  if (parts.length === 3) score += 0.12;
  if (parts.length === 4) score += 0.05;

  return Math.max(0, Math.min(1, score));
}

/* =============================
   Common-pair & org-ish guards
   ============================= */

// Obvious common title words that should not be mistaken as a 2-token name
const COMMON_TITLE_WORDS = new Set([
  "Change","Transitions","Transition","Moving","Forward","Next","Day","The",
  "Future","Guide","How","Policy","Center","Office","Press","News","Support"
]);

function looksLikeCommonPair(name: string): boolean {
  const toks = name.split(/\s+/);
  if (toks.length !== 2) return false;
  return toks.every(t => COMMON_TITLE_WORDS.has(t));
}

// Recognized publisher hosts (extend as needed)
const PUBLISHERS = [
  "us.macmillan.com","macmillan.com","penguinrandomhouse.com","prh.com",
  "harpercollins.com","simonandschuster.com","simonandschuster.net",
  "macmillanlearning.com","hachettebookgroup.com","fsgbooks.com"
];

// Light heuristic for org/brand bylines like "Change Transitions"
const ABSTRACT_TOKENS = new Set([
  "Change","Transitions","News","Press","Policy","Support","Guide","Team",
  "Center","Office","School","Library","Community","Media","Communications"
]);
function maybeOrgish(name: string): boolean {
  const toks = name.split(/\s+/);
  if (toks.length !== 2) return false;
  return toks.every(t => ABSTRACT_TOKENS.has(t));
}

/** Clean + validate a candidate. Returns null if it fails the threshold. */
function normalizeNameCandidate(raw: string, title: string): string | null {
  let s = raw.trim()
    .replace(/[)\]]+$/g, "")
    .replace(/[.,;:–—-]+$/g, "")
    .replace(/\s+/g, " ");

  // Token-level: drop trailing punctuation on tokens (e.g., "Gross,")
  s = s.split(/\s+/).map(tok => tok.replace(/[.,;:]+$/g, "")).join(" ");

  // Reject if exactly equals a token in title
  if (tokens(title).includes(s.toLowerCase())) return null;

  // Early reject: obvious common-word two-token pairs like "Change Transitions"
  const partsEarly = s.split(/\s+/);
  if (partsEarly.length === 2 && looksLikeCommonPair(s)) return null;

  // Drop one trailing garbage token if present
  const parts = s.split(" ");
  const last = parts[parts.length - 1];
  if (BAD_TAIL.has(last.toLowerCase()) || looksLikeAdverb(last)) {
    if (parts.length >= 3) { parts.pop(); s = parts.join(" "); }
  }

  return nameLikeness(s) >= 0.65 ? s : null;
}

/* =============================
   Author extraction (signal-aware)
   ============================= */

type Signal = "author_label" | "wrote_book" | "byline" | "plain_last_first";

function extractNamesFromText(
  text: string,
  opts?: { booky?: boolean }
): Array<{ name: string; signal: Signal }> {
  const booky = !!opts?.booky;
  const out: Array<{ name: string; signal: Signal }> = [];

  const patterns: Array<{ re: RegExp; signal: Signal; swap?: boolean }> = [
    { re: /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "author_label" },
    { re: /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "author_label" },
    { re: /\b([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s+(?:wrote|writes)\s+the\s+book\b/gu, signal: "wrote_book" },
    { re: /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "byline" },
    { re: /\bby\s+([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "byline", swap: true },
  ];

  // Only add the plain "Last, First" rule in book-y contexts, and guard against lists:
  // Negative look-ahead prevents matching "Transitions, Change, and Moving Forward"
  if (booky) {
    patterns.push({
      re: /\b([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)\b(?!\s*(?:and|or|&|,))/gu,
      signal: "plain_last_first",
      swap: true
    });
  }

  for (const { re, signal, swap } of patterns) {
    let m: RegExpExecArray | null;
    while ((m = re.exec(text))) {
      const name = swap ? `${m[2]} ${m[1]}` : m[1];
      out.push({ name: name.trim(), signal });
    }
  }

  return out;
}

/* =============================
   Phase 1: Determine Author
   ============================= */
async function resolveAuthor(bookTitle: string): Promise<{ name: string|null, confidence: number, debug: any }> {
  // Quote the title to reduce noise
  const query = `"${bookTitle}" book written by -film -movie -screenplay -soundtrack -director`;
  const items = await googleCSE(query, 10);

  const candidates: Record<string, number> = {};
  const highSignalSeen: Record<string, number> = {};

  for (const it of items) {
    const fields: string[] = [];
    if (typeof it.title === "string") fields.push(it.title);
    if (typeof it.snippet === "string") fields.push(it.snippet);

    // Metatags + booky hint
    const mtList = Array.isArray(it.pagemap?.metatags) ? it.pagemap.metatags : [];
    let mtIsBook = false;
    for (const mt of mtList) {
      if (mt && typeof mt === "object") {
        const t1 = (mt as any).title;
        const t2 = (mt as any)["og:title"];
        const t3 = (mt as any)["book:author"];
        const ogType = (mt as any)["og:type"];
        if (typeof t1 === "string") fields.push(t1);
        if (typeof t2 === "string") fields.push(t2);
        if (typeof t3 === "string") fields.push(t3);
        if (ogType && String(ogType).toLowerCase().includes("book")) mtIsBook = true;
      }
    }

    // Host features (publisher boost)
    let host = "";
    try { host = new URL(it.link).host.toLowerCase(); } catch {}

    const haystack = fields.join(" • ");
    const matches = extractNamesFromText(haystack, { booky: mtIsBook });

    for (const { name, signal } of matches) {
      const norm = normalizeNameCandidate(name, bookTitle);
      if (!norm) continue;

      // Base weight by signal strength (downgrade plain_last_first, keep byline light)
      let w: number;
      switch (signal) {
        case "author_label":     w = 3.0; break;
        case "wrote_book":       w = 2.0; break;
        case "byline":           w = 1.0; break;
        case "plain_last_first": w = 0.6; break;
        default:                 w = 1.0;
      }

      // Reject org-ish/common-word pairs for weak signals entirely
      if ((signal === "byline" || signal === "plain_last_first") &&
          (maybeOrgish(norm) || looksLikeCommonPair(norm))) {
        continue;
      }

      // Publisher boost
      if (host && PUBLISHERS.some(d => host.endsWith(d))) w *= 1.4;

      candidates[norm] = (candidates[norm] || 0) + w;

      // Count only truly high-signal evidence (do NOT count plain_last_first)
      if (signal === "author_label" || signal === "wrote_book") {
        highSignalSeen[norm] = (highSignalSeen[norm] || 0) + 1;
      }
    }
  }

  const entries = Object.entries(candidates);
  if (entries.length === 0) {
    return { name: null, confidence: 0, debug: { query, items, candidates } };
  }

  // Cluster variants (merge prefixes/supersets)
  const clusters = new Map<string, Array<{ name: string; count: number }>>();
  for (const [name, count] of entries) {
    const toks = name.split(/\s+/);
    if (toks.length < 2) continue;
    const key = toks.slice(0, 2).join(" ").toLowerCase();
    const arr = clusters.get(key) || [];
    arr.push({ name, count });
    clusters.set(key, arr);
  }

  const merged: Array<{ name: string; score: number }> = [];
  for (const arr of clusters.values()) {
    arr.sort((a, b) => {
      const al = a.name.split(/\s+/).length, bl = b.name.split(/\s+/).length;
      if (al !== bl) return bl - al;
      if (a.count !== b.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    });

    const longest = arr[0].name;
    const longLower = longest.toLowerCase();
    let score = 0;
    for (const { name, count } of arr) {
      const nLower = name.toLowerCase();
      if (longLower.startsWith(nLower) || nLower.startsWith(longLower)) score += count;
    }
    merged.push({ name: longest, score });
  }

  if (merged.length) {
    merged.sort((a, b) => b.score - a.score);

    // Prefer a high-signal candidate unless a low-signal dominates by a large margin
    const withHigh = merged.filter(m => (highSignalSeen[m.name] || 0) > 0);
    let pick = merged[0];
    if (withHigh.length) {
      const bestHigh = withHigh[0];
      const bestOverall = merged[0];
      if ((highSignalSeen[bestOverall.name] || 0) === 0) {
        if (bestOverall.score < 1.5 * bestHigh.score) pick = bestHigh;
      }
    }

    const confidence = pick.score >= 2 ? 0.95 : 0.8;
    return { name: pick.name, confidence, debug: { query, items, candidates, merged, highSignalSeen, picked: pick.name } };
  }

  // Fallback (should rarely happen)
  const sorted = entries.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return b[0].split(/\s+/).length - a[0].split(/\s+/).length;
  });
  const [best, count] = sorted[0];
  const confidence = count >= 2 ? 0.95 : 0.8;
  return { name: best, confidence, debug: { query, items, candidates, picked: best } };
}

/* =============================
   Phase 2: Resolve Website (normalized 0–1 scoring + threshold)
   ============================= */
const BLOCKED_DOMAINS = [
  "wikipedia.org","goodreads.com","google.com","books.google.com","reddit.com",
  "amazon.","barnesandnoble.com","bookshop.org","penguinrandomhouse.com",
  "harpercollins.com","simonandschuster.com","macmillan.com","hachettebookgroup.com",
  "spotify.com","apple.com"
];

function isBlockedHost(host: string): boolean {
  return BLOCKED_DOMAINS.some((d) => host.includes(d));
}

// Normalize to 0–1
function scoreCandidateUrl(u: URL, author: string): number {
  let score = 0;

  // homepage bonus
  const pathDepth = u.pathname.split("/").filter(Boolean).length;
  if (pathDepth === 0) score += 0.5;

  // https bonus
  if (u.protocol === "https:") score += 0.1;

  // host contains author token(s)
  const toks = tokens(author).filter(t => t.length > 2);
  const host = u.host.toLowerCase();
  for (const t of toks) {
    if (host.includes(t)) score += 0.4;
  }

  return Math.min(1, score);
}

// Minimum normalized score to accept a site (default 0.6)
const WEBSITE_MIN_SCORE = Number(process.env.WEBSITE_MIN_SCORE ?? 0.6);

async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
  // Prioritize CSE for "{author} author website"
  const queries = [
    `"${author}" author website`,
    `"${author}" official site`,
    `"${bookTitle}" "${author}" author website`,
  ];

  for (const q of queries) {
    const items = await googleCSE(q, 10);
    const filtered = items.filter(it => {
      try {
        const host = new URL(it.link).host.toLowerCase();
        return !isBlockedHost(host);
      } catch { return false; }
    });

    if (!filtered.length) continue;

    const ranked = filtered
      .map(it => {
        const urlObj = new URL(it.link);
        const score = scoreCandidateUrl(urlObj, author);
        return { it, urlObj, score };
      })
      .sort((a, b) => b.score - a.score);

    const top = ranked[0];

    if (top.score >= WEBSITE_MIN_SCORE) {
      return {
        url: top.urlObj.origin,
        debug: {
          query: q,
          threshold: WEBSITE_MIN_SCORE,
          topScore: top.score,
          picked: top.it,
          candidates: ranked.slice(0, 5).map(r => ({ url: r.urlObj.href, score: r.score }))
        }
      };
    }
    // else try next query
  }

  return { url: null, debug: { tried: queries, threshold: WEBSITE_MIN_SCORE } };
}

/* =============================
   Handler
   ============================= */
export default async function handler(req: any, res: any) {
  try {
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "POST required" });
    if (!requireAuth(req, res)) return;

    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;
    const bookTitle: string = String(body.book_title || "").trim();
    if (!bookTitle) return res.status(400).json({ error: "book_title required" });

    const authorRes = await resolveAuthor(bookTitle);
    if (!authorRes.name) {
      return res.status(200).json({
        book_title: bookTitle,
        inferred_author: null,
        confidence: 0,
        error: "no_author_found",
        _diag: { flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID, WEBSITE_MIN_SCORE }, author: authorRes.debug }
      });
    }

    const siteRes = await resolveWebsite(authorRes.name, bookTitle);

    return res.status(200).json({
      book_title: bookTitle,
      inferred_author: authorRes.name,
      author_confidence: authorRes.confidence,
      author_url: siteRes.url,
      confidence: siteRes.url ? 0.9 : 0,
      _diag: {
        flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID, WEBSITE_MIN_SCORE },
        author: authorRes.debug,
        site: siteRes.debug
      }
    });
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
