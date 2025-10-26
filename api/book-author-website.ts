// api/book-author-website.ts
/// <reference types="node" />

import pLimit from "p-limit";

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

function jaccard(a: string[], b: string[]) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}

/* =============================
   Heuristic "name-likeness" scoring
   ============================= */

// particles allowed in the middle and lowercased in many languages
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
  // Accept “O’Connor”, “Jean-Paul”, “Anne-Marie”, “D'Arcy”
  return /^[A-Z][\p{L}’'-]*[A-Za-z]$/u.test(tok);
}

function isParticle(tok: string): boolean {
  tok = tok.toLowerCase();
  return MIDDLE_PARTICLES.has(tok);
}

function isSuffix(tok: string): boolean {
  return GENERATIONAL_SUFFIX.has(tok.toLowerCase());
}

function looksLikeAdverb(tok: string): boolean {
  return /^[A-Za-z]{3,}ly$/u.test(tok); // “Frankly”, “Surely” …
}

function nameLikeness(raw: string): number {
  const s = raw.trim().replace(/\s+/g, " ");
  if (!s) return 0;

  const partsOrig = s.split(" ");
  // normalize multiword particles like "de la"
  const parts: string[] = [];
  for (let i = 0; i < partsOrig.length; i++) {
    const cur = partsOrig[i];
    const next = partsOrig[i + 1]?.toLowerCase();
    if (i + 1 < partsOrig.length && (cur.toLowerCase() === "de" && next === "la")) {
      parts.push("de la"); i++; continue;
    }
    parts.push(cur);
  }

  // Base constraints
  if (parts.length < 2) return 0;         // must be First+Last
  if (parts.length > 5) return 0.25;      // too many tokens

  // Scoring
  let score = 1.0;

  // Head (first token) must be a cased name token
  if (!isCasedWordToken(parts[0])) score -= 0.6;

  // Last token rules (allow suffix after the surname)
  const last = parts[parts.length - 1];
  const lastIsSuffix = isSuffix(last);
  const lastName = lastIsSuffix ? parts[parts.length - 2] : last;

  if (!isCasedWordToken(lastName)) score -= 0.6;

  // Particles allowed only between first and last name
  for (let i = 1; i < parts.length - (lastIsSuffix ? 2 : 1); i++) {
    const p = parts[i];
    if (!isCasedWordToken(p)) {
      if (isParticle(p.toLowerCase())) {
        score -= 0.05; // tiny penalty; allowed
      } else {
        score -= 0.35; // unknown lowercase or malformed token
      }
    }
  }

  // Trailing garbage penalties
  if (BAD_TAIL.has(last.toLowerCase())) score -= 0.7;
  if (!lastIsSuffix && looksLikeAdverb(last)) score -= 0.6;
  if (/\d/.test(s)) score -= 0.8;          // digits don’t belong in names
  if (/[;:!?]$/.test(s)) score -= 0.2;     // odd punctuation at end

  // Reward reasonable total length (2–4 tokens incl. particles/suffix)
  if (parts.length === 2) score += 0.10;
  if (parts.length === 3) score += 0.12;
  if (parts.length === 4) score += 0.05;

  // Cap and floor
  if (score > 1) score = 1;
  if (score < 0) score = 0;

  return score;
}

/** Clean + validate a candidate. Returns null if it fails the threshold. */
function normalizeNameCandidate(raw: string, title: string): string | null {
  // Trim obvious trailing punctuation/brackets
  let s = raw.trim()
    .replace(/[)\]]+$/g, "")
    .replace(/[.,;:–—-]+$/g, "")
    .replace(/\s+/g, " ");

  // Quick reject if the whole thing appears inside the title tokens
  if (tokens(title).includes(s.toLowerCase())) return null;

  // If trailing token is a bad tail or adverb, drop it once
  const parts = s.split(" ");
  const last = parts[parts.length - 1];
  if (BAD_TAIL.has(last.toLowerCase()) || looksLikeAdverb(last)) {
    if (parts.length >= 3) {
      parts.pop();
      s = parts.join(" ");
    }
  }

  // Score threshold: empirically, 0.65 cuts “Harari Frankly” but keeps legit names
  return nameLikeness(s) >= 0.65 ? s : null;
}

/* =============================
   Name extraction with strict right-boundary
   ============================= */
function extractNamesFromText(text: string): string[] {
  // Right boundary: stop before whitespace+punctuation OR end of string.
  const patterns = [
    /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
    /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
    /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
  ];
  const names: string[] = [];
  for (const pat of patterns) {
    let m;
    while ((m = pat.exec(text))) {
      names.push(m[1].trim());
    }
  }
  return names;
}

/* =============================
   Phase 1: Determine Author (heuristic + clustering)
   ============================= */
async function resolveAuthor(bookTitle: string): Promise<{ name: string|null, confidence: number, debug: any }> {
  const query = `"${bookTitle}" book written by -film -movie -screenplay -soundtrack -director`;
  const items = await googleCSE(query, 10);

  const candidates: Record<string, number> = {};
  for (const it of items) {
    const text = `${it.title} ${it.snippet || ""}`;
    for (const n of extractNamesFromText(text)) {
      const norm = normalizeNameCandidate(n, bookTitle);
      if (norm) {
        candidates[norm] = (candidates[norm] || 0) + 1;
      }
    }
  }

  const entries = Object.entries(candidates);
  if (entries.length === 0) {
    return { name: null, confidence: 0, debug: { query, items, candidates } };
  }

  // Build clusters keyed by the first two tokens (First + Middle/Last) to merge truncations.
  const clusters = new Map<string, Array<{ name: string; count: number }>>();
  for (const [name, count] of entries) {
    const toks = name.split(/\s+/);
    if (toks.length < 2) continue; // need First+Last
    const key = toks.slice(0, 2).join(" ").toLowerCase();
    const arr = clusters.get(key) || [];
    arr.push({ name, count });
    clusters.set(key, arr);
  }

  const merged: Array<{ name: string; score: number }> = [];
  for (const arr of clusters.values()) {
    // Prefer longer names; break ties by higher raw count, then lexicographically
    arr.sort((a, b) => {
      const al = a.name.split(/\s+/).length, bl = b.name.split(/\s+/).length;
      if (al !== bl) return bl - al;
      if (a.count !== b.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    });

    const longest = arr[0].name;
    const longLower = longest.toLowerCase();
    let score = 0;

    // Merge counts for prefix/superset variants within cluster
    for (const { name, count } of arr) {
      const nLower = name.toLowerCase();
      if (longLower.startsWith(nLower) || nLower.startsWith(longLower)) score += count;
    }
    merged.push({ name: longest, score });
  }

  if (merged.length) {
    merged.sort((a, b) => b.score - a.score);
    const best = merged[0].name;
    const confidence = merged[0].score >= 2 ? 0.95 : 0.8;
    return { name: best, confidence, debug: { query, items, candidates, merged, picked: best } };
  }

  // Fallback: prefer higher count then longer name
  const sorted = entries.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return b[0].split(/\s+/).length - a[0].split(/\s+/).length;
  });
  const [best, count] = sorted[0];
  const confidence = count >= 2 ? 0.95 : 0.8;
  return { name: best, confidence, debug: { query, items, candidates, picked: best } };
}


/* =============================
   Phase 2: Resolve Website  (PRIORITIZE "{author} author website")
   ============================= */
const BLOCKED_DOMAINS = [
  "wikipedia.org","goodreads.com","google.com","books.google.com","reddit.com",
  "amazon.","barnesandnoble.com","bookshop.org","penguinrandomhouse.com",
  "harpercollins.com","simonandschuster.com","macmillan.com","hachettebookgroup.com"
];

function isBlockedHost(host: string): boolean {
  return BLOCKED_DOMAINS.some((d) => host.includes(d));
}

function scoreCandidateUrl(u: URL, author: string): number {
  // Higher is better
  let score = 0;
  const pathDepth = u.pathname.split("/").filter(Boolean).length;
  if (pathDepth === 0) score += 2;        // homepage
  if (u.protocol === "https:") score += 0.25;

  // Bonus if hostname contains author tokens (e.g., johndoe.com, harari.org)
  const toks = tokens(author).filter(t => t.length > 2);
  const host = u.host.toLowerCase();
  for (const t of toks) {
    if (host.includes(t)) score += 0.4;
  }
  return score;
}

async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
  // New priority order:
  //  1) "{author} author website"
  //  2) "{author} official site"
  //  3) "{bookTitle}" "{author}" author website  (fallback)
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

    if (filtered.length) {
      // Pick best candidate by a simple score
      const ranked = filtered
        .map(it => ({ it, urlObj: new URL(it.link) }))
        .sort((a, b) => scoreCandidateUrl(b.urlObj, author) - scoreCandidateUrl(a.urlObj, author));

      const top = ranked[0];
      return {
        url: top.urlObj.origin,
        debug: { query: q, picked: top.it, items: filtered.map(i => i.link) }
      };
    }
  }
  return { url: null, debug: { tried: queries } };
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
        _diag: authorRes.debug
      });
    }

    const siteRes = await resolveWebsite(authorRes.name, bookTitle);

    const payload = {
      book_title: bookTitle,
      inferred_author: authorRes.name,
      author_confidence: authorRes.confidence,
      author_url: siteRes.url,
      confidence: siteRes.url ? 0.9 : 0,
      _diag: { author: authorRes.debug, site: siteRes.debug }
    };

    return res.status(200).json(payload);
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
