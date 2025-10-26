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

  // Drop one trailing garbage token if present
  const parts = s.split(" ");
  const last = parts[parts.length - 1];
  if (BAD_TAIL.has(last.toLowerCase()) || looksLikeAdverb(last)) {
    if (parts.length >= 3) { parts.pop(); s = parts.join(" "); }
  }

  return nameLikeness(s) >= 0.65 ? s : null;
}

/* =============================
   Name extraction (expanded patterns)
   ============================= */
function extractNamesFromText(text: string, opts?: { booky?: boolean }): string[] {
  const booky = !!opts?.booky;
  const patterns = [
    /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
    /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
    /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
    // "<Name> wrote the book"
    /\b([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s+(?:wrote|writes)\s+the\s+book\b/gu,
    // "by Last, First"
    /\bby\s+([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)(?=\s*(?:[),.?:;!–—-]\s|$))/gu,
    // PLAIN "Last, First" (only when page looks like a book page)
    ...(booky ? [/\b([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)\b/gu] : []),
  ];

  const names: string[] = [];
  for (const pat of patterns) {
    let m: RegExpExecArray | null;
    while ((m = pat.exec(text))) {
      const candidate = m.length === 3 ? `${m[2]} ${m[1]}` : m[1];
      names.push(candidate.trim());
    }
  }
  return names;
}

/* =============================
   Phase 1: Determine Author
   ============================= */
async function resolveAuthor(bookTitle: string): Promise<{ name: string|null, confidence: number, debug: any }> {
  const query = `"${bookTitle}" book written by -film -movie -screenplay -soundtrack -director`;
  const items = await googleCSE(query, 10);

  const candidates: Record<string, number> = {};

  for (const it of items) {
    const fields: string[] = [];
    if (typeof it.title === "string") fields.push(it.title);
    if (typeof it.snippet === "string") fields.push(it.snippet);

    // Collect metatag hints and whether the page looks "booky"
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

    const haystack = fields.join(" • ");
    for (const n of extractNamesFromText(haystack, { booky: mtIsBook })) {
      const norm = normalizeNameCandidate(n, bookTitle);
      if (norm) candidates[norm] = (candidates[norm] || 0) + 1;
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
    const best = merged[0].name;
    const confidence = merged[0].score >= 2 ? 0.95 : 0.8;
    return { name: best, confidence, debug: { query, items, candidates, merged, picked: best } };
  }

  const sorted = entries.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return b[0].split(/\s+/).length - a[0].split(/\s+/).length;
  });
  const [best, count] = sorted[0];
  const confidence = count >= 2 ? 0.95 : 0.8;
  return { name: best, confidence, debug: { query, items, candidates, picked: best } };
}

/* =============================
   Phase 2: Resolve Website (author-first)
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

function scoreCandidateUrl(u: URL, author: string): number {
  let score = 0;
  const pathDepth = u.pathname.split("/").filter(Boolean).length;
  if (pathDepth === 0) score += 2;       // homepage
  if (u.protocol === "https:") score += 0.25;
  const toks = tokens(author).filter(t => t.length > 2);
  const host = u.host.toLowerCase();
  for (const t of toks) if (host.includes(t)) score += 0.4;
  return score;
}

async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
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
      const ranked = filtered
        .map(it => ({ it, urlObj: new URL(it.link) }))
        .sort((a, b) => scoreCandidateUrl(b.urlObj, author) - scoreCandidateUrl(a.urlObj, author));

      const top = ranked[0];
      return { url: top.urlObj.origin, debug: { query: q, picked: top.it, items: filtered.map(i => i.link) } };
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
        _diag: { flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID }, author: authorRes.debug }
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
        flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID },
        author: authorRes.debug,
        site: siteRes.debug
      }
    });
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
