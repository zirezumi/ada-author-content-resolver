// api/book-author-website.ts
/// <reference types="node" />

/**
 * POST Body:
 * {
 *   "book_title": "Educated",
 *   "include_search": true,                 // default true (requires GOOGLE_CSE_* env)
 *   "min_author_confidence": 0.55,          // optional 0..1
 *   "min_site_confidence": 0.65,            // optional 0..1 (default tightened)
 *   "allow_estate_sites": false,            // optional, default false
 *   "debug": true,                          // optional
 *   "unsafe_disable_domain_filters": false  // optional
 * }
 *
 * Returns the author's official website ONLY if the author is viable for a personal site.
 */

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
   Tunables
   ============================= */
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 5500;
const USER_AGENT = "BookAuthorWebsite/1.4 (+https://example.com)";
const CONCURRENCY = 4;

const DEFAULT_MIN_AUTHOR_CONFIDENCE = 0.55;
const DEFAULT_MIN_SITE_CONFIDENCE = 0.65; // slightly stricter by default

/* ====== Google CSE config (trim to avoid whitespace/newlines) ====== */
const CSE_KEY = (process.env.GOOGLE_CSE_KEY || "").trim();
const CSE_ID = (process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "").trim();
const USE_SEARCH_BASE = !!(CSE_KEY && CSE_ID);

/* =============================
   Domain strategy
   ============================= */
/** Trusted for author discovery (author name correctness). */
const AUTHOR_DISCOVERY_ALLOW = [
  "wikipedia.org",
  "britannica.com",
  "books.google.com",
  "goodreads.com",
  "worldcat.org",
  "librarything.com",
  "loc.gov",
  "openlibrary.org",
  // major publishers
  "penguinrandomhouse.com",
  "harpercollins.com",
  "simonandschuster.com",
  "macmillan.com",
  "hachettebookgroup.com",
  "us.macmillan.com",
  "panmacmillan.com",
  "bloomsbury.com",
  "faber.co.uk",
];

/** Sites we query to extract original publication year. */
const PUBYEAR_SOURCES = [
  "books.google.com",
  "openlibrary.org",
  "worldcat.org",
  "goodreads.com",
  "wikipedia.org",
];

/** Block when choosing author's official site. */
const WEBSITE_BLOCKLIST = [
  // social / aggregators
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "goodreads.com",
  "imdb.com",
  "wikipedia.org",
  "wikidata.org",
  // shopping / retailers
  "amazon.",
  "ebay.",
  "barnesandnoble.com",
  "bookshop.org",
  "audible.",
  "itunes.apple.com",
  // podcast/audio
  "open.spotify.com",
  "podcasts.apple.com",
  "soundcloud.com",
  "stitcher.com",
  "podbean.com",
  "simplecast.com",
  "megaphone.fm",
  // news/magazines
  "nytimes.com",
  "theguardian.com",
  "wsj.com",
  "washingtonpost.com",
  "newyorker.com",
];

// plain-language negatives to avoid obvious wrong-site types
const WEBSITE_NEGATIVE_KEYWORDS = [
  // venues / locations
  "stadium","field","park","arena","ballpark","amphitheater","theatre","theater","museum",
  // services / businesses unrelated to authors
  "beauty","salon","spa","boutique","realtor","plumbing","roofing","hvac","law firm","attorney",
  "restaurant","cafe","bar","grill","hotel","resort","casino","real estate","accounting",
  // media that often hijacks book queries
  "trailer","soundtrack","screenplay","director","filmography","cinematography"
];

/* =============================
   Types
   ============================= */
type CacheValue = {
  book_title: string;
  inferred_author?: string;
  pub_year?: number | null;
  life_dates?: { birthYear?: number | null; deathYear?: number | null } | null;
  author_viable: boolean;
  viability_reason?: string;
  author_url?: string;
  site_title?: string | null;
  canonical_url?: string | null;
  confidence: number;           // site confidence
  author_confidence: number;    // author discovery confidence
  source: "web";
  _diag?: any;
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type WebHit = {
  title: string;
  url: string;
  host: string;
  snippet?: string;
  confidence: number;
  reasons: string[];
};

type AuthorCandidate = {
  name: string;
  score: number;
  reasons: string[];
};

type PubYearHit = {
  url: string;
  host: string;
  year: number;
  score: number;
  reasons: string[];
};

type Context = {
  bookTitle: string;
  bookTokens: string[];
  unsafeDisableDomainFilters: boolean;
  allowEstateSites: boolean;
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(parts: Record<string, any>) {
  return Object.keys(parts)
    .sort()
    .map((k) => `${k}=${JSON.stringify(parts[k])}`)
    .join("&");
}

function getCached(key: string): CacheValue | null {
  const hit = CACHE.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) {
    CACHE.delete(key);
    return null;
  }
  return hit.value;
}
function setCached(key: string, value: CacheValue) {
  CACHE.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
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

function sanitizeAuthorName(raw: string): string {
  // Remove parentheticals & punctuation noise
  let s = raw.replace(/\([^)]*\)/g, " ").replace(/[,;:]+/g, " ").replace(/\s+/g, " ").trim();
  const BAD_LEADING = new Set([
    "american","british","canadian","australian","irish","nigerian","indian","chinese","japanese","korean",
    "french","german","italian","spanish","mexican","colombian","argentine","russian","ukrainian","scottish","welsh",
    "award-winning","bestselling","best-selling","acclaimed","renowned","noted","celebrated"
  ]);
  const BAD_ROLES = new Set([
    "author","novelist","writer","poet","historian","journalist","professor","essayist","editor","biographer","memoirist"
  ]);
  const HONORIFICS = new Set(["dr.","dr","sir","dame","prof.","prof","mr.","mr","mrs.","mrs","ms.","ms"]);
  const toks = s.split(/\s+/);
  const out: string[] = [];
  for (const t of toks) {
    const tl = t.toLowerCase();
    if (HONORIFICS.has(tl)) continue;
    if (BAD_LEADING.has(tl)) continue;
    if (BAD_ROLES.has(tl)) continue;
    if (/^[A-Z][\p{L}'-]+$/u.test(t) || /^[A-Z]\.?$/.test(t)) out.push(t);
  }
  const cleaned = out.slice(0, 4).join(" ").trim();
  return cleaned || raw.trim();
}

function containsAll(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.every((n) => H.has(n));
}

function jaccard(a: string[], b: string[]) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}

function normalizeBook(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
}

function isHttpUrl(s: string) {
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function hostOf(u: string): string {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}

function originOf(u: string): string | null {
  try {
    return new URL(u).origin;
  } catch {
    return null;
  }
}

async function fetchWithTimeout(url: string, init?: RequestInit, ms = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, ...(init?.headers || {}) },
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

function isBlockedHost(host: string, disableFilters: boolean): boolean {
  if (disableFilters) return false;
  const h = host.toLowerCase();
  return WEBSITE_BLOCKLIST.some((bad) => h.includes(bad));
}
function isAllowedDiscoveryHost(host: string): boolean {
  const h = host.toLowerCase();
  return AUTHOR_DISCOVERY_ALLOW.some((good) => h.includes(good));
}

/* =============================
   Google CSE
   ============================= */
async function googleCSE(query: string, num = 10): Promise<any[]> {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(Math.max(1, Math.min(10, num))));

  const resp = await fetchWithTimeout(url.toString());
  const text = await resp.text();

  if (!resp?.ok) {
    throw new Error(`cse_http_${resp?.status}: ${text?.slice(0, 500)}`);
  }
  const data: any = JSON.parse(text);
  return Array.isArray(data?.items) ? data.items : [];
}

/* =============================
   Phase 1: Discover the author from the book title
   ============================= */

// OpenLibrary anchor to get canonical author & first publish year reliably (no API key required)
async function olLookupTitle(title: string): Promise<{ author?: string; year?: number } | null> {
  try {
    const url = new URL("https://openlibrary.org/search.json");
    url.searchParams.set("title", title);
    url.searchParams.set("limit", "10");
    const r = await fetchWithTimeout(url.toString());
    if (!r?.ok) return null;
    const j: any = await r.json();
    const docs = Array.isArray(j?.docs) ? j.docs : [];
    const scored = docs
      .filter((d: any) => d?.author_name)
      .map((d: any) => {
        const author = String((d.author_name || [])[0] || "").trim();
        const year = typeof d.first_publish_year === "number" ? d.first_publish_year : null;
        const t = String(d?.title || "");
        const sim = jaccard(tokens(t), tokens(title));
        let score = sim;
        if (year) score += 0.05;
        return { author, year, score };
      })
      .sort((a: any, b: any) => b.score - a.score);
    const top = scored[0];
    if (!top?.author) return null;
    return { author: sanitizeAuthorName(top.author), year: top.year ?? undefined };
  } catch {
    return null;
  }
}

function extractAuthorNamesFromText(text: string): string[] {
  const t = text.replace(/\s+/g, " ");
  const candidates = new Set<string>();

  // Common patterns: "by John Smith", "Author: John Smith", "Written by John Smith"
  const byRegex = /\bby\s+([A-Z][\p{L}'\-]+(?:\s+[A-Z][\p{L}'\-]+){0,3})\b/giu;
  const authorLabelRegex = /\b(?:author|writer)\s*:\s*([A-Z][\p{L}'\-]+(?:\s+[A-Z][\p{L}'\-]+){0,3})\b/giu;
  const writtenByRegex = /\bwritten\s+by\s+([A-Z][\p{L}'\-]+(?:\s+[A-Z][\p{L}'\-]+){0,3})\b/giu;

  for (const m of t.matchAll(byRegex)) candidates.add(m[1].trim());
  for (const m of t.matchAll(authorLabelRegex)) candidates.add(m[1].trim());
  for (const m of t.matchAll(writtenByRegex)) candidates.add(m[1].trim());

  return Array.from(candidates);
}

function scoreAuthorCandidate(name: string, host: string, title: string, snippet: string, ctx: Context): AuthorCandidate {
  let score = 0;
  const reasons: string[] = [];
  const nameTokens = tokens(name);
  const textToks = tokens((title || "") + " " + (snippet || ""));

  // If the book title appears strongly in the hit, that’s good.
  if (containsAll(textToks, ctx.bookTokens) || jaccard(textToks, ctx.bookTokens) >= 0.35) {
    score += 0.25;
    reasons.push("book_match");
  }

  // Trusted discovery hosts boost
  if (isAllowedDiscoveryHost(host)) {
    score += 0.25;
    reasons.push("trusted_discovery_domain");
  }

  // Full "by <name>" style extraction is strong
  const text = `${title} ${snippet}`;
  const foundBy = new RegExp(`\\bby\\s+${name.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\b`, "i").test(text);
  if (foundBy) {
    score += 0.25;
    reasons.push("by_phrase_match");
  }

  // If the candidate name is repeated or the snippet clearly centers on the person
  const nameJac = jaccard(tokens(text), nameTokens);
  if (nameJac >= 0.25) {
    score += 0.15;
    reasons.push(`name_similarity:${nameJac.toFixed(2)}`);
  }

  // penalize polluted labels (e.g., "American author Tara Westover")
  if (/\b(american|british|canadian|author|writer|novelist|journalist|poet|historian|professor)\b/i.test(name)) {
    score -= 0.15;
    reasons.push("name_contains_descriptor");
  }

  // Publisher domains get a small extra bump
  if (
    /penguinrandomhouse\.com|harpercollins\.com|simonandschuster\.com|macmillan\.com|hachettebookgroup\.com|bloomsbury\.com|panmacmillan\.com|faber\.co\.uk/i.test(
      host
    )
  ) {
    score += 0.10;
    reasons.push("publisher_domain");
  }

  return { name, score: Math.max(0, Math.min(1, score)), reasons };
}

async function discoverAuthorFromBook(bookTitle: string, ctx: Context, debug: boolean) {
  const quoted = `"${bookTitle}"`;

  // Prefer book contexts; downrank film/movie pages up front
  const queries = [
    `${quoted} book author -film -movie -screenplay -director`,
    `${quoted} novel author -film -movie`,
    `${quoted} author site:wikipedia.org -film -movie`,
    `${quoted} author site:books.google.com`,
    `${quoted} author site:goodreads.com`,
  ];

  const diag: any = { queries, hits: [], candidates: [] as any[], picked: undefined };

  // OpenLibrary anchor — get a strong initial author guess + year
  const ol = await olLookupTitle(bookTitle);
  if (debug) diag.openlibrary = ol || null;

  const limit = pLimit(CONCURRENCY);
  const allItems: any[] = (
    await Promise.all(
      queries.map((q) =>
        limit(async () => {
          const items = USE_SEARCH_BASE ? await googleCSE(q) : [];
          return items;
        })
      )
    )
  ).flat();

  const authorScores = new Map<string, AuthorCandidate>();

  for (const it of allItems) {
    const url = String(it.link || "");
    if (!isHttpUrl(url)) continue;
    const host = hostOf(url);
    const title = String(it.title || "");
    const snippet = String(it.snippet || it.htmlSnippet || "");

    if (!isAllowedDiscoveryHost(host)) continue;

    const names = new Set<string>([
      ...extractAuthorNamesFromText(title),
      ...extractAuthorNamesFromText(snippet),
    ]);

    for (const name of names) {
      if (!name) continue;
      const cand = scoreAuthorCandidate(name, host, title, snippet, ctx);
      const prev = authorScores.get(name);
      if (!prev || cand.score > prev.score) {
        authorScores.set(name, cand);
      }
    }

    if (debug) diag.hits.push({ title, url, host });
  }

  const ranked = Array.from(authorScores.values())
    .map(a => {
      // sanitize each candidate for fair comparison; boost if matching OL
      const clean = sanitizeAuthorName(a.name);
      const boost =
        ol?.author && jaccard(tokens(clean), tokens(ol.author)) >= 0.7 ? 0.20 :
        ol?.author && jaccard(tokens(clean), tokens(ol.author)) >= 0.5 ? 0.10 : 0;
      return clean && clean !== a.name
        ? { ...a, name: clean, score: Math.min(1, a.score + 0.05 + boost), reasons: [...a.reasons, "sanitized", ...(boost ? ["ol_anchor_boost"] : [])] }
        : { ...a, score: Math.min(1, a.score + boost), reasons: [...a.reasons, ...(boost ? ["ol_anchor_boost"] : [])] };
    })
    .sort((a, b) => b.score - a.score);

  const best = ranked[0] || null;
  if (debug) {
    diag.candidates = ranked.slice(0, 10);
    diag.picked = best;
  }
  return { best, _diag: debug ? diag : undefined };
}

/* =============================
   Phase 1a: Discover original publication year (author-aware)
   ============================= */
function yearCandidatesFromText(text: string): number[] {
  const out = new Set<number>();
  const re = /\b(1[6-9]\d{2}|20\d{2}|2100)\b/g; // 1600-2100
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const y = Number(m[1]);
    if (y >= 1600 && y <= new Date().getFullYear()) out.add(y);
  }
  return Array.from(out);
}

function scorePubYearHit(host: string, year: number, text: string, requireAuthorMention: boolean, authorName: string): number {
  let s = 0;
  const now = new Date().getFullYear();
  if (PUBYEAR_SOURCES.some(d => host.includes(d))) s += 0.5;
  if (year <= now) s += 0.1;
  if (year < now - 1) s += 0.05;
  if (requireAuthorMention) {
    const ok = new RegExp(`\\b${authorName.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\b`, "i").test(text);
    if (!ok) return 0; // reject hit if author isn't mentioned when required
    s += 0.2;
  }
  return Math.max(0, Math.min(1, s));
}

async function discoverOriginalPubYear(bookTitle: string, authorName: string, ctx: Context, debug: boolean) {
  const quotedBook = `"${bookTitle}"`;
  const quotedAuthor = `"${authorName}"`;
  const qA = [
    `${quotedBook} ${quotedAuthor} first published -film -movie`,
    `${quotedBook} ${quotedAuthor} original publication date -film -movie`,
    `${quotedBook} ${quotedAuthor} publication date -film -movie`,
    `${quotedBook} ${quotedAuthor} site:wikipedia.org`,
    `${quotedBook} ${quotedAuthor} site:books.google.com`,
    `${quotedBook} ${quotedAuthor} site:openlibrary.org`,
    `${quotedBook} ${quotedAuthor} site:worldcat.org`,
    `${quotedBook} ${quotedAuthor} site:goodreads.com`,
  ];
  const qB = [
    `${quotedBook} first published`,
    `${quotedBook} original publication date`,
    `${quotedBook} site:wikipedia.org`,
    `${quotedBook} site:books.google.com`,
    `${quotedBook} site:openlibrary.org`,
    `${quotedBook} site:worldcat.org`,
    `${quotedBook} site:goodreads.com`,
  ];
  const diag: any = { queriesA: qA, queriesB: qB, hits: [], candidates: [], picked: undefined };
  const limit = pLimit(CONCURRENCY);
  const itemsA = (await Promise.all(qA.map(q => limit(() => USE_SEARCH_BASE ? googleCSE(q) : [])))).flat();
  const itemsB = (await Promise.all(qB.map(q => limit(() => USE_SEARCH_BASE ? googleCSE(q) : [])))).flat();
  const cands: { year: number; score: number; url: string; host: string; phase: "A"|"B" }[] = [];
  for (const it of itemsA) {
    const url = String(it.link || ""); if (!isHttpUrl(url)) continue;
    const host = hostOf(url);
    const title = String(it.title || ""); const snippet = String(it.snippet || it.htmlSnippet || "");
    const years = new Set<number>([...yearCandidatesFromText(title), ...yearCandidatesFromText(snippet)]);
    for (const y of years) {
      const sc = scorePubYearHit(host, y, `${title} ${snippet}`, true, authorName);
      if (sc > 0) cands.push({ year: y, score: sc, url, host, phase: "A" });
    }
    if (debug) diag.hits.push({ phase: "A", title, url, host });
  }
  if (!cands.length) {
    for (const it of itemsB) {
      const url = String(it.link || ""); if (!isHttpUrl(url)) continue;
      const host = hostOf(url);
      const title = String(it.title || ""); const snippet = String(it.snippet || it.htmlSnippet || "");
      const years = new Set<number>([...yearCandidatesFromText(title), ...yearCandidatesFromText(snippet)]);
      for (const y of years) {
        const sc = scorePubYearHit(host, y, `${title} ${snippet}`, false, authorName) * 0.8;
        if (sc > 0) cands.push({ year: y, score: sc, url, host, phase: "B" });
      }
      if (debug) diag.hits.push({ phase: "B", title, url, host });
    }
  }
  const byYear = new Map<number, { year: number; score: number; support: number }>();
  for (const c of cands) {
    const g = byYear.get(c.year) || { year: c.year, score: 0, support: 0 };
    g.score = Math.max(g.score, c.score);
    g.support += 1;
    byYear.set(c.year, g);
  }
  let ranked = Array.from(byYear.values())
    .map(g => ({ ...g, score: g.score + Math.min(0.2, g.support * 0.03) }))
    .sort((a,b) => b.score - a.score || a.year - b.year);
  const best = ranked[0] ? { year: ranked[0].year } : null;
  if (debug) { diag.candidates = ranked.slice(0, 10); diag.picked = best; }

  // If still no year, fall back to OpenLibrary anchor (author-aware)
  if (!best?.year) {
    try {
      const url = new URL("https://openlibrary.org/search.json");
      url.searchParams.set("title", bookTitle);
      url.searchParams.set("author", authorName);
      url.searchParams.set("limit", "5");
      const r = await fetchWithTimeout(url.toString());
      if (r?.ok) {
        const j: any = await r.json();
        const yrs = (j?.docs || []).map((d: any) => d?.first_publish_year).filter((y: any) => typeof y === "number");
        if (yrs.length) {
          const y = Math.min(...yrs);
          if (debug) diag.openlibrary_year = y;
          return { best: { year: y }, _diag: debug ? diag : undefined };
        }
      }
    } catch { /* ignore */ }
  }

  return { best, _diag: debug ? diag : undefined };
}

// OpenLibrary fallback (no API key required)
async function tryOpenLibraryPubYear(title: string, author: string): Promise<number | null> {
  try {
    const url = new URL("https://openlibrary.org/search.json");
    url.searchParams.set("title", title);
    url.searchParams.set("author", author);
    url.searchParams.set("limit", "5");
    const r = await fetchWithTimeout(url.toString());
    if (!r?.ok) return null;
    const j: any = await r.json();
    const yrs = (j?.docs || [])
      .map((d: any) => d?.first_publish_year)
      .filter((y: any) => typeof y === "number");
    if (!yrs.length) return null;
    return Math.min(...yrs);
  } catch {
    return null;
  }
}

/* =============================
   Phase 1b: Author viability check (life dates + official link + pub year)
   ============================= */
async function findWikipediaUrlForAuthor(authorName: string): Promise<string | null> {
  if (!USE_SEARCH_BASE) return null;
  const items = await googleCSE(`"${authorName}" site:wikipedia.org`);
  const first = items.find((it: any) => typeof it?.link === "string" && /wikipedia\.org\/wiki\//i.test(it.link));
  return first ? String(first.link) : null;
}

function parseLifeDatesFromWikipedia(html: string): { birthYear?: number | null; deathYear?: number | null; hasOfficialLink: boolean } {
  const takeYear = (s?: string | null) => {
    const m = s?.match(/(\d{4})/);
    return m ? Number(m[1]) : null;
  };

  const birth = html.match(/class=["']bday["'][^>]*>(\d{4})-(\d{2})-(\d{2})/i)?.[1];
  const birthYearA = takeYear(birth);
  const bornRow = html.match(/>Born<[^]*?<td[^>]*>([^<]+)</i)?.[1] ?? null;
  const birthYearB = takeYear(bornRow);

  const deathdate = html.match(/class=["']dday deathdate["'][^>]*>(\d{4})-(\d{2})-(\d{2})/i)?.[1];
  const deathYearA = takeYear(deathdate);
  const diedRow = html.match(/>Died<[^]*?<td[^>]*>([^<]+)</i)?.[1] ?? null;
  const deathYearB = takeYear(diedRow);

  const hasOfficialLink =
    /Official\s+website/i.test(html) &&
    /<a[^>]+href=["'][^"']+["'][^>]*>/i.test(html);

  const birthYear = birthYearA ?? birthYearB ?? null;
  const deathYear = deathYearA ?? deathYearB ?? null;

  return { birthYear, deathYear, hasOfficialLink };
}

async function getAuthorLifeDatesAndOfficial(authorName: string): Promise<{ birthYear?: number | null; deathYear?: number | null; hasOfficialLink: boolean; wikiUrl?: string | null }> {
  try {
    const wikiUrl = await findWikipediaUrlForAuthor(authorName);
    if (!wikiUrl) return { birthYear: null, deathYear: null, hasOfficialLink: false, wikiUrl: null };
    const res = await fetchWithTimeout(wikiUrl);
    if (!res?.ok) return { birthYear: null, deathYear: null, hasOfficialLink: false, wikiUrl };
    const html = await res.text();
    const parsed = parseLifeDatesFromWikipedia(html);
    return { ...parsed, wikiUrl };
  } catch {
    return { birthYear: null, deathYear: null, hasOfficialLink: false, wikiUrl: null };
  }
}

function evaluateAuthorViability(
  birthYear: number | null | undefined,
  deathYear: number | null | undefined,
  hasOfficialLinkOnWiki: boolean,
  pubYear: number | null | undefined,
  allowEstateSites: boolean
): { viable: boolean; reason: string } {
  const nowYear = new Date().getFullYear();

  // If Wikipedia explicitly lists an Official website, accept.
  if (hasOfficialLinkOnWiki) {
    return { viable: true, reason: "official_link_listed_on_wikipedia" };
  }

  // Life dates provided:
  if (typeof deathYear === "number") {
    if (deathYear < 1995) return { viable: false, reason: "deceased_pre_web_era" };
    if (deathYear < 2005 && !allowEstateSites) {
      return { viable: false, reason: "deceased_early_web_era_estate_not_allowed" };
    }
    return allowEstateSites
      ? { viable: true, reason: "deceased_estate_allowed" }
      : { viable: false, reason: "deceased_estate_sites_not_allowed" };
  }
  if (typeof birthYear === "number") {
    const age = nowYear - birthYear;
    if (age >= 0 && age < 120) {
      return { viable: true, reason: "likely_living_author" };
    }
  }

  // No life dates: use original publication year heuristics
  if (typeof pubYear === "number") {
    if (pubYear < 1900) {
      return { viable: false, reason: "pubyear_lt_1900_not_viable" };
    } else if (pubYear < 1950) {
      return allowEstateSites
        ? { viable: true, reason: "pubyear_1900_1950_estate_allowed" }
        : { viable: false, reason: "pubyear_1900_1950_estate_not_allowed" };
    } else if (pubYear < 1995) {
      return { viable: true, reason: "pubyear_1950_1994_cautious" };
    } else {
      return { viable: true, reason: "pubyear_ge_1995_likely_living" };
    }
  }

  // Nothing definitive: be conservative
  return { viable: false, reason: "unknown_life_and_pubyear_not_confident" };
}

/* =============================
   Phase 2: Find the author's official website (only if viable)
   ============================= */
function baseHit(it: any): WebHit {
  const url = String(it.link || "");
  return {
    title: String(it.title || ""),
    url,
    host: hostOf(url),
    snippet: String(it.snippet || it.htmlSnippet || ""),
    confidence: 0,
    reasons: [],
  };
}

function scoreBaseWebsiteSignals(hit: WebHit, authorName: string): WebHit {
  let score = 0;
  const reasons: string[] = [];

  const t = tokens(hit.title + " " + (hit.snippet || ""));
  const authorTokens = tokens(authorName);

  if (containsAll(t, authorTokens) || jaccard(t, authorTokens) >= 0.35) {
    score += 0.35;
    reasons.push("author_in_title_or_snippet");
  }

  if (isBlockedHost(hit.host, false)) {
    score -= 1.0;
    reasons.push("blocked_domain");
  }

  // Vanity domain preference
  const compact = authorName.toLowerCase().replace(/\s+/g, "");
  const hostBare = hit.host.replace(/^www\./, "");
  if (hostBare.startsWith(compact) || hostBare.includes(compact)) {
    score += 0.25;
    reasons.push("vanity_domain_match");
  }

  // Slight preference for custom domains over platforms
  if (!/substack\.com|medium\.com|wordpress\.com|blogspot\.|ghost\.io/i.test(hit.host)) {
    score += 0.05;
    reasons.push("custom_domain_preferred");
  } else {
    reasons.push("platform_host");
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reasons = reasons;
  return hit;
}

async function fetchHtmlSignals(url: string, authorName: string, bookTokens: string[]): Promise<{
  titleTokens: string[];
  hasCopyrightName: boolean;
  hasSchemaPerson: boolean;
  hasOfficialWords: boolean;
  bookMentioned: boolean;
  estateWording: boolean;
  isPublisherAuthorPage: boolean;
  isClearlyFilmOrDirector: boolean;
  hasAuthorialNav: boolean; // presence of "Books" or "Writing" sections
  negativeKeywordHit: string | null;
}> {
  try {
    const res = await fetchWithTimeout(url);
    if (!res?.ok) {
      return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false, estateWording: false, isPublisherAuthorPage: false, isClearlyFilmOrDirector: false, hasAuthorialNav: false, negativeKeywordHit: null };
    }
    const html = await res.text();
    const head = html.slice(0, 120_000);

    const title = /<title[^>]*>([^<]*)<\/title>/i.exec(head)?.[1] ?? "";
    const tt = tokens(title);

    const lower = head.toLowerCase();
    const authorLower = authorName.toLowerCase();

    const hasOfficialWords = /\bofficial (site|website)\b/i.test(title) || /\bofficial (site|website)\b/i.test(head);
    const hasSchemaPerson = /"@type"\s*:\s*"(?:Person|Author)"/i.test(head);

    const hasCopyrightName =
      (/\u00A9|&copy;|copyright/i.test(head)) &&
      (lower.includes(authorLower) || jaccard(tokens(head), tokens(authorName)) >= 0.25);

    const bookMentioned =
      bookTokens.length > 0 &&
      (containsAll(tokens(head), bookTokens) || jaccard(tokens(head), bookTokens) >= 0.25);

    const estateWording = /\b(Estate\s+of\s+|Official\s+Estate\s+of\s+)/i.test(head);

    const isPublisherAuthorPage =
      /\b(author|authors)\/[a-z0-9-]+/i.test(head) || /\brel=["']author["']/i.test(head);

    const isClearlyFilmOrDirector =
      /\b(film|movie|director|screenplay|trailer|cinematography|production company)\b/i.test(head);

    const hasAuthorialNav =
      /\b(books|novels|writing|bio|about)\b/i.test(head);

    let negativeKeywordHit: string | null = null;
    for (const w of WEBSITE_NEGATIVE_KEYWORDS) {
      if (new RegExp(`\\b${w}\\b`, "i").test(head)) { negativeKeywordHit = w; break; }
    }

    return { titleTokens: tt, hasCopyrightName, hasSchemaPerson, hasOfficialWords, bookMentioned, estateWording, isPublisherAuthorPage, isClearlyFilmOrDirector, hasAuthorialNav, negativeKeywordHit };
  } catch {
    return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false, estateWording: false, isPublisherAuthorPage: false, isClearlyFilmOrDirector: false, hasAuthorialNav: false, negativeKeywordHit: null };
  }
}

async function enrichWebsiteHit(hit: WebHit, authorName: string, bookTokens: string[]): Promise<WebHit> {
  const origin = originOf(hit.url);
  if (!origin) return hit;

  const [home, about] = await Promise.all([
    fetchHtmlSignals(origin, authorName, bookTokens),
    fetchHtmlSignals(origin + "/about", authorName, bookTokens),
  ]);

  const authorTokens = tokens(authorName);

  for (const s of [home, about]) {
    if (!s) continue;

    if (containsAll(s.titleTokens, authorTokens) || jaccard(s.titleTokens, authorTokens) >= 0.4) {
      hit.confidence = Math.min(1, hit.confidence + 0.15);
      hit.reasons.push("title_matches_author");
    }
    if (s.hasOfficialWords) {
      hit.confidence = Math.min(1, hit.confidence + 0.20);
      hit.reasons.push("official_wording");
    }
    if (s.hasCopyrightName) {
      hit.confidence = Math.min(1, hit.confidence + 0.15);
      hit.reasons.push("copyright_name_match");
    }
    if (s.hasSchemaPerson) {
      hit.confidence = Math.min(1, hit.confidence + 0.10);
      hit.reasons.push("schema_person_present");
    }
    if (s.bookMentioned) {
      hit.confidence = Math.min(1, hit.confidence + 0.10);
      hit.reasons.push("book_mentioned_on_site");
    }
    if (s.estateWording) {
      hit.reasons.push("estate_wording_detected");
    }
    if (s.isPublisherAuthorPage) {
      hit.confidence = Math.min(1, hit.confidence + 0.15);
      hit.reasons.push("publisher_author_page");
    }
    if (s.hasAuthorialNav) {
      hit.confidence = Math.min(1, hit.confidence + 0.05);
      hit.reasons.push("authorial_nav_present");
    }
    if (s.isClearlyFilmOrDirector) {
      hit.confidence = Math.max(0, hit.confidence - 0.35);
      hit.reasons.push("film_director_context");
    }
    if (s.negativeKeywordHit) {
      hit.confidence = Math.max(0, hit.confidence - 0.50);
      hit.reasons.push(`negative_keyword:${s.negativeKeywordHit}`);
    }
  }

  return hit;
}

// ---- Book+Author fast-path queries (new) ----
function buildBookAuthorSiteQueries(bookTitle: string, authorName: string): string[] {
  const qBook = `"${bookTitle}"`;
  const qAuthor = `"${authorName}"`;
  const tail = "-film -movie -director -trailer";
  return [
    `${qBook} ${qAuthor} official website ${tail}`,
    `${qBook} ${qAuthor} author website ${tail}`,
    `${qBook} ${qAuthor} official site ${tail}`,
    // Publisher context sometimes points to a personal domain via the author page:
    `${qBook} ${qAuthor} site:penguinrandomhouse.com ${tail}`,
    `${qBook} ${qAuthor} site:harpercollins.com ${tail}`,
    `${qBook} ${qAuthor} site:simonandschuster.com ${tail}`,
    `${qBook} ${qAuthor} site:macmillan.com ${tail}`,
    `${qBook} ${qAuthor} site:hachettebookgroup.com ${tail}`,
  ];
}

async function searchAuthorWebsiteWithBookContext(
  authorName: string,
  bookTitle: string,
  ctx: Context,
  debug: boolean
) {
  const queries = buildBookAuthorSiteQueries(bookTitle, authorName);
  const diag: any = { queries, candidates: [], picked: undefined };

  const limit = pLimit(CONCURRENCY);
  const items = (await Promise.all(
    queries.map(q => limit(() => (USE_SEARCH_BASE ? googleCSE(q) : [])))
  )).flat();

  const seen = new Set<string>();
  const prelim = items
    .map(baseHit)
    .filter(h => isHttpUrl(h.url))
    .filter(h => !seen.has(h.url) && seen.add(h.url))
    .map(h => scoreBaseWebsiteSignals(h, authorName))
    .filter(h => (ctx.unsafeDisableDomainFilters ? true : !isBlockedHost(h.host, false)));

  const enriched = await Promise.all(prelim.map(h => limit(() => enrichWebsiteHit(h, authorName, ctx.bookTokens))));

  // Require strong author-site signals
  const accepted = enriched.filter(h => {
    if (!ctx.allowEstateSites && h.reasons.includes("estate_wording_detected")) return false;

    const strong =
      h.reasons.includes("vanity_domain_match") ||
      h.reasons.includes("schema_person_present") ||
      h.reasons.includes("publisher_author_page") ||
      h.reasons.includes("copyright_name_match") ||
      h.reasons.includes("title_matches_author");

    return strong && !h.reasons.includes("blocked_domain");
  });

  accepted.sort((a, b) => b.confidence - a.confidence);

  if (debug) {
    diag.candidates = accepted.slice(0, 10).map(h => ({
      url: h.url, host: h.host, confidence: Number(h.confidence.toFixed(2)), reasons: h.reasons
    }));
    if (accepted[0]) diag.picked = { url: accepted[0].url, confidence: Number(accepted[0].confidence.toFixed(2)), reasons: accepted[0].reasons };
  }

  return { best: accepted[0] || null, _diag: debug ? diag : undefined };
}

// ---- Generic author-only search (existing) ----
async function searchAuthorWebsite(authorName: string, ctx: Context, debug: boolean) {
  const quotedAuthor = `"${authorName}"`;

  const queries = [
    `${quotedAuthor} official site -film -movie -director`,
    `${quotedAuthor} official website -film -movie -director`,
    `${quotedAuthor} author website -film -movie`,
    `${quotedAuthor} site -film -movie`,
    `${quotedAuthor} website -film -movie`,
  ];

  const diag: any = { queries, candidates: [] as any[], picked: undefined };

  const limit = pLimit(CONCURRENCY);
  const allItems: any[] = (
    await Promise.all(
      queries.map((q) =>
        limit(async () => {
          const items = USE_SEARCH_BASE ? await googleCSE(q) : [];
          return items;
        })
      )
    )
  ).flat();

  const seen = new Set<string>();
  const prelim: WebHit[] = [];
  for (const it of allItems) {
    const hit = baseHit(it);
    if (!isHttpUrl(hit.url)) continue;
    if (seen.has(hit.url)) continue;
    seen.add(hit.url);
    prelim.push(scoreBaseWebsiteSignals(hit, authorName));
  }

  const filtered = prelim.filter((h) =>
    ctx.unsafeDisableDomainFilters ? true : !isBlockedHost(h.host, false)
  );

  const enriched = await Promise.all(filtered.map((h) => limit(() => enrichWebsiteHit(h, authorName, ctx.bookTokens))));

  const postFiltered = enriched.filter((h) => {
    if (!ctx.allowEstateSites && h.reasons.includes("estate_wording_detected")) return false;
    // Acceptance guard: require one of these strong conditions
    const strongSignals =
      h.reasons.includes("vanity_domain_match") ||
      h.reasons.includes("schema_person_present") ||
      h.reasons.includes("publisher_author_page") ||
      h.reasons.includes("copyright_name_match") ||
      h.reasons.includes("title_matches_author");
    return strongSignals && !h.reasons.includes("blocked_domain");
  });

  postFiltered.sort((a, b) => b.confidence - a.confidence);

  if (debug) diag.candidates = postFiltered.slice(0, 10).map((h) => ({
    url: h.url, host: h.host, confidence: Number(h.confidence.toFixed(2)), reasons: h.reasons
  }));

  const best = postFiltered[0] || null;
  if (debug && best) diag.picked = { url: best.url, confidence: Number(best.confidence.toFixed(2)), reasons: best.reasons };

  return { best, _diag: debug ? diag : undefined };
}

/* =============================
   CORS
   ============================= */
function cors(res: any, origin?: string) {
  res.setHeader("Access-Control-Allow-Origin", origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Auth");
}

/* =============================
   Handler
   ============================= */
export default async function handler(req: any, res: any) {
  try {
    cors(res, req.headers?.origin);
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "POST required" });
    if (!requireAuth(req, res)) return;

    const bodyRaw = req.body ?? {};
    const body = typeof bodyRaw === "string" ? JSON.parse(bodyRaw || "{}") : bodyRaw;

    // Require ONLY a book title
    const bookTitleInput = String(body.book_title || "");
    const bookTitle = normalizeBook(bookTitleInput);
    if (!bookTitle) return res.status(400).json({ error: "book_title required" });

    const debug: boolean = body.debug === true;
    const minAuthorConfidence: number =
      typeof body.min_author_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_author_confidence))
        : DEFAULT_MIN_AUTHOR_CONFIDENCE;

    const minSiteConfidence: number =
      typeof body.min_site_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_site_confidence))
        : DEFAULT_MIN_SITE_CONFIDENCE;

    const allowEstateSites: boolean = body.allow_estate_sites === true;
    const unsafeDisableDomainFilters: boolean = body.unsafe_disable_domain_filters === true;

    const includeSearchRequested: boolean = body.include_search !== false; // default true
    const USE_SEARCH = includeSearchRequested && USE_SEARCH_BASE;
    res.setHeader("x-use-search", String(USE_SEARCH));
    res.setHeader("x-cse-id-present", String(!!CSE_ID));
    res.setHeader("x-cse-key-present", String(!!CSE_KEY));

    const bookTokens = tokens(bookTitle);
    const ctx: Context = { bookTitle, bookTokens, unsafeDisableDomainFilters, allowEstateSites };

    const cacheKey = makeCacheKey({
      bookTitle, minAuthorConfidence, minSiteConfidence, USE_SEARCH, unsafeDisableDomainFilters, allowEstateSites,
    });

    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    if (!USE_SEARCH) {
      const fail: CacheValue = {
        book_title: bookTitle,
        inferred_author: undefined,
        pub_year: null,
        life_dates: null,
        author_viable: false,
        viability_reason: "search_disabled_or_not_configured",
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: 0,
        source: "web",
        _diag: debug ? { reason: "search_disabled_or_not_configured" } : undefined,
      };
      setCached(cacheKey, fail);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(fail);
    }

    /* ---------- Phase 1: Author discovery ---------- */
    const { best: authorBest, _diag: authorDiag } = await discoverAuthorFromBook(bookTitle, ctx, debug);

    if (!authorBest || authorBest.score < minAuthorConfidence) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorBest?.name && sanitizeAuthorName(authorBest.name),
        pub_year: null,
        life_dates: null,
        author_viable: false,
        viability_reason: "no_confident_author_match",
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: authorBest ? Number(authorBest.score.toFixed(2)) : 0,
        source: "web",
        _diag: debug ? { author: authorDiag } : undefined,
      };
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Ensure sanitized author name is used everywhere downstream
    const authorName = sanitizeAuthorName(authorBest.name);

    /* ---------- Phase 1a: Original publication year (author-aware) ---------- */
    const { best: pubBest, _diag: pubDiag } = await discoverOriginalPubYear(bookTitle, authorName, ctx, debug);
    let pubYear = pubBest?.year ?? null;
    if (!pubYear) {
      const ol = await tryOpenLibraryPubYear(bookTitle, authorName);
      if (ol) pubYear = ol;
    }

    /* ---------- Phase 1b: Viability gate ---------- */
    const life = await getAuthorLifeDatesAndOfficial(authorName);

    // Guard against absurd mismatches: pub year decades before the author's plausible life
    let finalPubYear = pubYear;
    if (life?.birthYear && typeof finalPubYear === "number") {
      if (finalPubYear < (life.birthYear as number) - 20) {
        finalPubYear = null; // discard obviously wrong year
      }
    }

    const { viable, reason } = evaluateAuthorViability(
      life.birthYear ?? null,
      life.deathYear ?? null,
      life.hasOfficialLink,
      finalPubYear,
      ctx.allowEstateSites
    );

    if (!viable) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorName,
        pub_year: finalPubYear,
        life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
        author_viable: false,
        viability_reason: reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: Number(authorBest.score.toFixed(2)),
        source: "web",
        _diag: debug ? { author: authorDiag, pubyear: pubDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason } } : undefined,
      };
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    /* ---------- Phase 2: Author website search ---------- */

    // 2a) Book+Author fast path (new)
    const fast = await searchAuthorWebsiteWithBookContext(authorName, bookTitle, ctx, debug);
    let siteBest = fast.best;
    const siteDiagAll: any = { book_author_firstpass: fast._diag };

    // 2b) Fallback to generic author-only search (existing) if needed
    if (!siteBest || siteBest.confidence < minSiteConfidence) {
      const generic = await searchAuthorWebsite(authorName, ctx, debug);
      if (!siteBest || (generic.best && generic.best.confidence > siteBest.confidence)) {
        siteBest = generic.best;
      }
      siteDiagAll.generic = generic._diag;
    }

    if (!siteBest || siteBest.confidence < minSiteConfidence) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorName,
        pub_year: finalPubYear,
        life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
        author_viable: true,
        viability_reason: reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: siteBest ? Number(siteBest.confidence.toFixed(2)) : 0,
        author_confidence: Number(authorBest.score.toFixed(2)),
        source: "web",
        _diag: debug ? { author: authorDiag, pubyear: pubDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason }, site: siteDiagAll } : undefined,
      };
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Normalize to origin, fetch canonical & title
    const origin = originOf(siteBest.url)!;

    let siteTitle: string | null = null;
    let canonical: string | null = null;
    try {
      const resp = await fetchWithTimeout(origin);
      if (resp?.ok) {
        const html = await resp.text();
        siteTitle = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1]?.trim() || null;
        const mCanon = html.match(/<link[^>]+rel=["']canonical["'][^>]*>/i)?.[0] || "";
        canonical =
          /href=["']([^"']+)["']/i.exec(mCanon)?.[1] ||
          null;
        if (canonical && !isHttpUrl(canonical)) {
          canonical = new URL(canonical, origin).toString();
        }
      }
    } catch {
      /* ignore */
    }

    const payload: CacheValue = {
      book_title: bookTitle,
      inferred_author: authorName,
      pub_year: finalPubYear,
      life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
      author_viable: true,
      viability_reason: reason,
      author_url: origin,
      site_title: siteTitle,
      canonical_url: canonical || origin,
      confidence: Number(siteBest.confidence.toFixed(2)),
      author_confidence: Number(authorBest.score.toFixed(2)),
      source: "web",
      _diag: debug ? { author: authorDiag, pubyear: pubDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason }, site: siteDiagAll } : undefined,
    };

    setCached(cacheKey, payload);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(payload);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
