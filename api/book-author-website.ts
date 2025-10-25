// api/book-author-website.ts
/// <reference types="node" />

/**
 * POST Body:
 * {
 *   "book_title": "Educated",
 *   "include_search": true,                 // default true (requires GOOGLE_CSE_* env)
 *   "min_author_confidence": 0.55,          // optional 0..1
 *   "min_site_confidence": 0.65,            // optional 0..1
 *   "allow_estate_sites": false,            // optional, default false
 *   "exclude_publisher_sites": true,        // optional, default true-ish behavior via classifier/acceptance
 *   "unsafe_disable_domain_filters": false, // optional
 *   "debug": true                           // optional
 * }
 *
 * Returns an author's official/personal website ONLY if the author is viable for a personal site.
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
const FETCH_TIMEOUT_MS = 7000;
const USER_AGENT = "BookAuthorWebsite/2.0 (+https://example.com)";
const CONCURRENCY = 4;

const DEFAULT_MIN_AUTHOR_CONFIDENCE = 0.58;
const DEFAULT_MIN_SITE_CONFIDENCE = 0.68;

/* ====== Google CSE config ====== */
const CSE_KEY = (process.env.GOOGLE_CSE_KEY || "").trim();
const CSE_ID = (process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "").trim();
const USE_SEARCH_BASE = !!(CSE_KEY && CSE_ID);

/* =============================
   Domain/host policy
   ============================= */
/** Block when choosing author's official site: obvious non-author destinations (not publishers). */
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
];

/** Sites used to extract original publication year. */
const PUBYEAR_SOURCES = [
  "books.google.com",
  "openlibrary.org",
  "worldcat.org",
  "goodreads.com",
  "wikipedia.org",
];

// negative keywords to avoid wrong-site types
const WEBSITE_NEGATIVE_KEYWORDS = [
  // venues / locations
  "stadium","field","park","arena","ballpark","amphitheater","theatre","theater","museum",
  // services / businesses unrelated to authors
  "beauty","salon","spa","boutique","realtor","plumbing","roofing","hvac","law firm","attorney",
  "restaurant","cafe","bar","grill","hotel","resort","casino","real estate","accounting",
  // media that often hijacks book queries
  "trailer","soundtrack","screenplay","director","filmography","cinematography",
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
  // classifier outputs
  personalScore?: number;
  publisherScore?: number;
  typeSignals?: string[];
};

type AuthorCandidate = {
  name: string;
  score: number;
  reasons: string[];
};

type Context = {
  bookTitle: string;
  bookTokens: string[];
  unsafeDisableDomainFilters: boolean;
  allowEstateSites: boolean;
  excludePublisherSites: boolean;
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

function eTLD1(host: string): string {
  const h = (host || "").replace(/^www\./, "");
  const parts = h.split(".");
  if (parts.length <= 2) return h;
  return parts.slice(-2).join(".");
}

function dedupeByETLD1(urls: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const u of urls) {
    try {
      const k = eTLD1(hostOf(u));
      if (!seen.has(k)) {
        seen.add(k);
        out.push(u);
      }
    } catch { /* ignore */ }
  }
  return out;
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

  if (containsAll(textToks, ctx.bookTokens) || jaccard(textToks, ctx.bookTokens) >= 0.35) {
    score += 0.25;
    reasons.push("book_match");
  }
  if (isAllowedDiscoveryHost(host)) {
    score += 0.25;
    reasons.push("trusted_discovery_domain");
  }
  const text = `${title} ${snippet}`;
  const foundBy = new RegExp(`\\bby\\s+${name.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\b`, "i").test(text);
  if (foundBy) {
    score += 0.25;
    reasons.push("by_phrase_match");
  }
  const nameJac = jaccard(tokens(text), nameTokens);
  if (nameJac >= 0.25) {
    score += 0.12;
    reasons.push(`name_similarity:${nameJac.toFixed(2)}`);
  }
  if (/\b(american|british|canadian|author|writer|novelist|journalist|poet|historian|professor)\b/i.test(name)) {
    score -= 0.15;
    reasons.push("name_contains_descriptor");
  }
  return { name, score: Math.max(0, Math.min(1, score)), reasons };
}

async function discoverAuthorFromBook(bookTitle: string, ctx: Context, debug: boolean) {
  const quoted = `"${bookTitle}"`;
  const queries = [
    `${quoted} book author -film -movie -screenplay -director`,
    `${quoted} novel author -film -movie`,
    `${quoted} author site:wikipedia.org -film -movie`,
    `${quoted} author site:books.google.com`,
    `${quoted} author site:goodreads.com`,
  ];
  const diag: any = { queries, hits: [], candidates: [] as any[], picked: undefined };

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
   Phase 1a: Publication year (author-aware) & viability
   ============================= */
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

  if (hasOfficialLinkOnWiki) return { viable: true, reason: "official_link_listed_on_wikipedia" };

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
    if (age >= 0 && age < 120) return { viable: true, reason: "likely_living_author" };
  }

  if (typeof pubYear === "number") {
    if (pubYear < 1900) return { viable: false, reason: "pubyear_lt_1900_not_viable" };
    if (pubYear < 1950) return allowEstateSites
      ? { viable: true, reason: "pubyear_1900_1950_estate_allowed" }
      : { viable: false, reason: "pubyear_1900_1950_estate_not_allowed" };
    if (pubYear < 1995) return { viable: true, reason: "pubyear_1950_1994_cautious" };
    return { viable: true, reason: "pubyear_ge_1995_likely_living" };
  }

  return { viable: false, reason: "unknown_life_and_pubyear_not_confident" };
}

/* =============================
   Phase 2 pre-step: Wikidata official website (P856)
   ============================= */
async function lookupWikidataOfficialSite(authorName: string): Promise<string | null> {
  try {
    const api = new URL("https://www.wikidata.org/w/api.php");
    api.searchParams.set("action", "wbsearchentities");
    api.searchParams.set("search", authorName);
    api.searchParams.set("language", "en");
    api.searchParams.set("format", "json");
    api.searchParams.set("type", "item");
    const r = await fetchWithTimeout(api.toString());
    if (!r?.ok) return null;
    const j: any = await r.json();
    const id = j?.search?.[0]?.id;
    if (!id) return null;

    const ent = new URL("https://www.wikidata.org/w/api.php");
    ent.searchParams.set("action", "wbgetentities");
    ent.searchParams.set("ids", id);
    ent.searchParams.set("props", "claims");
    ent.searchParams.set("format", "json");
    const rr = await fetchWithTimeout(ent.toString());
    if (!rr?.ok) return null;
    const ej: any = await rr.json();
    const p856 = ej?.entities?.[id]?.claims?.P856?.[0]?.mainsnak?.datavalue?.value;
    return typeof p856 === "string" ? p856 : null;
  } catch {
    return null;
  }
}

function isBareHomepage(url: string): boolean {
  try {
    const u = new URL(url);
    const p = u.pathname.replace(/\/+$/, "");
    return p === "" || p === "/";
  } catch {
    return false;
  }
}

function hasAuthorSlugPath(url: string): boolean {
  try {
    const p = new URL(url).pathname.toLowerCase();
    return /\b\/authors?\/[-a-z0-9]+/.test(p) || /\b\/author\/[-a-z0-9]+/.test(p);
  } catch {
    return false;
  }
}

/* =============================
   Classifier: personal vs publisher page
   ============================= */
type SiteTypeScores = {
  personalScore: number;
  publisherScore: number;
  signals: string[];
};

function classifySiteType(html: string, url: string, authorName: string, bookTokens: string[]): SiteTypeScores {
  const signals: string[] = [];
  let personal = 0;
  let publisher = 0;

  const lower = html.toLowerCase();
  const title = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1] ?? "";
  const h1 = /<h1[^>]*>([^<]{2,160})<\/h1>/i.exec(html)?.[1] ?? "";
  const titleTokens = tokens(title);
  const h1Tokens = tokens(h1);
  const authorTokens = tokens(authorName);

  const hasPersonSchema = /"@type"\s*:\s*"(?:Person|Author)"/i.test(html);
  const hasOrgSchema = /"@type"\s*:\s*"(?:Organization|Corporation|Brand|BookStore)"/i.test(html);

  const authorInTitle = containsAll(titleTokens, authorTokens) || jaccard(titleTokens, authorTokens) >= 0.5;
  const authorInH1 = containsAll(h1Tokens, authorTokens) || jaccard(h1Tokens, authorTokens) >= 0.5;

  const hasOfficialWording = /\bofficial (site|website)\b/i.test(html) || /\bthe official website of\b/i.test(html);
  const hasBioNav = /\b(about|bio|contact|events|appearances|press|media)\b/i.test(lower);
  const hasWritingNav = /\b(books|novels|writing|blog|news|essays)\b/i.test(lower);

  const hasISBN = /\bISBN(?:-1[03])?\b/i.test(html);
  const manyIsbns = (html.match(/\bISBN(?:-1[03])?\b/gi) || []).length >= 2;
  const hasBuyButtons = /\b(add to cart|buy now|pre-?order|preorder)\b/i.test(lower);
  const hasPrice = /\b\$\d+(\.\d{2})?\b/i.test(html) || /\b£\d+(\.\d{2})?\b/i.test(html) || /\b€\d+(\.\d{2})?\b/i.test(html);
  const hasRightsImprint = /\b(imprint|rights|permissions|publicity|catalog)\b/i.test(lower);
  const hasAuthorGrid = /\b(authors|our authors)\b/i.test(lower) && /class=["'][^"']*(grid|cards?|list|directory)[^"']*["']/i.test(html);
  const hasStoreNav = /\b(shop|store|merch|cart|basket|checkout)\b/i.test(lower);

  const bookMentioned =
    bookTokens.length > 0 && (containsAll(tokens(html), bookTokens) || jaccard(tokens(html), bookTokens) >= 0.25);

  const pathHasAuthor = hasAuthorSlugPath(url);
  const homepage = isBareHomepage(url);

  if (hasPersonSchema) { personal += 0.35; signals.push("schema:Person"); }
  if (hasOrgSchema) { publisher += 0.25; signals.push("schema:Organization"); }

  if (authorInTitle) { personal += 0.25; signals.push("author_in_title"); }
  if (authorInH1) { personal += 0.25; signals.push("author_in_h1"); }
  if (hasOfficialWording) { personal += 0.20; signals.push("official_wording"); }
  if (hasBioNav) { personal += 0.10; signals.push("nav_bio_contact"); }
  if (hasWritingNav) { personal += 0.10; signals.push("nav_books_writing"); }
  if (bookMentioned) { personal += 0.08; signals.push("book_mentioned"); }

  if (hasISBN) { publisher += 0.15; signals.push("isbn_present"); }
  if (manyIsbns) { publisher += 0.15; signals.push("multiple_isbns"); }
  if (hasBuyButtons) { publisher += 0.15; signals.push("buy_buttons"); }
  if (hasPrice) { publisher += 0.10; signals.push("price_present"); }
  if (hasRightsImprint) { publisher += 0.15; signals.push("rights_imprint_terms"); }
  if (hasAuthorGrid) { publisher += 0.15; signals.push("author_grid_directory"); }
  if (hasStoreNav) { publisher += 0.10; signals.push("nav_store_cart"); }

  if (pathHasAuthor) { personal += 0.15; signals.push("path_author_slug"); }
  if (homepage) { publisher += 0.10; signals.push("bare_homepage"); }

  personal = Math.max(0, Math.min(1, personal));
  publisher = Math.max(0, Math.min(1, publisher));

  return { personalScore: personal, publisherScore: publisher, signals };
}

/* =============================
   Phase 2 scoring/selection helpers
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

  const compact = authorName.toLowerCase().replace(/\s+/g, "");
  const hostBare = hit.host.replace(/^www\./, "");
  if (hostBare.startsWith(compact) || hostBare.includes(compact)) {
    score += 0.25;
    reasons.push("vanity_domain_match");
  }

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
  isClearlyFilmOrDirector: boolean;
  hasAuthorialNav: boolean;
  negativeKeywordHit: string | null;
}> {
  try {
    const res = await fetchWithTimeout(url);
    if (!res?.ok) {
      return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false, estateWording: false, isClearlyFilmOrDirector: false, hasAuthorialNav: false, negativeKeywordHit: null };
    }
    const html = await res.text();
    const head = html.slice(0, 150_000);

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

    const isClearlyFilmOrDirector =
      /\b(film|movie|director|screenplay|trailer|cinematography|production company)\b/i.test(head);

    const hasAuthorialNav =
      /\b(books|novels|writing|bio|about|contact|events|press|media)\b/i.test(head);

    let negativeKeywordHit: string | null = null;
    for (const w of WEBSITE_NEGATIVE_KEYWORDS) {
      if (new RegExp(`\\b${w}\\b`, "i").test(head)) { negativeKeywordHit = w; break; }
    }

    return { titleTokens: tt, hasCopyrightName, hasSchemaPerson, hasOfficialWords, bookMentioned, estateWording, isClearlyFilmOrDirector, hasAuthorialNav, negativeKeywordHit };
  } catch {
    return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false, estateWording: false, isClearlyFilmOrDirector: false, hasAuthorialNav: false, negativeKeywordHit: null };
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

  // Classify site type using homepage HTML (for decisive personal vs publisher signal)
  try {
    const resp = await fetchWithTimeout(origin);
    if (resp?.ok) {
      const html = await resp.text();
      const cls = classifySiteType(html, origin, authorName, bookTokens);
      hit.personalScore = cls.personalScore;
      hit.publisherScore = cls.publisherScore;
      hit.typeSignals = cls.signals;

      // Nudge confidence along classifier direction
      hit.confidence = Math.min(1, Math.max(0, hit.confidence + (cls.personalScore - cls.publisherScore) * 0.2));

      // Soft penalty for generic homepage not matching vanity-ish host
      const isHomepage = isBareHomepage(hit.url);
      const compact = authorName.toLowerCase().replace(/\s+/g, "");
      const vanityish = hit.host.replace(/^www\./, "").includes(compact);
      if (isHomepage && !vanityish) {
        hit.confidence = Math.max(0, hit.confidence - 0.25);
        hit.reasons.push("homepage_generic_penalty");
      }
    }
  } catch { /* ignore */ }

  return hit;
}

function acceptAuthorSite(hit: WebHit, authorName: string, opts: {
  minSiteConfidence: number;
  excludePublisherSites: boolean;
}): boolean {
  if (hit.reasons.includes("blocked_domain")) return false;

  const strong =
    hit.reasons.includes("vanity_domain_match") ||
    hit.reasons.includes("schema_person_present") ||
    hit.reasons.includes("title_matches_author") ||
    hit.reasons.includes("copyright_name_match");

  const authorNamed =
    hit.reasons.includes("author_in_title_or_snippet") ||
    hit.reasons.includes("title_matches_author") ||
    hit.reasons.includes("schema_person_present");

  if (!(strong && authorNamed)) return false;

  const ps = hit.personalScore ?? 0;
  const qs = hit.publisherScore ?? 0;

  if (opts.excludePublisherSites) {
    if (isBareHomepage(hit.url)) {
      const vanityish = hit.host.replace(/^www\./, "").includes(authorName.toLowerCase().replace(/\s+/g, ""));
      if (!vanityish) return false;
    }
    if (!(ps >= 0.55 && ps >= qs + 0.20)) return false;
  } else {
    const authorPage = hasAuthorSlugPath(hit.url) && !isBareHomepage(hit.url);
    if (!(ps >= qs + 0.10 || authorPage)) return false;
  }

  return hit.confidence >= opts.minSiteConfidence;
}

function compareCandidates(a: WebHit, b: WebHit, authorName: string): number {
  const key = authorName.toLowerCase().replace(/\s+/g, "");
  const ak = a.host.replace(/^www\./, "").includes(key) ? 1 : 0;
  const bk = b.host.replace(/^www\./, "").includes(key) ? 1 : 0;
  if (ak !== bk) return bk - ak;

  const am = (a.personalScore ?? 0) - (a.publisherScore ?? 0);
  const bm = (b.personalScore ?? 0) - (b.publisherScore ?? 0);
  if (am !== bm) return bm - am;

  const as = a.reasons.includes("schema_person_present") ? 1 : 0;
  const bs = b.reasons.includes("schema_person_present") ? 1 : 0;
  if (as !== bs) return bs - as;

  const ah1 = a.reasons.includes("title_matches_author") ? 1 : 0;
  const bh1 = b.reasons.includes("title_matches_author") ? 1 : 0;
  if (ah1 !== bh1) return bh1 - ah1;

  return (b.confidence - a.confidence);
}

function pickCanonicalOrOrigin(html: string, url: string, authorName: string): string {
  const origin = originOf(url) || url;
  const m = html.match(/<link[^>]+rel=["']canonical["'][^>]*>/i)?.[0] || "";
  const href = /href=["']([^"']+)["']/i.exec(m)?.[1] || null;
  if (!href) return origin;
  const canon = isHttpUrl(href) ? href : new URL(href, origin).toString();

  const hostCanon = hostOf(canon).replace(/^www\./, "");
  const hostOrig  = hostOf(origin).replace(/^www\./, "");
  const authorKey = authorName.toLowerCase().replace(/\s+/g, "");
  const canonVanity = hostCanon.includes(authorKey);
  const origVanity  = hostOrig.includes(authorKey);

  if (canonVanity && !origVanity) return canon;
  if (canonVanity && origVanity) return canon;
  if (isBareHomepage(canon)) return origin;

  return canon;
}

/* =============================
   Query builders (Phase 2)
   ============================= */
function buildBookAuthorSiteQueries(bookTitle: string, authorName: string): string[] {
  const qBook = `"${bookTitle}"`;
  const qAuthor = `"${authorName}"`;
  const tail = "-film -movie -director -trailer";
  return [
    `${qBook} ${qAuthor} official website ${tail}`,
    `${qBook} ${qAuthor} author website ${tail}`,
    `${qBook} ${qAuthor} official site ${tail}`,
    // Late publisher-context queries (still screened by acceptance)
    `${qBook} ${qAuthor} site:penguinrandomhouse.com -film -movie -director`,
    `${qBook} ${qAuthor} site:harpercollins.com -film -movie -director`,
    `${qBook} ${qAuthor} site:simonandschuster.com -film -movie -director`,
    `${qBook} ${qAuthor} site:macmillan.com -film -movie -director`,
    `${qBook} ${qAuthor} site:hachettebookgroup.com -film -movie -director`,
  ];
}

async function searchAuthorWebsiteWithBookContext(
  authorName: string,
  bookTitle: string,
  ctx: Context,
  minSiteConfidence: number,
  debug: boolean
) {
  const queries = buildBookAuthorSiteQueries(bookTitle, authorName);
  const diag: any = { queries, candidates: [], picked: undefined };

  const limit = pLimit(CONCURRENCY);
  const items = (await Promise.all(
    queries.map(q => limit(() => (USE_SEARCH_BASE ? googleCSE(q) : [])))
  )).flat();

  const urls = items.map((it: any) => String(it.link || "")).filter(isHttpUrl);
  const dedupedUrls = dedupeByETLD1(urls);

  const prelim: WebHit[] = dedupedUrls
    .map((u) => {
      const it = items.find((x: any) => x.link === u) || { title: "", snippet: "" };
      return baseHit({ link: u, title: it.title, snippet: it.snippet });
    })
    .map(h => scoreBaseWebsiteSignals(h, authorName))
    .filter(h => (ctx.unsafeDisableDomainFilters ? true : !isBlockedHost(h.host, false)));

  const enriched = await Promise.all(prelim.map(h => limit(() => enrichWebsiteHit(h, authorName, ctx.bookTokens))));

  const accepted = enriched.filter(h => acceptAuthorSite(h, authorName, {
    minSiteConfidence,
    excludePublisherSites: ctx.excludePublisherSites,
  }));

  accepted.sort((a,b) => compareCandidates(a,b,authorName));

  if (debug) {
    diag.candidates = accepted.slice(0, 10).map(h => ({
      url: h.url, host: h.host, confidence: Number(h.confidence.toFixed(2)),
      personalScore: h.personalScore ?? 0,
      publisherScore: h.publisherScore ?? 0,
      reasons: h.reasons, typeSignals: h.typeSignals
    }));
    if (accepted[0]) diag.picked = {
      url: accepted[0].url,
      confidence: Number(accepted[0].confidence.toFixed(2)),
      personalScore: accepted[0].personalScore ?? 0,
      publisherScore: accepted[0].publisherScore ?? 0,
      reasons: accepted[0].reasons,
      typeSignals: accepted[0].typeSignals
    };
  }

  return { best: accepted[0] || null, _diag: debug ? diag : undefined };
}

async function searchAuthorWebsite(
  authorName: string,
  ctx: Context,
  minSiteConfidence: number,
  debug: boolean
) {
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

  const urls = allItems.map((it: any) => String(it.link || "")).filter(isHttpUrl);
  const dedupedUrls = dedupeByETLD1(urls);

  const prelim: WebHit[] = dedupedUrls
    .map((u) => {
      const it = allItems.find((x: any) => x.link === u) || { title: "", snippet: "" };
      return baseHit({ link: u, title: it.title, snippet: it.snippet });
    })
    .map(h => scoreBaseWebsiteSignals(h, authorName));

  const filtered = prelim.filter((h) =>
    ctx.unsafeDisableDomainFilters ? true : !isBlockedHost(h.host, false)
  );

  const enriched = await Promise.all(filtered.map((h) => limit(() => enrichWebsiteHit(h, authorName, ctx.bookTokens))));

  const accepted = enriched.filter(h => acceptAuthorSite(h, authorName, {
    minSiteConfidence,
    excludePublisherSites: ctx.excludePublisherSites,
  }));

  accepted.sort((a,b) => compareCandidates(a,b,authorName));

  if (debug) diag.candidates = accepted.slice(0, 10).map((h) => ({
    url: h.url, host: h.host, confidence: Number(h.confidence.toFixed(2)),
    personalScore: h.personalScore ?? 0,
    publisherScore: h.publisherScore ?? 0,
    reasons: h.reasons, typeSignals: h.typeSignals
  }));

  const best = accepted[0] || null;
  if (debug && best) diag.picked = {
    url: best.url, confidence: Number(best.confidence.toFixed(2)),
    personalScore: best.personalScore ?? 0, publisherScore: best.publisherScore ?? 0,
    reasons: best.reasons, typeSignals: best.typeSignals
  };

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
    const excludePublisherSites: boolean = body.exclude_publisher_sites !== false; // default true-ish

    const includeSearchRequested: boolean = body.include_search !== false; // default true
    const USE_SEARCH = includeSearchRequested && USE_SEARCH_BASE;
    res.setHeader("x-use-search", String(USE_SEARCH));
    res.setHeader("x-cse-id-present", String(!!CSE_ID));
    res.setHeader("x-cse-key-present", String(!!CSE_KEY));

    const bookTokens = tokens(bookTitle);
    const ctx: Context = {
      bookTitle,
      bookTokens,
      unsafeDisableDomainFilters,
      allowEstateSites,
      excludePublisherSites,
    };

    const cacheKey = makeCacheKey({
      bookTitle,
      minAuthorConfidence,
      minSiteConfidence,
      USE_SEARCH,
      unsafeDisableDomainFilters,
      allowEstateSites,
      excludePublisherSites,
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

    const authorName = sanitizeAuthorName(authorBest.name);

    // Pub year (best-effort, OpenLibrary author-aware)
    const pubYear = await tryOpenLibraryPubYear(bookTitle, authorName);

    // Life dates & viability
    const life = await getAuthorLifeDatesAndOfficial(authorName);
    const { viable, reason } = evaluateAuthorViability(
      life.birthYear ?? null,
      life.deathYear ?? null,
      life.hasOfficialLink,
      pubYear ?? null,
      ctx.allowEstateSites
    );

    if (!viable) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorName,
        pub_year: pubYear ?? null,
        life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
        author_viable: false,
        viability_reason: reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: Number(authorBest.score.toFixed(2)),
        source: "web",
        _diag: debug ? { author: authorDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason } } : undefined,
      };
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    /* ---------- Phase 2 pre-step: Wikidata fast-path ---------- */
    let fastDiag: any = undefined;
    let siteBest: WebHit | null = null;

    try {
      const wd = await lookupWikidataOfficialSite(authorName);
      if (wd) {
        const origin = originOf(wd) ?? wd;
        const resp = await fetchWithTimeout(origin);
        if (resp?.ok) {
          const html = await resp.text();
          const cls = classifySiteType(html, origin, authorName, ctx.bookTokens);
          const okName =
            jaccard(tokens(html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1] ?? ""), tokens(authorName)) >= 0.35 ||
            jaccard(tokens(html.match(/<h1[^>]*>([^<]*)<\/h1>/i)?.[1] ?? ""), tokens(authorName)) >= 0.35;

          const confidence = Math.min(1, 0.7 + (cls.personalScore - cls.publisherScore) * 0.3);
          if (cls.personalScore >= 0.55 && cls.personalScore >= cls.publisherScore + 0.20 && okName) {
            siteBest = {
              title: "",
              url: origin,
              host: hostOf(origin),
              confidence,
              reasons: ["wikidata_fastpath", "classifier_personal"],
              personalScore: cls.personalScore,
              publisherScore: cls.publisherScore,
              typeSignals: cls.signals,
            };
          }
          fastDiag = { wikidata: wd, cls, accepted: !!siteBest };
        }
      }
    } catch {
      /* ignore */
    }

    /* ---------- Phase 2: Search candidates ---------- */
    const siteDiagAll: any = {};
    if (!siteBest || siteBest.confidence < minSiteConfidence) {
      const fast = await searchAuthorWebsiteWithBookContext(authorName, bookTitle, ctx, minSiteConfidence, debug);
      if (fast.best) siteBest = fast.best;
      siteDiagAll.book_author = fast._diag;
    }
    if (!siteBest || siteBest.confidence < minSiteConfidence) {
      const generic = await searchAuthorWebsite(authorName, ctx, minSiteConfidence, debug);
      if (!siteBest || (generic.best && generic.best.confidence > siteBest.confidence)) {
        siteBest = generic.best;
      }
      siteDiagAll.generic = generic._diag;
    }

    if (!siteBest || siteBest.confidence < minSiteConfidence) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorName,
        pub_year: pubYear ?? null,
        life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
        author_viable: true,
        viability_reason: reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: siteBest ? Number(siteBest.confidence.toFixed(2)) : 0,
        author_confidence: Number(authorBest.score.toFixed(2)),
        source: "web",
        _diag: debug ? { author: authorDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason }, wikidata: fastDiag, site: siteDiagAll } : undefined,
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
        canonical = pickCanonicalOrOrigin(html, origin, authorName);
      }
    } catch { /* ignore */ }

    const payload: CacheValue = {
      book_title: bookTitle,
      inferred_author: authorName,
      pub_year: pubYear ?? null,
      life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
      author_viable: true,
      viability_reason: reason,
      author_url: origin,
      site_title: siteTitle,
      canonical_url: canonical || origin,
      confidence: Number(siteBest.confidence.toFixed(2)),
      author_confidence: Number(authorBest.score.toFixed(2)),
      source: "web",
      _diag: debug ? { author: authorDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason }, wikidata: fastDiag, site: siteDiagAll, chosen: { ...siteBest } } : undefined,
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
