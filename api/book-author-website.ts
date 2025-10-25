// api/book-author-website.ts
/// <reference types="node" />

/**
 * POST Body:
 * {
 *   "book_title": "The Art of War",
 *   "include_search": true,                 // default true (requires GOOGLE_CSE_* env)
 *   "min_author_confidence": 0.55,          // optional 0..1
 *   "min_site_confidence": 0.55,            // optional 0..1
 *   "allow_estate_sites": false,            // optional, default false
 *   "debug": true,                          // optional
 *   "unsafe_disable_domain_filters": false  // optional
 * }
 *
 * Returns the author's official website ONLY if the author is viable for a personal site.
 * Viability now integrates original publication year if life dates are missing.
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
const USER_AGENT = "BookAuthorWebsite/1.2 (+https://example.com)";
const CONCURRENCY = 4;

const DEFAULT_MIN_AUTHOR_CONFIDENCE = 0.55;
const DEFAULT_MIN_SITE_CONFIDENCE = 0.55;

/* ====== Google CSE config ====== */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_ID = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "";
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
  if (!resp?.ok) {
    const text = await resp?.text?.();
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  return Array.isArray(data?.items) ? data.items : [];
}

/* =============================
   Phase 1: Discover the author from the book title
   ============================= */
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

  const queries = [
    `${quoted} author`,
    `${quoted} book author`,
    `${quoted} novel author`,
    `${quoted} site:wikipedia.org`,
    `${quoted} site:books.google.com`,
    `${quoted} site:goodreads.com`,
  ];

  const diag: any = { queries, hits: [], candidates: [] as any[], picked: undefined };

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

  const ranked = Array.from(authorScores.values()).sort((a, b) => b.score - a.score);
  if (debug) diag.candidates = ranked.slice(0, 10);

  const best = ranked[0] || null;
  if (debug) diag.picked = best;
  return { best, _diag: debug ? diag : undefined };
}

/* =============================
   Phase 1a: Discover original publication year
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

function scorePubYearCandidate(host: string, year: number, ctx: Context): PubYearHit["score"] {
  let score = 0;
  const now = new Date().getFullYear();

  // Trusted source boosts
  if (PUBYEAR_SOURCES.some((d) => host.includes(d))) score += 0.4;

  // Recency sanity: a plausible original year is <= now and not a "future" year
  if (year <= now) score += 0.1;

  // Prefer "older" earliest mentions from snippets (original ≠ latest edition)
  if (year < now - 1) score += 0.05;

  return Math.max(0, Math.min(1, score));
}

async function discoverOriginalPubYear(bookTitle: string, ctx: Context, debug: boolean) {
  const quoted = `"${bookTitle}"`;
  const queries = [
    `${quoted} original publication date`,
    `${quoted} first published`,
    `${quoted} publication date`,
    `${quoted} site:books.google.com`,
    `${quoted} site:openlibrary.org`,
    `${quoted} site:worldcat.org`,
    `${quoted} site:goodreads.com`,
    `${quoted} site:wikipedia.org`,
  ];

  const diag: any = { queries, hits: [] as any[], candidates: [] as any[], picked: undefined };

  const limit = pLimit(CONCURRENCY);
  const items = (
    await Promise.all(
      queries.map((q) => limit(async () => (USE_SEARCH_BASE ? await googleCSE(q) : [])))
    )
  ).flat();

  const candidates: PubYearHit[] = [];
  for (const it of items) {
    const url = String(it.link || "");
    if (!isHttpUrl(url)) continue;
    const host = hostOf(url);
    const title = String(it.title || "");
    const snippet = String(it.snippet || it.htmlSnippet || "");

    const years = new Set<number>([
      ...yearCandidatesFromText(title),
      ...yearCandidatesFromText(snippet),
    ]);

    for (const y of years) {
      const sc = scorePubYearCandidate(host, y, ctx);
      if (sc > 0) {
        candidates.push({
          url,
          host,
          year: y,
          score: sc,
          reasons: [
            ...(PUBYEAR_SOURCES.some((d) => host.includes(d)) ? ["trusted_pub_source"] : []),
          ],
        });
      }
    }

    if (debug) diag.hits.push({ title, url, host });
  }

  // Heuristic: pick the **earliest** year among top-scored candidates to approximate original publication
  candidates.sort((a, b) => b.score - a.score || a.year - b.year);
  let best = candidates[0] || null;

  if (best) {
    const topScore = best.score;
    const nearTop = candidates.filter((c) => Math.abs(c.score - topScore) <= 0.05);
    const earliestNearTop = nearTop.sort((a, b) => a.year - b.year)[0] || best;
    best = earliestNearTop;
  }

  if (debug) {
    diag.candidates = candidates.slice(0, 12);
    diag.picked = best;
  }

  return { best, _diag: debug ? diag : undefined };
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

/**
 * Evaluate viability using:
 * 1) Wikipedia life dates & official link (if available)
 * 2) Original publication year (if life dates missing)
 * 3) Estate policy
 */
function evaluateAuthorViability(
  birthYear: number | null | undefined,
  deathYear: number | null | undefined,
  hasOfficialLinkOnWiki: boolean,
  pubYear: number | null | undefined,
  allowEstateSites: boolean
): { viable: boolean; reason: string } {
  const nowYear = new Date().getFullYear();

  // If Wikipedia explicitly lists an Official website, accept (covers living and some deceased authors).
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
      // 1950–1994: possible living author, but stricter — require strong site signals later
      return { viable: true, reason: "pubyear_1950_1994_cautious" };
    } else {
      // 1995+: very likely a living/modern author
      return { viable: true, reason: "pubyear_ge_1995_likely_living" };
    }
  }

  // Nothing definitive: be conservative (do not proceed)
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
}> {
  try {
    const res = await fetchWithTimeout(url);
    if (!res?.ok) {
      return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false, estateWording: false };
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

    return { titleTokens: tt, hasCopyrightName, hasSchemaPerson, hasOfficialWords, bookMentioned, estateWording };
  } catch {
    return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false, estateWording: false };
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
  }

  return hit;
}

async function searchAuthorWebsite(authorName: string, ctx: Context, debug: boolean) {
  const quotedAuthor = `"${authorName}"`;

  const queries = [
    `${quotedAuthor} official site`,
    `${quotedAuthor} official website`,
    `${quotedAuthor} author website`,
    `${quotedAuthor} site`,
    `${quotedAuthor} website`,
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
    if (ctx.allowEstateSites) return true;
    return !h.reasons.includes("estate_wording_detected");
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
        inferred_author: authorBest?.name,
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

    /* ---------- Phase 1a: Original publication year ---------- */
    const { best: pubBest, _diag: pubDiag } = await discoverOriginalPubYear(bookTitle, ctx, debug);
    const pubYear = pubBest?.year ?? null;

    /* ---------- Phase 1b: Viability gate ---------- */
    const life = await getAuthorLifeDatesAndOfficial(authorBest.name);
    const { viable, reason } = evaluateAuthorViability(
      life.birthYear ?? null,
      life.deathYear ?? null,
      life.hasOfficialLink,
      pubYear,
      ctx.allowEstateSites
    );

    if (!viable) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorBest.name,
        pub_year: pubYear,
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
    const { best: siteBest, _diag: siteDiag } = await searchAuthorWebsite(authorBest.name, ctx, debug);

    if (!siteBest || siteBest.confidence < minSiteConfidence) {
      const payload: CacheValue = {
        book_title: bookTitle,
        inferred_author: authorBest.name,
        pub_year: pubYear,
        life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
        author_viable: true,
        viability_reason: reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: siteBest ? Number(siteBest.confidence.toFixed(2)) : 0,
        author_confidence: Number(authorBest.score.toFixed(2)),
        source: "web",
        _diag: debug ? { author: authorDiag, pubyear: pubDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason }, site: siteDiag } : undefined,
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
      inferred_author: authorBest.name,
      pub_year: pubYear,
      life_dates: { birthYear: life.birthYear ?? null, deathYear: life.deathYear ?? null },
      author_viable: true,
      viability_reason: reason,
      author_url: origin,
      site_title: siteTitle,
      canonical_url: canonical || origin,
      confidence: Number(siteBest.confidence.toFixed(2)),
      author_confidence: Number(authorBest.score.toFixed(2)),
      source: "web",
      _diag: debug ? { author: authorDiag, pubyear: pubDiag, viability: { wikiUrl: life.wikiUrl, hasOfficialLink: life.hasOfficialLink, reason }, site: siteDiag } : undefined,
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
