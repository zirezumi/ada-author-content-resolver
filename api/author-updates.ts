// api/author-updates.ts
/// <reference types="node" />

import Parser from "rss-parser";
import pLimit from "p-limit";

// Ensure Node runtime on Vercel (NOT edge)
export const config = { runtime: "nodejs" } as const;

/* =========================================
   Auth
   ========================================= */
// Comma-separated secrets allowed: "s1,s2"
const AUTH_SECRETS = (process.env.AUTHOR_UPDATES_SECRET || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// In local dev you can set SKIP_AUTH=true to bypass auth checks.
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
    res
      .status(500)
      .json({ error: "server_misconfigured: missing AUTHOR_UPDATES_SECRET" });
    return false;
  }
  const provided = (headerCI(req, "x-auth") || "").trim();
  const ok = provided && AUTH_SECRETS.includes(provided);
  if (!ok) {
    res.status(401).json({ error: "unauthorized" });
    return false;
  }
  return true;
}

/* =========================================
   Tunables
   ========================================= */
const LOOKBACK_DEFAULT = 30; // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 180; // allow wider windows if desired
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 3000; // per network call (tight)
const USER_AGENT = "AuthorUpdates/1.7 (+https://example.com)";
const CONCURRENCY = 4;

// CSE constraints
const SEARCH_NUM = 10; // Google caps at 10
const SEARCH_PAGES = 3; // 1..30 results (tweak to 4 if needed)
const CSE_BASE = "https://www.googleapis.com/customsearch/v1";

// Identity / acceptance thresholds
const DEFAULT_MIN_CONFIDENCE = 0.5;
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.5;

// Candidate scan limits
const MAX_CANDIDATES_HTML_VERIFY = 5; // fetch page text for top N only
const MAX_AUTHOR_SITE_OUTLINKS = 12; // max outlinks to verify from author site

// Blocklists
const SOCIAL_HOSTS = [
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "youtube.com",
  "youtu.be",
  "threads.net",
  "bluesky.social",
];

const SHOPPING_HOSTS = [
  "amazon.com",
  "amazon.co.uk",
  "amazon.de",
  "amazon.es",
  "amazon.fr",
  "amazon.ca",
  "amazon.com.au",
  "bookshop.org",
  "barnesandnoble.com",
  "audible.com",
  "goodreads.com", // often book meta, not author-generated content
  "play.google.com",
  "apps.apple.com",
  "itunes.apple.com",
  "music.apple.com",
];

const AUDIO_PODCAST_HOSTS = [
  "open.spotify.com",
  "podcasts.apple.com",
  "rss.art19.com",
  "simplecast.com",
  "omny.fm",
  "megaphone.fm",
  "soundcloud.com",
];

const GENERIC_BLOCKED_HOSTS = [
  "substack.com", // (you can remove if you want substack results)
];

const BLOCKED_DOMAINS = new Set<string>([
  ...SOCIAL_HOSTS,
  ...SHOPPING_HOSTS,
  ...AUDIO_PODCAST_HOSTS,
  ...GENERIC_BLOCKED_HOSTS,
]);

// For “news-ish” follow-through from author site:
const NEWSY_HINTS = [
  "guardian.com",
  "ft.com",
  "nytimes.com",
  "economist.com",
  "newyorker.com",
  "washingtonpost.com",
  "theatlantic.com",
  "bloomberg.com",
  "bbc.co.uk",
  "bbc.com",
  "time.com",
  "wired.com",
  "vox.com",
  "latimes.com",
  "wsj.com",
]; // only as hints, not strict whitelist

/* =========================================
   Types
   ========================================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string; // "web" | "rss" etc.
  author_url?: string;
  _debug?: Array<{
    feedUrl: string; // or page URL in web path
    ok: boolean;
    source: string;
    latest: string | null;
    recentWithinWindow: boolean;
    confidence: number;
    reason: string[];
    siteTitle?: string | null;
    feedTitle?: string | null;
    error: string | null;
  }>;
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type PlatformHints = {
  platform?: "substack" | "medium" | "wordpress" | "ghost" | "blogger";
  handle?: string;
  feed_url?: string;
  site_url?: string;
};

type WebHit = {
  title: string;
  url: string;
  snippet?: string;
  publishedAt?: string; // ISO if provided by CSE
  host: string;
  confidence: number; // 0..1 after scoring
  reason: string[];
  isLikelyAuthorSite?: boolean;
};

/* =========================================
   Cache
   ========================================= */
const CACHE = new Map<string, CacheEntry>();
function makeCacheKey(params: Record<string, unknown>) {
  return Object.entries(params)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${k}=${typeof v === "string" ? v.toLowerCase() : JSON.stringify(v)}`)
    .join("|");
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

/* =========================================
   Utilities
   ========================================= */
const parser = new Parser();
function daysAgo(d: Date) {
  return (Date.now() - d.getTime()) / 86_400_000;
}
function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}
function hostOf(u: string) {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}
function isHttpUrl(s: string) {
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}
function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s.'-]/gu, " ")
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
function containsAll(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.every((n) => H.has(n));
}
function stripHtml(html: string): string {
  // very light strip
  return html
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, " ")
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<\/?[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
function extractMetaContent(html: string, prop: RegExp): string | null {
  const m = html.match(
    new RegExp(
      `<meta[^>]+(?:name|property)=["']${prop.source}["'][^>]+content=["']([^"']+)["']`,
      "i"
    )
  );
  return m?.[1] ?? null;
}
function extractPublishDateFromHtml(html: string): string | undefined {
  // common meta tags
  const tags = [
    /article:published_time/i,
    /og:updated_time/i,
    /date/i,
    /publishdate/i,
    /dc.date/i,
    /dc.date.issued/i,
  ];
  for (const t of tags) {
    const v = extractMetaContent(html, t);
    if (v) {
      const d = new Date(v);
      if (!isNaN(d.getTime())) return d.toISOString();
    }
  }
  // Schema.org JSON-LD quick scrape (very light)
  const ld = html.match(/<script[^>]*application\/ld\+json[^>]*>([\s\S]*?)<\/script>/i);
  if (ld?.[1]) {
    try {
      const obj = JSON.parse(ld[1]);
      const tryKeys = ["datePublished", "dateModified", "uploadDate"];
      const pick = (o: any): string | undefined => {
        if (!o || typeof o !== "object") return undefined;
        for (const k of tryKeys) {
          if (o[k]) {
            const d = new Date(o[k]);
            if (!isNaN(d.getTime())) return d.toISOString();
          }
        }
        // array or graph
        if (Array.isArray(o)) {
          for (const el of o) {
            const got = pick(el);
            if (got) return got;
          }
        }
        if (Array.isArray(o["@graph"])) {
          for (const el of o["@graph"]) {
            const got = pick(el);
            if (got) return got;
          }
        }
        return undefined;
      };
      const out = pick(obj);
      if (out) return out;
    } catch {
      // ignore
    }
  }
  return undefined;
}
async function fetchWithTimeout(
  url: string,
  init?: RequestInit,
  ms = FETCH_TIMEOUT_MS
): Promise<Response | null> {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, ...(init?.headers || {}) },
    });
    return res;
  } catch {
    return null;
  } finally {
    clearTimeout(id);
  }
}
async function fetchPageText(url: string): Promise<{ text: string; html: string } | null> {
  const r = await fetchWithTimeout(url);
  if (!r || !r.ok) return null;
  const html = await r.text();
  const text = stripHtml(html).toLowerCase();
  return { text, html };
}
function extractOutboundLinks(html: string, base: string): string[] {
  const out: string[] = [];
  const re = /<a\s[^>]*href=["']([^"']+)["'][^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    const href = m[1];
    try {
      const abs = new URL(href, base).toString();
      if (isHttpUrl(abs)) out.push(abs);
    } catch {
      /* ignore */
    }
  }
  return Array.from(new Set(out));
}

/* =========================================
   Search + Scoring
   ========================================= */
function isBlockedHost(host: string): boolean {
  return BLOCKED_DOMAINS.has(host);
}
function looksLikeNewsHost(host: string): boolean {
  if (isBlockedHost(host)) return false;
  // use hints to prioritize scan on author site outlinks
  return NEWSY_HINTS.some((h) => host.endsWith(h));
}
function pageContainsSignals(
  bodyText: string,
  authorName: string,
  bookTitle: string | undefined
): { author: boolean; book: boolean; cooccur: boolean } {
  const t = bodyText;
  const aTokens = tokens(authorName);
  const hasAuthor = containsAll(tokens(t), aTokens) || jaccard(tokens(t), aTokens) >= 0.35;

  let hasBook = false;
  let cooccur = false;

  if (bookTitle) {
    const bTokens = tokens(bookTitle);
    const bodyToks = tokens(t);
    hasBook = containsAll(bodyToks, bTokens) || jaccard(bodyToks, bTokens) >= 0.25;

    // co-occurrence window—very light: check if both appear anywhere (we already did),
    // then cheap adjacency heuristic: the joined lowercase strings contain both within ~200 chars.
    if (hasAuthor && hasBook) {
      const ao = t.indexOf(aTokens[0] ?? "");
      const bo = t.indexOf(bTokens[0] ?? "");
      if (ao >= 0 && bo >= 0 && Math.abs(ao - bo) <= 2000) cooccur = true;
    }
  }
  return { author: hasAuthor, book: hasBook, cooccur };
}
function scoreWebHitBasic(hit: WebHit, authorName: string, bookTitle?: string): WebHit {
  const reasons: string[] = [];
  let score = 0;

  const titleTokens = tokens(hit.title || "");
  const snippetTokens = tokens(hit.snippet || "");
  const combined = Array.from(new Set([...titleTokens, ...snippetTokens]));
  const aToks = tokens(authorName);
  const bToks = bookTitle ? tokens(bookTitle) : [];

  // blocklist
  if (isBlockedHost(hit.host)) {
    hit.confidence = 0;
    hit.reason = ["blocked_host"];
    return hit;
  }

  // Author presence (title or snippet)
  const authorMatch =
    aToks.length &&
    (jaccard(aToks, combined) >= 0.3 || containsAll(combined, aToks));
  if (authorMatch) {
    score += 0.35;
    reasons.push("author_in_title_or_snippet");
  }

  // Book presence (title or snippet)
  let bookMatch = false;
  if (bToks.length) {
    bookMatch = jaccard(bToks, combined) >= 0.25 || containsAll(combined, bToks);
    if (bookMatch) {
      score += 0.3;
      reasons.push("book_in_title_or_snippet");
    }
  }

  // “Conversation / interview” hints bonus
  const convoHints = ["interview", "conversation", "q&a", "in conversation"];
  const hasConvoHint = convoHints.some((h) => combined.includes(h));
  if (hasConvoHint) {
    score += 0.15;
    reasons.push("conversation_hint");
  }

  // Treat official site as plausible source
  if (!bookTitle) {
    // if no book is required, official site can be fine
    if (/official|home|website/.test(hit.title.toLowerCase())) {
      score += 0.15;
      hit.isLikelyAuthorSite = true;
      reasons.push("official_site_title_hint");
    }
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

/* =========================================
   Google CSE (multi-page)
   ========================================= */
async function cseQuery(
  q: string,
  dateRestrict: string,
  start: number
): Promise<any> {
  const url = new URL(CSE_BASE);
  url.searchParams.set("key", process.env.GOOGLE_CSE_KEY || "");
  url.searchParams.set("cx", process.env.GOOGLE_CSE_CX || "");
  url.searchParams.set("q", q);
  url.searchParams.set("dateRestrict", dateRestrict);
  url.searchParams.set("num", String(SEARCH_NUM));
  if (start > 1) url.searchParams.set("start", String(start));
  const resp = await fetchWithTimeout(url.toString());
  if (!resp || !resp.ok) return null;
  try {
    return await resp.json();
  } catch {
    return null;
  }
}

async function googleSearchHits(
  authorName: string,
  bookTitle: string | undefined,
  lookbackDays: number
): Promise<WebHit[]> {
  if (!process.env.GOOGLE_CSE_KEY || !process.env.GOOGLE_CSE_CX) return [];

  // Build queries (quoted + relaxed)
  const excluded =
    "-site:x.com -site:twitter.com -site:facebook.com -site:instagram.com -site:tiktok.com -site:linkedin.com -site:youtube.com -site:amazon.com -site:open.spotify.com -site:apple.com -site:podcasts.apple.com -site:substack.com";
  const convo = '(interview OR "in conversation" OR "Q&A" OR conversation)';

  const qStrict = bookTitle
    ? `"${authorName}" "${bookTitle}" ${convo} ${excluded}`
    : `"${authorName}" ${convo} ${excluded}`;

  const qRelaxed = bookTitle
    ? `"${authorName}" ${bookTitle} ${convo} ${excluded}`
    : `"${authorName}" ${convo} ${excluded}`;

  const dateRestrict = lookbackDays <= 7 ? "d7" : lookbackDays <= 30 ? "d30" : "d90";

  const queries = [qStrict, qRelaxed];
  const hits: WebHit[] = [];

  for (const q of queries) {
    for (let p = 0; p < SEARCH_PAGES; p++) {
      const start = 1 + p * SEARCH_NUM;
      const data = await cseQuery(q, dateRestrict, start);
      if (!data || !Array.isArray(data.items)) continue;
      for (const it of data.items) {
        const title = String(it.name || it.title || "");
        const url = String(it.link || it.url || "");
        if (!isHttpUrl(url)) continue;
        const host = hostOf(url);
        const snippet = String(it.snippet || it.description || "");
        const publishedAt = it.pagemap?.metatags?.[0]?.["article:published_time"]
          ? new Date(it.pagemap.metatags[0]["article:published_time"]).toISOString()
          : it.pagemap?.videoobject?.[0]?.uploaddate
          ? new Date(it.pagemap.videoobject[0].uploaddate).toISOString()
          : undefined;

        hits.push(
          scoreWebHitBasic(
            { title, url, snippet, publishedAt, host, confidence: 0, reason: [] },
            authorName,
            bookTitle
          )
        );
      }
    }
  }

  // Dedup by URL
  const seen = new Set<string>();
  const deduped = hits.filter((h) => {
    if (seen.has(h.url)) return false;
    seen.add(h.url);
    return true;
  });

  // sort by confidence (desc)
  return deduped.sort((a, b) => b.confidence - a.confidence);
}

/* =========================================
   HTML verification + author-site follow-through
   ========================================= */
function acceptArticleByBody(
  t: string,
  author: string,
  bookTitle: string | undefined,
  requireBookMatch: boolean
): { pass: boolean; reason: string[] } {
  const reasons: string[] = [];
  const { author: hasA, book: hasB, cooccur } = pageContainsSignals(t, author, bookTitle);

  if (hasA) reasons.push("content_contains_author");
  if (hasB) reasons.push("content_contains_book");
  if (cooccur) reasons.push("author_book_cooccur");

  if (requireBookMatch) {
    if (hasA && hasB) return { pass: true, reason: reasons };
    return { pass: false, reason: reasons };
  } else {
    // accept author-only with some signal
    if (hasA) return { pass: true, reason: reasons };
    return { pass: false, reason: reasons };
  }
}

function withinLookback(iso?: string, limitDays?: number): boolean {
  if (!iso || !limitDays) return false;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return false;
  return daysAgo(d) <= limitDays;
}

function looksLikeOfficialAuthorSite(title: string, host: string, author: string): boolean {
  // simple heuristics: title contains name + words like "Official", or host matches name closely
  const t = title.toLowerCase();
  const a = author.toLowerCase();
  const hostBase = host.split(":")[0];

  const nameTokens = tokens(a).filter(Boolean);
  const titleTokens = tokens(t);

  const nameInTitle = containsAll(titleTokens, nameTokens) || jaccard(titleTokens, nameTokens) >= 0.4;
  const officialHint = /\bofficial\b|\bhomepage?\b|\bwebsite\b/.test(t);

  // host subdomain equals or is close to name tokens squashed
  const sub = hostBase.split(".")[0];
  const subToks = tokens(sub);

  const handleClose = jaccard(nameTokens, subToks) >= 0.45;

  return (nameInTitle && officialHint) || handleClose;
}

async function verifyTopCandidates(
  hits: WebHit[],
  author: string,
  bookTitle: string | undefined,
  lookbackDays: number,
  requireBookMatch: boolean,
  minSearchConfidence: number
): Promise<{
  accepted: null | { title: string; url: string; publishedAt?: string; authorUrl?: string; reason: string[] };
  debug: Array<{
    feedUrl: string;
    ok: boolean;
    source: string;
    latest: string | null;
    recentWithinWindow: boolean;
    confidence: number;
    reason: string[];
    siteTitle?: string | null;
    feedTitle?: string | null;
    error: string | null;
  }>;
}> {
  const debug: Array<{
    feedUrl: string;
    ok: boolean;
    source: string;
    latest: string | null;
    recentWithinWindow: boolean;
    confidence: number;
    reason: string[];
    siteTitle?: string | null;
    feedTitle?: string | null;
    error: string | null;
  }> = [];

  const candidates = hits.filter((h) => h.confidence >= minSearchConfidence);

  // 1) Direct verification on top N
  for (const h of candidates.slice(0, MAX_CANDIDATES_HTML_VERIFY)) {
    const pg = await fetchPageText(h.url);
    if (!pg) {
      debug.push({
        feedUrl: h.url,
        ok: false,
        source: "web",
        latest: null,
        recentWithinWindow: false,
        confidence: Number(h.confidence.toFixed(2)),
        reason: [...h.reason, "fetch_failed"],
        error: "fetch_failed",
      });
      continue;
    }

    // Try to infer publish date (if any)
    const pageISO = extractPublishDateFromHtml(pg.html);
    const recencyOK = pageISO ? withinLookback(pageISO, lookbackDays) : true;

    const verdict = acceptArticleByBody(pg.text, author, bookTitle, requireBookMatch);

    debug.push({
      feedUrl: h.url,
      ok: verdict.pass && recencyOK,
      source: "web",
      latest: pageISO || null,
      recentWithinWindow: recencyOK,
      confidence: Number(h.confidence.toFixed(2)),
      reason: [...h.reason, ...verdict.reason],
      error: null,
    });

    if (verdict.pass && recencyOK) {
      return {
        accepted: {
          title: h.title,
          url: h.url,
          publishedAt: pageISO,
          authorUrl: `https://${h.host}`,
          reason: [...h.reason, ...verdict.reason],
        },
        debug,
      };
    }
  }

  // 2) If any looks like official author site, follow its outlinks to find a news/interview item
  for (const h of candidates.slice(0, MAX_CANDIDATES_HTML_VERIFY)) {
    // quick check: either heuristic from earlier, or title contains author strongly
    const officialish =
      h.isLikelyAuthorSite ||
      looksLikeOfficialAuthorSite(h.title || "", h.host, author);

    if (!officialish) continue;

    const pg = await fetchPageText(h.url);
    if (!pg) continue;

    const outlinks = extractOutboundLinks(pg.html, h.url)
      .map((u) => ({ url: u, host: hostOf(u) }))
      .filter((o) => o.host && !isBlockedHost(o.host));

    // Prioritize “newsy” hosts first
    const prioritized = [
      ...outlinks.filter((o) => looksLikeNewsHost(o.host)),
      ...outlinks.filter((o) => !looksLikeNewsHost(o.host)),
    ].slice(0, MAX_AUTHOR_SITE_OUTLINKS);

    for (const o of prioritized) {
      const subPg = await fetchPageText(o.url);
      if (!subPg) continue;

      const pageISO = extractPublishDateFromHtml(subPg.html);
      const recencyOK = pageISO ? withinLookback(pageISO, lookbackDays) : true;
      const verdict = acceptArticleByBody(subPg.text, author, bookTitle, requireBookMatch);

      debug.push({
        feedUrl: o.url,
        ok: verdict.pass && recencyOK,
        source: "web",
        latest: pageISO || null,
        recentWithinWindow: recencyOK,
        confidence: Number(h.confidence.toFixed(2)),
        reason: ["author_site_follow_through", ...verdict.reason],
        error: null,
      });

      if (verdict.pass && recencyOK) {
        return {
          accepted: {
            title: "", // unknown without another fetch of <title>, optional
            url: o.url,
            publishedAt: pageISO,
            authorUrl: `https://${o.host}`,
            reason: ["author_site_follow_through", ...verdict.reason],
          },
          debug,
        };
      }
    }
  }

  return { accepted: null, debug };
}

/* =========================================
   CORS + Handler
   ========================================= */
function cors(res: any, origin?: string) {
  res.setHeader("Access-Control-Allow-Origin", origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Auth");
}

export default async function handler(req: any, res: any) {
  try {
    cors(res, req.headers?.origin);
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "POST required" });
    if (!requireAuth(req, res)) return;

    const bodyRaw = req.body ?? {};
    const body = typeof bodyRaw === "string" ? JSON.parse(bodyRaw || "{}") : bodyRaw;

    const rawAuthor = (body.author_name ?? "").toString();
    const author = rawAuthor.normalize("NFKC").replace(/\s+/g, " ").trim();
    if (!author) return res.status(400).json({ error: "author_name required" });

    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    const bookTitle: string | undefined =
      typeof body.book_title === "string" && body.book_title.trim() ? body.book_title.trim() : undefined;

    const strictAuthorMatch: boolean = body.strict_author_match === true;
    const requireBookMatch: boolean = body.require_book_match === true; // require book to appear on page
    const fallbackAuthorOnly: boolean = body.fallback_author_only === true; // allow author-only if book missing
    const minConfidence: number =
      typeof body.min_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_confidence))
        : DEFAULT_MIN_CONFIDENCE;

    const includeSearch: boolean = body.include_search !== false; // default true
    const minSearchConfidence: number =
      typeof body.min_search_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_search_confidence))
        : DEFAULT_MIN_SEARCH_CONFIDENCE;

    const debug: boolean = body.debug === true;

    // Build cache key
    const cacheKey = makeCacheKey({
      author,
      lookback,
      bookTitle: bookTitle || "",
      strictAuthorMatch,
      requireBookMatch,
      fallbackAuthorOnly,
      minConfidence,
      includeSearch,
      minSearchConfidence,
      version: "1.7",
    });

    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // ===== Web search path (single-call approach) =====
    if (includeSearch && process.env.GOOGLE_CSE_KEY && process.env.GOOGLE_CSE_CX) {
      const hits = await googleSearchHits(author, bookTitle, lookback);

      // Apply strict author gate on hit-level confidence
      const filtered = hits.filter((h) => h.confidence >= minSearchConfidence);

      // Try direct verify + author-site follow-through
      const { accepted, debug: dbg } = await verifyTopCandidates(
        filtered,
        author,
        bookTitle,
        lookback,
        /* requireBookMatch = */ requireBookMatch && !fallbackAuthorOnly ? true : !!bookTitle && requireBookMatch,
        minSearchConfidence
      );

      if (accepted) {
        const payload: CacheValue = {
          author_name: author,
          has_recent: true,
          latest_title: accepted.title || undefined,
          latest_url: accepted.url,
          published_at: accepted.publishedAt,
          source: "web",
          author_url: accepted.authorUrl,
        };
        if (debug) payload._debug = dbg;
        setCached(cacheKey, payload);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      }

      // If we didn’t accept any, but fallbackAuthorOnly is allowed, we can relax book requirement
      if (fallbackAuthorOnly && bookTitle) {
        const { accepted: relaxAccepted, debug: dbg2 } = await verifyTopCandidates(
          filtered,
          author,
          /* bookTitle */ undefined, // ignore book
          lookback,
          /* requireBookMatch */ false,
          minSearchConfidence
        );
        if (relaxAccepted) {
          const payload: CacheValue = {
            author_name: author,
            has_recent: true,
            latest_title: relaxAccepted.title || undefined,
            latest_url: relaxAccepted.url,
            published_at: relaxAccepted.publishedAt,
            source: "web",
            author_url: relaxAccepted.authorUrl,
          };
          if (debug) payload._debug = dbg2;
          setCached(cacheKey, payload);
          res.setHeader("x-cache", "MISS");
          return res.status(200).json(payload);
        }
        if (debug) {
          // keep last debug if available
          const empty: CacheValue = { author_name: author, has_recent: false, _debug: dbg2 };
          setCached(cacheKey, empty);
          res.setHeader("x-cache", "MISS");
          return res.status(200).json({ error: "no_confident_author_match" });
        }
      }

      if (debug) {
        const empty: CacheValue = { author_name: author, has_recent: false, _debug: dbg };
        setCached(cacheKey, empty);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json({ error: "no_confident_author_match" });
      }

      return res.status(200).json({ error: "no_confident_author_match" });
    }

    // If search disabled/misconfigured
    return res.status(500).json({ error: "search_unavailable" });
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
