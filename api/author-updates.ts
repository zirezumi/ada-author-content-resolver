// api/author-updates.ts
/// <reference types="node" />

/**
 * Author Updates Resolver
 * - Single-call flow using Google CSE (no Bing, no press feeds)
 * - Multi-variant queries (author+book, interview terms, author-only interview)
 * - Paginated CSE (up to 40 results) + query-time -site: exclusions
 * - Page fetch & validation: article-like check, byline/author signals, book signals, recency
 * - Block social/shopping/podcast/homepage/index pages
 * - 24h in-memory cache
 * - X-Auth secret header support
 */

export const config = { runtime: "nodejs" } as const;

/* =============================
   Auth
   ============================= */
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
    res.status(500).json({ error: "server_misconfigured: missing AUTHOR_UPDATES_SECRET" });
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

/* =============================
   Tunables
   ============================= */
const LOOKBACK_DEFAULT = 30; // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 365;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 6500;
const USER_AGENT = "AuthorUpdates/2.0 (+https://example.com)";

// Google CSE
const USE_SEARCH = !!(process.env.GOOGLE_CSE_KEY && process.env.GOOGLE_CSE_CX);
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_CX = process.env.GOOGLE_CSE_CX || "";
const SEARCH_MAX_RESULTS = 20;  // results per page
const SEARCH_PAGES = 2;         // pages to fetch (2*20 = 40 results)

// Domain/host parts to exclude (query-time -site: AND runtime checks)
// Adjust to your taste.
const BLOCKED_HOST_PARTS = [
  // Socials
  "x.com", "twitter.com", "instagram.com", "facebook.com", "tiktok.com",
  "linkedin.com", "youtube.com", "youtu.be",
  // Podcasts/audio
  "podcasts.apple.com", "music.apple.com", "open.spotify.com", "spotify.com",
  // Shopping/catalog
  "amazon.", "amazon.com", "amazon.co", "barnesandnoble.com", "bookshop.org",
  // Misc community/UGC (often not authored pieces)
  "goodreads.com",
  // Optional: sometimes authors publish on Substack; block or allow based on preference
  "substack.com"
];

// Paths that often indicate non-article
const BLOCKED_PATH_HINTS = [
  "/store", "/shop", "/product", "/products", "/cart", "/tag/", "/tags/", "/topic/", "/topics/", "/category/", "/categories/"
];

// Accept if title contains these signals (for interviews/features)
const FEATURE_TITLE_RE = /\b(interview|q&a|in conversation|conversation|in-conversation|talks with|a conversation with)\b/i;

// Minimum text length to treat page as article-like (rough heuristic)
const MIN_ARTICLE_TEXT_CHARS = 700;

// Acceptance error messages
const ERR_NO_CONFIDENT = { error: "no_confident_author_match" } as const;

/* =============================
   Types
   ============================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string;     // "web"
  author_url?: string; // host root of accepted result
  _debug?: Array<{
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
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type WebHit = {
  title: string;
  url: string;
  snippet?: string;
  publishedAt?: string; // meta-derived; may be undefined
  host: string;
  confidence: number;   // 0..1
  reason: string[];
};

type PageCheck = {
  ok: boolean;
  reason: string[];
  publishedAt?: string; // ISO
  title?: string;
  textLen?: number;
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();
function makeCacheKey(body: Record<string, unknown>): string {
  // shallow, order-stable key
  const allow = [
    "author_name", "book_title", "lookback_days",
    "include_search", "strict_author_match",
    "require_book_match", "fallback_author_only",
    "min_confidence", "min_search_confidence", "debug"
  ];
  const o: Record<string, unknown> = {};
  for (const k of allow) o[k] = body[k];
  return JSON.stringify(o);
}
function getCached(key: string): CacheValue | null {
  const hit = CACHE.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) { CACHE.delete(key); return null; }
  return hit.value;
}
function setCached(key: string, value: CacheValue) {
  CACHE.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
}

/* =============================
   Utilities
   ============================= */
function daysAgo(d: Date) { return (Date.now() - d.getTime()) / 86_400_000; }
function clamp(n: number, min: number, max: number) { return Math.max(min, Math.min(max, n)); }
function normalizeAuthor(s: string) { return s.normalize("NFKC").replace(/\s+/g, " ").trim(); }
function hostOf(u: string) { try { return new URL(u).host.toLowerCase(); } catch { return ""; } }
function originOf(u: string) { try { return new URL(u).origin; } catch { return ""; } }
function pathnameOf(u: string) { try { return new URL(u).pathname; } catch { return ""; } }
function isHttpUrl(s: string) { try { const u = new URL(s); return u.protocol === "http:" || u.protocol === "https:"; } catch { return false; } }

function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
}
function jaccard(a: string[], b: string[]) {
  const A = new Set(a); const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}
function containsAll(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.every((n) => H.has(n));
}

function cors(res: any, origin?: string) {
  res.setHeader("Access-Control-Allow-Origin", origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Auth");
}

// fetch with timeout
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
  } finally { clearTimeout(id); }
}

/* =============================
   Block/accept helpers
   ============================= */
function isBlockedHost(u: string): boolean {
  const host = hostOf(u);
  if (!host) return true;
  return BLOCKED_HOST_PARTS.some((part) => host.includes(part));
}
function looksHomepage(u: string): boolean {
  const path = pathnameOf(u) || "/";
  if (path === "/" || path === "") return true;
  // short root-ish pages are often home/landing
  return path.split("/").filter(Boolean).length <= 1 && !/-/.test(path);
}
function looksIndexOrListing(u: string): boolean {
  const path = pathnameOf(u).toLowerCase();
  if (/\b(page|p)=\d+\b/i.test(u)) return true;
  return ["/tag/", "/topic/", "/topics/", "/category/", "/categories/"].some((seg) => path.includes(seg));
}
function looksShoppingOrPodcast(u: string): boolean {
  const host = hostOf(u);
  const path = pathnameOf(u).toLowerCase();
  if (BLOCKED_PATH_HINTS.some((p) => path.includes(p))) return true;
  if (host.includes("amazon.")) return true;
  if (host.includes("barnesandnoble.com")) return true;
  if (host.includes("bookshop.org")) return true;
  if (host.includes("podcasts.apple.com")) return true;
  if (host.includes("music.apple.com")) return true;
  if (host.includes("spotify.com")) return true;
  return false;
}

function cseExcludeQuery(): string {
  // Convert host parts to root domains for -site: filters
  const roots = Array.from(
    new Set(
      BLOCKED_HOST_PARTS
        .map((h) => h.replace(/^www\./, ""))
        .map((h) => (h.includes(".") ? h.split("/")[0] : h))
    )
  );
  return roots.map((d) => `-site:${d}`).join(" ");
}

/* =============================
   CSE Search + scoring
   ============================= */
function scoreWebHit(hit: WebHit, authorName: string, bookTitle: string): WebHit {
  const reasons: string[] = [];
  let score = 0;

  const tTitle = tokens(hit.title || "");
  const tSnippet = tokens(hit.snippet || "");
  const combined = Array.from(new Set([...tTitle, ...tSnippet]));
  const tAuthor = tokens(authorName);
  const tBook = tokens(bookTitle);

  // Author present (in title gets stronger weight)
  const authorInTitle = tAuthor.length && jaccard(tAuthor, tTitle) >= 0.35;
  const authorInAny = tAuthor.length && (jaccard(tAuthor, combined) >= 0.3 || containsAll(combined, tAuthor));
  if (authorInTitle) { score += 0.4; reasons.push("author_in_title"); }
  else if (authorInAny) { score += 0.25; reasons.push("author_in_snippet"); }

  // Book present (title > snippet)
  const bookInTitle = tBook.length && jaccard(tBook, tTitle) >= 0.3;
  const bookInAny = tBook.length && (jaccard(tBook, combined) >= 0.25 || containsAll(combined, tBook));
  if (bookInTitle) { score += 0.3; reasons.push("book_in_title"); }
  else if (bookInAny) { score += 0.2; reasons.push("book_in_snippet"); }

  // Interview/feature signals in title
  if (FEATURE_TITLE_RE.test(hit.title || "")) {
    score += 0.2; reasons.push("featured_title_signal");
  }

  // Light recency nudge (if meta gave us something)
  if (hit.publishedAt) {
    score += 0.1; reasons.push("has_meta_date");
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

async function webSearchCSE(authorName: string, bookTitle: string, lookbackDays: number): Promise<WebHit[]> {
  if (!USE_SEARCH) return [];

  const dateRestrict = `d${Math.max(1, Math.min(LOOKBACK_MAX, lookbackDays))}`;
  const exclude = cseExcludeQuery();

  const variants: string[] = [
    `"${authorName}" "${bookTitle}" ${exclude}`.trim(),
    `"${authorName}" "${bookTitle}" (interview OR "in conversation" OR "Q&A" OR conversation) ${exclude}`.trim(),
    `"${authorName}" (interview OR "in conversation" OR "Q&A" OR conversation) ${exclude}`.trim(),
  ].filter((q) => q.replace(/\s+/g, " ").trim().length > 0);

  const all: WebHit[] = [];

  for (const q of variants) {
    let startIndex = 1;
    for (let page = 0; page < SEARCH_PAGES; page++) {
      const url = new URL("https://www.googleapis.com/customsearch/v1");
      url.searchParams.set("key", CSE_KEY);
      url.searchParams.set("cx", CSE_CX);
      url.searchParams.set("q", q);
      url.searchParams.set("num", String(SEARCH_MAX_RESULTS));
      url.searchParams.set("dateRestrict", dateRestrict);
      url.searchParams.set("start", String(startIndex));

      const resp = await fetchWithTimeout(url.toString(), undefined, FETCH_TIMEOUT_MS);
      if (!resp?.ok) break;
      const data: any = await resp.json();

      const items: any[] = Array.isArray(data?.items) ? data.items : [];
      for (const it of items) {
        const title = String(it.title || "");
        const link = String(it.link || "");
        const snippet = String(it.snippet || "");
        const pagemap = it.pagemap || {};
        const meta = Array.isArray(pagemap?.metatags) && pagemap.metatags.length ? pagemap.metatags[0] : undefined;
        const publishedAt =
          meta?.["article:published_time"] ||
          meta?.["og:updated_time"] ||
          meta?.["og:published_time"] ||
          undefined;
        const host = hostOf(link);
        all.push({ title, url: link, snippet, publishedAt, host, confidence: 0, reason: [] });
      }

      const next = data?.queries?.nextPage?.[0]?.startIndex;
      if (!next) break;
      startIndex = Number(next);
      if (!Number.isFinite(startIndex) || startIndex < 2) break;
    }
  }

  // Score, de-dup, drop obviously blocked hosts early
  const scored = all
    .filter((h) => !isBlockedHost(h.url))
    .map((h) => scoreWebHit(h, authorName, bookTitle))
    .reduce((acc: WebHit[], h) => (acc.some((x) => x.url === h.url) ? acc : acc.concat(h)), [])
    .sort((a, b) => b.confidence - a.confidence);

  return scored;
}

/* =============================
   Page fetch & validation
   ============================= */
function extractMeta(html: string, name: string, attr = "content"): string | undefined {
  // Supports <meta name="..."> or <meta property="...">
  const re = new RegExp(`<meta[^>]+(?:name|property)=["']${name}["'][^>]*>`, "i");
  const m = html.match(re)?.[0];
  if (!m) return undefined;
  const v = m.match(new RegExp(`${attr}=["']([^"']+)["']`, "i"))?.[1];
  return v ? v.trim() : undefined;
}

function extractTitle(html: string): string | undefined {
  return html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim();
}

function extractPublishedISO(html: string): string | undefined {
  const cands = [
    extractMeta(html, "article:published_time"),
    extractMeta(html, "og:published_time"),
    extractMeta(html, "og:updated_time"),
    extractMeta(html, "date"),
  ].filter(Boolean) as string[];

  for (const c of cands) {
    const d = new Date(c);
    if (!isNaN(d.getTime())) return d.toISOString();
  }

  // Try JSON-LD
  // Minimal scan for "datePublished" in Article/NewsArticle nodes
  const ldBlocks = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const blk of ldBlocks) {
    const raw = blk.replace(/^[\s\S]*?>/, "").replace(/<\/script>[\s\S]*$/, "");
    try {
      const data = JSON.parse(raw);
      const arr = Array.isArray(data) ? data : [data];
      for (const node of arr) {
        if (node && typeof node === "object") {
          if (/Article|NewsArticle/i.test(node["@type"] || "")) {
            const dp = node["datePublished"] || node["dateCreated"] || node["uploadDate"];
            if (dp) {
              const d = new Date(String(dp));
              if (!isNaN(d.getTime())) return d.toISOString();
            }
          }
        }
      }
    } catch { /* ignore */ }
  }
  return undefined;
}

function pageHasBylineFor(html: string, authorName: string): boolean {
  const name = authorName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); // escape regex
  const BY = new RegExp(`\\bby\\s+${name}\\b`, "i");
  if (BY.test(html)) return true;

  // JSON-LD author
  const ldBlocks = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const blk of ldBlocks) {
    const raw = blk.replace(/^[\s\S]*?>/, "").replace(/<\/script>[\s\S]*$/, "");
    try {
      const data = JSON.parse(raw);
      const arr = Array.isArray(data) ? data : [data];
      for (const node of arr) {
        const author = (node && (node.author || node.creator)) as any;
        if (!author) continue;
        if (typeof author === "string" && new RegExp(name, "i").test(author)) return true;
        if (Array.isArray(author)) {
          if (author.some((a) => typeof a === "string" && new RegExp(name, "i").test(a))) return true;
          if (author.some((a) => typeof a === "object" && a?.name && new RegExp(name, "i").test(String(a.name)))) return true;
        }
        if (typeof author === "object" && author?.name && new RegExp(name, "i").test(String(author.name))) return true;
      }
    } catch { /* ignore */ }
  }
  return false;
}

function pageText(html: string): string {
  // extract paragraph text roughly
  const paras = Array.from(html.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)).map((m) => m[1]);
  const cleaned = paras
    .map((t) => t.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .join(" ");
  return cleaned.slice(0, 20000);
}

function isArticleLike(html: string): boolean {
  const ogType = extractMeta(html, "og:type") || "";
  if (/article|newsarticle|profile|blog/i.test(ogType)) return true;

  // JSON-LD Article/NewsArticle presence
  const hasLdArticle = /"@type"\s*:\s*"(?:NewsArticle|Article)"/i.test(html);
  if (hasLdArticle) return true;

  // fallback heuristic: enough paragraph text
  const text = pageText(html);
  return text.length >= MIN_ARTICLE_TEXT_CHARS;
}

function textHasTokens(text: string, needleTokens: string[], minJac = 0.25): boolean {
  if (!needleTokens.length) return false;
  const hay = tokens(text);
  return jaccard(hay, needleTokens) >= minJac || containsAll(hay, needleTokens);
}

async function fetchAndValidateArticle(
  url: string,
  authorName: string,
  bookTitle: string,
  lookbackDays: number,
  requireBookMatch: boolean
): Promise<PageCheck> {
  const reasons: string[] = [];

  if (!isHttpUrl(url)) return { ok: false, reason: ["bad_url"] };
  if (isBlockedHost(url)) return { ok: false, reason: ["blocked_host"] };
  if (looksHomepage(url)) return { ok: false, reason: ["looks_homepage"] };
  if (looksIndexOrListing(url)) return { ok: false, reason: ["looks_index"] };
  if (looksShoppingOrPodcast(url)) return { ok: false, reason: ["shopping_or_podcast"] };

  const resp = await fetchWithTimeout(url, undefined, FETCH_TIMEOUT_MS);
  if (!resp?.ok) return { ok: false, reason: ["fetch_failed"] };

  const html = await resp.text();
  if (!isArticleLike(html)) return { ok: false, reason: ["not_article_like"] };

  // Title (for featured/interview signals)
  const title = extractTitle(html) || extractMeta(html, "og:title") || "";
  if (FEATURE_TITLE_RE.test(title)) reasons.push("accept_featured_title");

  // Byline signal
  const byAuthor = pageHasBylineFor(html, authorName);
  if (byAuthor) reasons.push("byline_author");

  // Text/body tokens
  const body = pageText(html);
  const authorTokens = tokens(authorName);
  const bookTokens = tokens(bookTitle);

  const hasAuthorTokens = textHasTokens(title + " " + body, authorTokens, 0.3);
  if (hasAuthorTokens) reasons.push("content_contains_author");

  const hasBookTokens = bookTokens.length ? textHasTokens(title + " " + body, bookTokens, 0.2) : false;
  if (requireBookMatch && !hasBookTokens) {
    return { ok: false, reason: ["missing_book_tokens"] };
  }
  if (hasBookTokens) reasons.push("content_contains_book");

  // Publish time + recency
  const publishedAt = extractPublishedISO(html);
  if (publishedAt) {
    const d = new Date(publishedAt);
    if (!isNaN(d.getTime())) {
      const within = daysAgo(d) <= lookbackDays;
      if (!within) return { ok: false, reason: ["stale"], publishedAt, title, textLen: body.length };
      reasons.push("fresh_within_window");
    }
  } else {
    // If no explicit date, allow if strong signals exist (byline + featured title)
    if (!(byAuthor || FEATURE_TITLE_RE.test(title))) {
      return { ok: false, reason: ["no_date_weak_signals"], title, textLen: body.length };
    }
    reasons.push("no_date_but_strong_signals");
  }

  return { ok: true, reason: reasons, publishedAt, title, textLen: body.length };
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

    const rawAuthor = (body.author_name ?? "").toString();
    const author = normalizeAuthor(rawAuthor);
    if (!author) return res.status(400).json({ error: "author_name required" });

    const bookTitle = typeof body.book_title === "string" ? body.book_title : "";
    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    const includeSearch: boolean = body.include_search !== false; // default true
    const strictAuthorMatch: boolean = body.strict_author_match === true;
    const requireBookMatch: boolean = body.require_book_match === true;
    const fallbackAuthorOnly: boolean = body.fallback_author_only === true;

    const minConfidence: number =
      typeof body.min_confidence === "number" ? Math.max(0, Math.min(1, body.min_confidence)) : 0.6;
    const minSearchConfidence: number =
      typeof body.min_search_confidence === "number" ? Math.max(0, Math.min(1, body.min_search_confidence)) : 0.6;

    const debug: boolean = body.debug === true;

    const cacheKey = makeCacheKey({
      author_name: author,
      book_title: bookTitle,
      lookback_days: lookback,
      include_search: includeSearch,
      strict_author_match: strictAuthorMatch,
      require_book_match: requireBookMatch,
      fallback_author_only: fallbackAuthorOnly,
      min_confidence: minConfidence,
      min_search_confidence: minSearchConfidence,
      debug
    });
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // Only web search (no RSS press feeds)
    if (!USE_SEARCH || !includeSearch) {
      const empty: CacheValue = { author_name: author, has_recent: false };
      if (debug) {
        empty._debug = [{
          feedUrl: "search_disabled",
          ok: false,
          source: "web",
          latest: null,
          recentWithinWindow: false,
          confidence: 0,
          reason: ["search_disabled"],
          siteTitle: null,
          feedTitle: null,
          error: null
        }];
      }
      setCached(cacheKey, empty);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(empty);
    }

    const hits = await webSearchCSE(author, bookTitle, lookback);

    // Primary pass (author+book required if caller asked for it)
    if (requireBookMatch) {
      for (const h of hits) {
        if (h.confidence < minSearchConfidence) continue;
        const page = await fetchAndValidateArticle(h.url, author, bookTitle, lookback, true);
        if (!page.ok) continue;

        const payload: CacheValue = {
          author_name: author,
          has_recent: true,
          latest_title: page.title || h.title,
          latest_url: h.url,
          published_at: page.publishedAt,
          source: "web",
          author_url: originOf(h.url),
          _debug: debug ? [{
            feedUrl: h.url,
            ok: true,
            source: "web",
            latest: page.publishedAt || null,
            recentWithinWindow: true,
            confidence: Number(h.confidence.toFixed(2)),
            reason: [...h.reason, ...page.reason],
            siteTitle: null,
            feedTitle: null,
            error: null
          }] : undefined
        };
        setCached(cacheKey, payload);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      }
    }

    // Fallback pass (author-only interview/feature acceptance)
    if (fallbackAuthorOnly) {
      for (const h of hits) {
        if (h.confidence < minSearchConfidence) continue;
        const page = await fetchAndValidateArticle(h.url, author, bookTitle, lookback, false);
        if (!page.ok) continue;

        // Require at least a strong author signal (byline) or featured/interview title
        const strong = page.reason.includes("byline_author") || page.reason.includes("accept_featured_title");
        if (!strong) continue;

        const payload: CacheValue = {
          author_name: author,
          has_recent: true,
          latest_title: page.title || h.title,
          latest_url: h.url,
          published_at: page.publishedAt,
          source: "web",
          author_url: originOf(h.url),
          _debug: debug ? [{
            feedUrl: h.url,
            ok: true,
            source: "web",
            latest: page.publishedAt || null,
            recentWithinWindow: true,
            confidence: Number(h.confidence.toFixed(2)),
            reason: [...h.reason, ...page.reason],
            siteTitle: null,
            feedTitle: null,
            error: null
          }] : undefined
        };
        setCached(cacheKey, payload);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      }
    }

    // Nothing acceptable
    if (strictAuthorMatch) {
      const rej = { ...ERR_NO_CONFIDENT } as CacheValue;
      if (debug) {
        rej._debug = hits.slice(0, 5).map((h) => ({
          feedUrl: h.url,
          ok: false,
          source: "web",
          latest: null,
          recentWithinWindow: false,
          confidence: Number(h.confidence.toFixed(2)),
          reason: h.reason,
          siteTitle: null,
          feedTitle: null,
          error: null
        }));
      }
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(rej);
    }

    const empty: CacheValue = { author_name: author, has_recent: false };
    if (debug) {
      empty._debug = hits.slice(0, 5).map((h) => ({
        feedUrl: h.url,
        ok: false,
        source: "web",
        latest: null,
        recentWithinWindow: false,
        confidence: Number(h.confidence.toFixed(2)),
        reason: h.reason,
        siteTitle: null,
        feedTitle: null,
        error: null
      }));
    }
    setCached(cacheKey, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(empty);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
