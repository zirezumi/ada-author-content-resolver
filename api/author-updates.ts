// api/author-updates.ts
/// <reference types="node" />

import Parser from "rss-parser";
import pLimit from "p-limit";

// Ensure Node runtime on Vercel (NOT edge)
export const config = { runtime: "nodejs" } as const;

/* =============================
   Auth
   ============================= */
// Comma-separated secrets allowed: "s1,s2"
const AUTH_SECRETS = (process.env.AUTHOR_UPDATES_SECRET || "")
  .split(",")
  .map(s => s.trim())
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
const LOOKBACK_DEFAULT = 30;                  // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 90;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;     // 24h
const FETCH_TIMEOUT_MS = 4500;                // per network call
const USER_AGENT = "AuthorUpdates/1.4 (+https://example.com)";
const CONCURRENCY = 4;                        // feed checks in parallel (bounded)
const MAX_FEED_CANDIDATES = 15;               // latency guard
const MAX_KNOWN_URLS = 5;                     // abuse guard

/* ===== Web search tunables (Bing) ===== */
const USE_SEARCH = !!process.env.BING_SEARCH_KEY; // enabled if key present
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.7;
const SEARCH_MAX_RESULTS = 10;
const DOMAIN_WHITELIST = (process.env.SEARCH_DOMAIN_WHITELIST || "")
  .split(",")
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

/* ===== Identity scoring tunables ===== */
const DEFAULT_MIN_CONFIDENCE = 0.6; // 0..1
const STRICT_REJECT_PAYLOAD = { error: "no_confident_author_match" };

/* =============================
   Types
   ============================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string;      // "rss" | "substack" | "medium" | "web" | etc.
  author_url?: string;  // best site/feed homepage
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

type PlatformHints = {
  platform?: "substack" | "medium" | "wordpress" | "ghost" | "blogger";
  handle?: string;     // e.g., substack/medium handle
  feed_url?: string;   // exact feed, if known
  site_url?: string;   // base site, if known
};

type FeedItem = { title: string; link: string; date: Date };

type Context = {
  authorName: string;
  knownHosts: string[];         // from known_urls
  bookTitleTokens: string[];    // from book_title
  publisherTokens: string[];    // from publisher
  isbn?: string;                // raw isbn string if provided
};

type EvalResult = {
  feedUrl: string;
  authorUrl?: string;
  siteTitle?: string | null;
  feedTitle?: string | null;
  latest?: FeedItem;
  recentWithinWindow: boolean;
  source: string;
  ok: boolean;
  confidence: number;     // 0..1
  reason: string[];       // why score reached value
  error?: string;
};

type WebHit = {
  title: string;
  url: string;
  snippet?: string;
  publishedAt?: string; // ISO
  host: string;
  confidence: number;   // 0..1
  reason: string[];
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();
function makeCacheKey(
  author: string,
  urls: string[],
  lookback: number,
  hints?: unknown,
  strict?: boolean,
  minConf?: number,
  bookTitle?: string,
  publisher?: string,
  isbn?: string,
  includeSearch?: boolean,
  minSearchConf?: number
) {
  const urlKey = urls.map(u => u.trim().toLowerCase()).filter(Boolean).sort().join("|");
  const hintKey = hints
    ? JSON.stringify(Object.keys(hints as Record<string, unknown>).sort().reduce((o: any, k: string) => (o[k] = (hints as any)[k], o), {}))
    : "";
  return [
    author.trim().toLowerCase(),
    lookback,
    urlKey,
    hintKey,
    strict ? "strict" : "loose",
    String(minConf ?? DEFAULT_MIN_CONFIDENCE),
    (bookTitle || "").toLowerCase().trim(),
    (publisher || "").toLowerCase().trim(),
    (isbn || "").replace(/[-\s]/g, ""),
    includeSearch ? "search" : "nosearch",
    String(minSearchConf ?? DEFAULT_MIN_SEARCH_CONFIDENCE)
  ].join("::");
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
const parser = new Parser();
function daysAgo(d: Date) { return (Date.now() - d.getTime()) / 86_400_000; }
function clamp(n: number, min: number, max: number) { return Math.max(min, Math.min(max, n)); }
function sourceFromUrl(url?: string) {
  if (!url) return "unknown";
  const u = url.toLowerCase();
  if (u.includes("substack.com")) return "substack";
  if (u.includes("medium.com")) return "medium";
  if (u.includes("ghost")) return "ghost";
  if (u.includes("blogspot.") || u.includes("blogger.")) return "blogger";
  if (u.includes("wordpress")) return "wordpress";
  return "rss";
}
function hostOf(u: string) { try { return new URL(u).host.toLowerCase(); } catch { return ""; } }
function guessFeedsFromSite(site: string): string[] {
  const clean = site.replace(/\/+$/, "");
  return [`${clean}/feed`, `${clean}/rss.xml`, `${clean}/atom.xml`, `${clean}/index.xml`, `${clean}/?feed=rss2`];
}
function extractRssLinks(html: string, base: string): string[] {
  const links = Array.from(html.matchAll(/<link[^>]+rel=["']alternate["'][^>]*>/gi)).map(m => m[0]);
  const hrefs = links.flatMap((tag: string) => {
    const href = /href=["']([^"']+)["']/i.exec(tag)?.[1];
    const type = /type=["']([^"']+)["']/i.exec(tag)?.[1] ?? "";
    const looksRss = /rss|atom|application\/(rss|atom)\+xml/i.test(type) || /rss|atom/i.test(href ?? "");
    if (!href || !looksRss) return [];
    try { return [new URL(href, base).toString()]; } catch { return []; }
  });
  return Array.from(new Set(hrefs));
}
async function fetchWithTimeout(url: string, init?: RequestInit, ms = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, { ...init, signal: ctrl.signal, headers: { "User-Agent": USER_AGENT, ...(init?.headers || {}) } });
    return res;
  } finally { clearTimeout(id); }
}
async function parseFeedURL(feedUrl: string) {
  const res = await fetchWithTimeout(feedUrl);
  if (!res || !res.ok) return null;
  const xml = await res.text();
  try { return await parser.parseString(xml); } catch { return null; }
}
function newestItem(feed: unknown): FeedItem | null {
  const raw: any[] = (feed as any)?.items ?? [];
  const items: FeedItem[] = raw.map((it: any) => {
      const d = new Date(it.isoDate || it.pubDate || it.published || it.date || 0);
      return { title: String(it.title ?? ""), link: String(it.link ?? ""), date: d };
    })
    .filter((x: FeedItem) => x.link && !isNaN(x.date.getTime()))
    .sort((a: FeedItem, b: FeedItem) => b.date.getTime() - a.date.getTime());
  return items.length ? items[0] : null;
}
function normalizeAuthor(s: string) { return s.normalize("NFKC").replace(/\s+/g, " ").trim(); }
function isHttpUrl(s: string) { try { const u = new URL(s); return u.protocol === "http:" || u.protocol === "https:"; } catch { return false; } }

function tokens(s: string): string[] {
  return s.toLowerCase().normalize("NFKC").replace(/[^\p{L}\p{N}\s.-]/gu, " ").split(/[\s.-]+/).filter(Boolean);
}
function jaccard(a: string[], b: string[]) {
  const A = new Set(a); const B = new Set(b);
  const inter = [...A].filter(x => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}
function containsAll(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.every(n => H.has(n));
}
function startsWithAll(hay: string[], needles: string[]) {
  const h = hay.join(" ");
  const n = needles.join(" ");
  return h.startsWith(n);
}
function itemText(it: any): string {
  const bits = [
    it?.title,
    it?.contentSnippet,
    it?.content,
    it?.["content:encodedSnippet"],
    it?.["content:encoded"],
    it?.summary
  ].filter(Boolean);
  return String(bits.join(" ").slice(0, 5000));
}

/* =============================
   Feed candidates
   ============================= */
async function findFeeds(author: string, knownUrls: string[] = [], hints?: PlatformHints): Promise<string[]> {
  const candidates = new Set<string>();

  // 0) Precise hints first
  if (hints?.feed_url && isHttpUrl(hints.feed_url)) candidates.add(hints.feed_url);
  if (hints?.site_url && isHttpUrl(hints.site_url)) {
    for (const f of guessFeedsFromSite(hints.site_url)) candidates.add(f);
  }

  // 1) Platform hints (handle)
  if (hints?.platform && hints?.handle) {
    const h = hints.handle.replace(/^@/, "");
    if (hints.platform === "substack") candidates.add(`https://${h}.substack.com/feed`);
    if (hints.platform === "medium") {
      candidates.add(`https://medium.com/feed/@${h}`);
      candidates.add(`https://medium.com/feed/${h}`);
    }
  }

  // 2) Heuristics from author name
  const handles = [author.toLowerCase().replace(/\s+/g, ""), author.toLowerCase().replace(/\s+/g, "-")];
  for (const h of handles) {
    candidates.add(`https://${h}.substack.com/feed`);
    candidates.add(`https://medium.com/feed/@${h}`);
    candidates.add(`https://medium.com/feed/${h}`);
  }

  // 3) From known sites: feed patterns + discovery
  for (const site of knownUrls) {
    if (!isHttpUrl(site)) continue;
    for (const f of guessFeedsFromSite(site)) candidates.add(f);
    try {
      const html = await fetchWithTimeout(site);
      if (html?.ok) {
        const text = await html.text();
        for (const u of extractRssLinks(text, site)) candidates.add(u);
      }
    } catch {/* ignore */}
  }

  return Array.from(candidates).slice(0, MAX_FEED_CANDIDATES);
}

/* =============================
   Identity scoring + evaluation
   ============================= */
async function fetchSiteTitle(url: string): Promise<string | null> {
  try {
    const origin = new URL(url).origin;
    const res = await fetchWithTimeout(origin);
    if (!res?.ok) return null;
    const html = await res.text();
    const mTitle = html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim();
    const mOG = html.match(/<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)["']/i)?.[1]?.trim();
    return (mOG || mTitle || null);
  } catch { return null; }
}

async function evaluateFeed(feedUrl: string, lookback: number, ctx: Context): Promise<EvalResult> {
  try {
    const feed = await parseFeedURL(feedUrl);
    if (!feed) {
      return { feedUrl, ok: false, recentWithinWindow: false, source: sourceFromUrl(feedUrl), confidence: 0, reason: ["parse_failed"], error: "parse_failed" };
    }

    const authorUrl = (feed as any)?.link?.startsWith?.("http") ? (feed as any).link : new URL(feedUrl).origin;
    const siteTitle = await fetchSiteTitle(feedUrl);
    const feedTitle = (feed as any)?.title ? String((feed as any).title) : null;

    const latest = newestItem(feed) || undefined;
    const recentWithinWindow = !!(latest && daysAgo(latest.date) <= lookback);

    // ---- Identity scoring ----
    const reasons: string[] = [];
    let score = 0;

    const authorTokens = tokens(ctx.authorName);
    const siteTokens = tokens(siteTitle || "");
    const feedTokens = tokens(feedTitle || "");
    const host = hostOf(authorUrl || feedUrl);

    // 1) Known host match
    const hostMatch = ctx.knownHosts.includes(host);
    if (hostMatch) { score += 0.35; reasons.push(`host_match:${host}`); }

    // 2) Name appears in site or feed title
    const jSite = jaccard(authorTokens, siteTokens);
    const jFeed = jaccard(authorTokens, feedTokens);
    if (jSite >= 0.5) { score += 0.25; reasons.push(`site_name_sim:${jSite.toFixed(2)}`); }
    if (jFeed >= 0.5) { score += 0.25; reasons.push(`feed_title_sim:${jFeed.toFixed(2)}`); }

    // 3) Strong string inclusion
    if (startsWithAll(siteTokens, authorTokens) || containsAll(siteTokens, authorTokens)) {
      score += 0.15; reasons.push("site_contains_author");
    }
    if (containsAll(feedTokens, authorTokens)) {
      score += 0.15; reasons.push("feed_contains_author");
    }

    // 4) Platform handle similarity
    if (host.endsWith("substack.com") || host.endsWith("medium.com")) {
      const subdomain = host.split(".").slice(0, -2).join(".");
      const handleTokens = tokens(subdomain.replace(/^@/, ""));
      const jHandle = jaccard(authorTokens, handleTokens);
      if (jHandle >= 0.5) { score += 0.2; reasons.push(`handle_sim:${jHandle.toFixed(2)}`); }
    }

    // 5) Book/Publisher tokens in latest ITEM text (title + summary/body)
    const latestText = latest ? itemText((feed as any).items?.[0] ?? {}) : "";
    const contentTokens = tokens([
      latest?.title || "",
      latestText || "",
      feedTitle || "",
      siteTitle || ""
    ].join(" "));

    const hasBook = ctx.bookTitleTokens.length
      ? jaccard(ctx.bookTitleTokens, contentTokens) >= 0.2 || containsAll(contentTokens, ctx.bookTitleTokens)
      : false;

    const hasPub  = ctx.publisherTokens.length
      ? jaccard(ctx.publisherTokens, contentTokens) >= 0.2
      : false;

    const authorInItem = authorTokens.length
      ? jaccard(authorTokens, contentTokens) >= 0.3 || containsAll(contentTokens, authorTokens)
      : false;

    if (hasBook && authorInItem) { score += 0.3; reasons.push("item_mentions_author_and_book"); }
    else if (hasBook) { score += 0.15; reasons.push("item_mentions_book"); }

    if (hasPub)  { score += 0.05; reasons.push("publisher_term_presence"); }

    // Cap to [0,1]
    score = Math.max(0, Math.min(1, score));

    return {
      feedUrl,
      authorUrl,
      siteTitle,
      feedTitle,
      latest,
      recentWithinWindow,
      source: sourceFromUrl(feedUrl),
      ok: true,
      confidence: score,
      reason: reasons
    };
  } catch (e: unknown) {
    const msg = (e as Error)?.message || "error";
    return { feedUrl, ok: false, recentWithinWindow: false, source: sourceFromUrl(feedUrl), confidence: 0, reason: ["exception"], error: msg };
  }
}

async function evaluateFeeds(feeds: string[], lookback: number, ctx: Context) {
  const limit = pLimit(CONCURRENCY);
  const results = await Promise.all(feeds.map(f => limit(() => evaluateFeed(f, lookback, ctx))));

  // 1) Any recent within window? choose highest confidence; tie-break by newest
  const recent = results
    .filter(r => r.ok && r.recentWithinWindow && r.latest)
    .sort((a, b) => (b.confidence - a.confidence) || (b.latest!.date.getTime() - a.latest!.date.getTime()));
  if (recent.length) return { choice: recent[0], results };

  // 2) Otherwise, pick highest confidence overall; tie-break by freshest latest
  const byConf = results
    .filter(r => r.ok && (r.authorUrl || r.latest))
    .sort((a, b) => (b.confidence - a.confidence) || ((b.latest?.date.getTime() || 0) - (a.latest?.date.getTime() || 0)));
  if (byConf.length) return { choice: byConf[0], results };

  // 3) Fallback any OK result
  const anyOk = results.find(r => r.ok);
  if (anyOk) return { choice: anyOk, results };

  // 4) Nothing usable
  return { choice: null, results };
}

/* =============================
   Web Search (Bing) Integration
   ============================= */
function scoreWebHit(hit: WebHit, authorName: string, bookTitle: string, lookbackDays: number): WebHit {
  const reasons: string[] = [];
  let score = 0;

  const tTitle = tokens(hit.title || "");
  const tSnippet = tokens(hit.snippet || "");
  const tCombined = Array.from(new Set([...tTitle, ...tSnippet]));
  const tAuthor = tokens(authorName);
  const tBook = tokens(bookTitle);

  // author presence
  const authorMatch = tAuthor.length && (jaccard(tAuthor, tCombined) >= 0.3 || containsAll(tCombined, tAuthor));
  if (authorMatch) { score += 0.35; reasons.push("author_in_title_or_snippet"); }

  // book presence
  const bookMatch = tBook.length && (jaccard(tBook, tCombined) >= 0.3 || containsAll(tCombined, tBook));
  if (bookMatch) { score += 0.35; reasons.push("book_in_title_or_snippet"); }

  // domain whitelist bonus
  const host = hit.host.toLowerCase();
  if (DOMAIN_WHITELIST.includes(host)) { score += 0.15; reasons.push(`domain_whitelist:${host}`); }

  // recency bonus
  let within = false;
  if (hit.publishedAt) {
    const d = new Date(hit.publishedAt);
    if (!isNaN(d.getTime()) && daysAgo(d) <= lookbackDays) { score += 0.25; reasons.push("fresh_within_window"); within = true; }
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

async function webSearchBing(authorName: string, bookTitle: string, lookbackDays: number): Promise<WebHit[]> {
  if (!process.env.BING_SEARCH_KEY) return [];
  const q = `"${authorName}" "${bookTitle}"`;
  const freshness = lookbackDays <= 7 ? "Week" : "Month";

  const url = new URL("https://api.bing.microsoft.com/v7.0/news/search");
  url.searchParams.set("q", q);
  url.searchParams.set("mkt", "en-US");
  url.searchParams.set("freshness", freshness);
  url.searchParams.set("count", String(SEARCH_MAX_RESULTS));
  url.searchParams.set("textDecorations", "false");
  url.searchParams.set("textFormat", "Raw");

  const resp = await fetchWithTimeout(url.toString(), {
    headers: { "Ocp-Apim-Subscription-Key": process.env.BING_SEARCH_KEY! }
  }, FETCH_TIMEOUT_MS);

  if (!resp?.ok) return [];
  const data: any = await resp.json();
  const items: any[] = Array.isArray(data?.value) ? data.value : [];

  const hits: WebHit[] = items.map((it: any) => {
    const title = String(it.name || "");
    const url = String(it.url || "");
    const publishedAt = it.datePublished ? new Date(it.datePublished).toISOString() : undefined;
    const snippet = String(it.description || "");
    const host = hostOf(url);
    return { title, url, snippet, publishedAt, host, confidence: 0, reason: [] };
  });

  return hits
    .map(h => scoreWebHit(h, authorName, bookTitle, lookbackDays))
    .sort((a, b) => b.confidence - a.confidence)
    .slice(0, SEARCH_MAX_RESULTS);
}

/* =============================
   CORS + Handler
   ============================= */
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
    const author = normalizeAuthor(rawAuthor);
    if (!author) return res.status(400).json({ error: "author_name required" });

    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    let knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls : [];
    knownUrls = knownUrls.map((u: unknown) => String(u).trim()).filter(isHttpUrl).slice(0, MAX_KNOWN_URLS);

    const hints: PlatformHints | undefined =
      body.hints && typeof body.hints === "object"
        ? {
            platform: body.hints.platform as PlatformHints["platform"],
            handle: typeof body.hints.handle === "string" ? body.hints.handle : undefined,
            feed_url: typeof body.hints.feed_url === "string" ? body.hints.feed_url : undefined,
            site_url: typeof body.hints.site_url === "string" ? body.hints.site_url : undefined,
          }
        : undefined;

    const debug: boolean = body.debug === true;
    const strictAuthorMatch: boolean = body.strict_author_match === true;
    const minConfidence: number = typeof body.min_confidence === "number"
      ? Math.max(0, Math.min(1, body.min_confidence))
      : DEFAULT_MIN_CONFIDENCE;

    const includeSearch: boolean = body.include_search === true;
    const minSearchConfidence: number = typeof body.min_search_confidence === "number"
      ? Math.max(0, Math.min(1, body.min_search_confidence))
      : DEFAULT_MIN_SEARCH_CONFIDENCE;

    const bookTitle = typeof body.book_title === "string" ? body.book_title : "";
    const publisher = typeof body.publisher === "string" ? body.publisher : "";
    const isbn = typeof body.isbn === "string" ? body.isbn : undefined;

    const ctx: Context = {
      authorName: author,
      knownHosts: knownUrls.map(hostOf).filter(Boolean),
      bookTitleTokens: tokens(bookTitle),
      publisherTokens: tokens(publisher),
      isbn
    };

    const cacheKey = makeCacheKey(
      author, knownUrls, lookback, hints,
      strictAuthorMatch, minConfidence,
      bookTitle, publisher, isbn,
      includeSearch, minSearchConfidence
    );
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // Build candidates
    const baseFeeds = await findFeeds(author, knownUrls, hints);
    const feeds = Array.from(new Set(baseFeeds));

    // Optional web search (Bing) for author+book
    let bestSearch: WebHit | null = null;
    if (includeSearch && USE_SEARCH && bookTitle) {
      const hits = await webSearchBing(author, bookTitle, lookback);
      const top = hits[0];
      if (top && top.confidence >= minSearchConfidence) {
        // validate recency if publish date present
        let recentOk = false;
        if (top.publishedAt) {
          const d = new Date(top.publishedAt);
          recentOk = !isNaN(d.getTime()) && daysAgo(d) <= lookback;
        }
        // Even if publishedAt absent, you might still accept based on signals — we enforce recency when date exists.
        if (recentOk || !top.publishedAt) {
          bestSearch = top;
        }
      }
    }

    // If search produced a confident, recent hit, return it as the "latest"
    if (bestSearch && bestSearch.publishedAt) {
      const d = new Date(bestSearch.publishedAt);
      const within = !isNaN(d.getTime()) && daysAgo(d) <= lookback;
      if (within) {
        const payload: CacheValue = {
          author_name: author,
          has_recent: true,
          latest_title: bestSearch.title,
          latest_url: bestSearch.url,
          published_at: bestSearch.publishedAt,
          source: "web",
          author_url: `https://${bestSearch.host}`
        };
        if (debug) {
          payload._debug = [{
            feedUrl: bestSearch.url,
            ok: true,
            source: "web",
            latest: bestSearch.publishedAt || null,
            recentWithinWindow: true,
            confidence: Number(bestSearch.confidence.toFixed(2)),
            reason: bestSearch.reason,
            siteTitle: null,
            feedTitle: null,
            error: null
          }];
        }
        setCached(cacheKey, payload);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      }
    }

    // Evaluate feeds and identity scoring
    const { choice, results } = await evaluateFeeds(feeds, lookback, ctx);

    // Strict author matching gate
    if (strictAuthorMatch && (!choice || choice.confidence < minConfidence)) {
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(STRICT_REJECT_PAYLOAD);
    }

    // Build debug helper
    const buildDebug = (list: EvalResult[]) => list.slice(0, 5).map(r => ({
      feedUrl: r.feedUrl,
      ok: r.ok,
      source: r.source,
      latest: r.latest ? r.latest.date.toISOString() : null,
      recentWithinWindow: r.recentWithinWindow,
      confidence: Number(r.confidence.toFixed(2)),
      reason: r.reason,
      siteTitle: r.siteTitle || null,
      feedTitle: r.feedTitle || null,
      error: r.error || null
    }));

    if (choice && choice.recentWithinWindow && choice.latest) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: choice.latest.title,
        latest_url: choice.latest.link,
        published_at: choice.latest.date.toISOString(),
        source: choice.source,
        author_url: choice.authorUrl
      };
      if (debug) payload._debug = buildDebug(results);
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    if (choice && choice.authorUrl) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: false,
        source: choice.source,
        author_url: choice.authorUrl
      };
      if (debug) payload._debug = buildDebug(results);
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    const empty: CacheValue = { author_name: author, has_recent: false };
    if (debug) empty._debug = buildDebug(results);
    setCached(cacheKey, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(empty);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
