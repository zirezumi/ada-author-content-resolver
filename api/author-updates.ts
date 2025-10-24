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
const LOOKBACK_MAX = 120;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 6000;
const USER_AGENT = "AuthorUpdates/1.9 (+https://example.com)";
const CONCURRENCY = 4;

const MAX_FEED_CANDIDATES = 15;
const MAX_KNOWN_URLS = 5;

/* ===== Google Custom Search (CSE) ===== */
const USE_SEARCH = !!(process.env.GOOGLE_CSE_KEY && process.env.GOOGLE_CSE_CX);
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_CX = process.env.GOOGLE_CSE_CX || "";
const SEARCH_MAX_RESULTS = 10;

/* ===== Web filtering / acceptance ===== */
// Domains we consider noisy for our use case (social, shopping, podcasts, newsletters)
const BLOCKED_HOST_PARTS: string[] = [
  // socials
  "twitter.com",
  "x.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "threads.net",
  "linkedin.com",
  "youtube.com",
  "youtu.be",
  "bluesky",
  "mastodon",
  // podcasts/audio
  "spotify.com",
  "open.spotify.com",
  "podcasts.apple.com",
  "music.apple.com",
  "apps.apple.com",
  "simplecast.com",
  "buzzsprout.com",
  "podbean.com",
  "soundcloud.com",
  "megaphone.fm",
  "stitcher.com",
  // shopping
  "amazon.",
  "amzn.to",
  "barnesandnoble.com",
  "bookshop.org",
  "walmart",
  "target.com",
  "audible.com",
  // newsletter we currently block per requirements
  "substack.com",
];

const INDEX_PATH_HINTS = [
  "/tag/",
  "/tags/",
  "/category/",
  "/topics/",
  "/topic/",
  "/page/",
  "/author/",
  "/authors/",
  "/search?",
  "/?s=",
];

const ARTICLE_WORD_HINTS = ["article", "story", "news", "books", "culture"];
const DATE_PATH_RE = /\/20\d{2}(?:\/[01]?\d(?:\/[0-3]?\d)?)?\//;

/* ===== Identity / acceptance thresholds ===== */
const DEFAULT_MIN_CONFIDENCE = 0.6; // legacy (RSS path)
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.7;

/* =============================
   Types
   ============================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string;
  author_url?: string;
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
  handle?: string;
  feed_url?: string;
  site_url?: string;
};

type FeedItem = { title: string; link: string; date: Date };

type Context = {
  authorName: string;
  knownHosts: string[];
  bookTitleTokens: string[];
  publisherTokens: string[];
  isbn?: string;
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
  confidence: number;
  reason: string[];
  error?: string;
};

type WebHit = {
  title: string;
  url: string;
  snippet?: string;
  publishedAt?: string;
  host: string;
  confidence: number;
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
  minSearchConf?: number,
  requireBook?: boolean,
  fallbackAuthorOnly?: boolean
) {
  const urlKey = urls.map((u) => u.trim().toLowerCase()).filter(Boolean).sort().join("|");
  const hintKey = hints
    ? JSON.stringify(
        Object.keys(hints as Record<string, unknown>)
          .sort()
          .reduce((o: Record<string, unknown>, k: string) => {
            o[k] = (hints as Record<string, unknown>)[k];
            return o;
          }, {})
      )
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
    String(minSearchConf ?? DEFAULT_MIN_SEARCH_CONFIDENCE),
    requireBook ? "reqBook" : "noReqBook",
    fallbackAuthorOnly ? "fb_author_only" : "no_fb"
  ].join("::");
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
   Utilities
   ============================= */
const parser = new Parser();

function daysAgo(d: Date) {
  return (Date.now() - d.getTime()) / 86_400_000;
}
function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}
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
function hostOf(u: string) {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}
function guessFeedsFromSite(site: string): string[] {
  const clean = site.replace(/\/+$/, "");
  return [
    `${clean}/feed`,
    `${clean}/rss.xml`,
    `${clean}/atom.xml`,
    `${clean}/index.xml`,
    `${clean}/?feed=rss2`,
  ];
}
function extractRssLinks(html: string, base: string): string[] {
  const links = Array.from(html.matchAll(/<link[^>]+rel=["']alternate["'][^>]*>/gi)).map((m) => m[0]);
  const hrefs = links.flatMap((tag: string) => {
    const href = /href=["']([^"']+)["']/i.exec(tag)?.[1];
    const type = /type=["']([^"']+)["']/i.exec(tag)?.[1] ?? "";
    const looksRss =
      /rss|atom|application\/(rss|atom)\+xml/i.test(type) ||
      /rss|atom/i.test(href ?? "");
    if (!href || !looksRss) return [];
    try {
      return [new URL(href, base).toString()];
    } catch {
      return [];
    }
  });
  return Array.from(new Set(hrefs));
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
async function parseFeedURL(feedUrl: string) {
  const res = await fetchWithTimeout(feedUrl);
  if (!res || !res.ok) return null;
  const xml = await res.text();
  try {
    return await parser.parseString(xml);
  } catch {
    return null;
  }
}
function newestItem(feed: unknown): FeedItem | null {
  const rawItems: Array<any> = (feed as any)?.items ?? [];
  const items: FeedItem[] = rawItems
    .map((it: any) => {
      const d = new Date(it.isoDate || it.pubDate || it.published || it.date || 0);
      return { title: String(it.title ?? ""), link: String(it.link ?? ""), date: d };
    })
    .filter((x: FeedItem) => x.link && !isNaN(x.date.getTime()))
    .sort((a: FeedItem, b: FeedItem) => b.date.getTime() - a.date.getTime());
  return items.length ? items[0] : null;
}
function normalizeAuthor(s: string) {
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

function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s.-]/gu, " ")
    .split(/[\s.-]+/)
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
    it?.summary,
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
  const handles = [
    author.toLowerCase().replace(/\s+/g, ""),
    author.toLowerCase().replace(/\s+/g, "-"),
  ];
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
    } catch {
      /* ignore */
    }
  }

  return Array.from(candidates).slice(0, MAX_FEED_CANDIDATES);
}

/* =============================
   Identity scoring + evaluation (RSS)
   ============================= */
async function fetchSiteTitle(url: string): Promise<string | null> {
  try {
    const origin = new URL(url).origin;
    const res = await fetchWithTimeout(origin);
    if (!res?.ok) return null;
    const html = await res.text();
    const mTitle = html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim();
    const mOG =
      html.match(/<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)["']/i)?.[1]?.trim();
    return mOG || mTitle || null;
  } catch {
    return null;
  }
}

async function evaluateFeed(feedUrl: string, lookback: number, ctx: Context): Promise<EvalResult> {
  try {
    const feed = await parseFeedURL(feedUrl);
    if (!feed) {
      return {
        feedUrl,
        ok: false,
        recentWithinWindow: false,
        source: sourceFromUrl(feedUrl),
        confidence: 0,
        reason: ["parse_failed"],
        error: "parse_failed",
      };
    }

    const authorUrl =
      (feed as any)?.link?.startsWith?.("http") ? (feed as any).link : new URL(feedUrl).origin;
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

    const hostMatch = ctx.knownHosts.includes(host);
    if (hostMatch) {
      score += 0.35;
      reasons.push(`host_match:${host}`);
    }

    const jSite = jaccard(authorTokens, siteTokens);
    const jFeed = jaccard(authorTokens, feedTokens);
    if (jSite >= 0.5) {
      score += 0.25;
      reasons.push(`site_name_sim:${jSite.toFixed(2)}`);
    }
    if (jFeed >= 0.5) {
      score += 0.25;
      reasons.push(`feed_title_sim:${jFeed.toFixed(2)}`);
    }

    if (startsWithAll(siteTokens, authorTokens) || containsAll(siteTokens, authorTokens)) {
      score += 0.15;
      reasons.push("site_contains_author");
    }
    if (containsAll(feedTokens, authorTokens)) {
      score += 0.15;
      reasons.push("feed_contains_author");
    }

    if (host.endsWith("substack.com") || host.endsWith("medium.com")) {
      const subdomain = host.split(".").slice(0, -2).join(".");
      const handleTokens = tokens(subdomain.replace(/^@/, ""));
      const jHandle = jaccard(authorTokens, handleTokens);
      if (jHandle >= 0.5) {
        score += 0.2;
        reasons.push(`handle_sim:${jHandle.toFixed(2)}`);
      }
    }

    const latestText = latest ? itemText((feed as any).items?.[0] ?? {}) : "";
    const contentTokens = tokens([latest?.title || "", latestText || "", feedTitle || "", siteTitle || ""].join(" "));

    const hasBook = ctx.bookTitleTokens.length
      ? jaccard(ctx.bookTitleTokens, contentTokens) >= 0.2 || containsAll(contentTokens, ctx.bookTitleTokens)
      : false;

    const hasPub = ctx.publisherTokens.length ? jaccard(ctx.publisherTokens, contentTokens) >= 0.2 : false;

    const authorInItem = authorTokens.length
      ? jaccard(authorTokens, contentTokens) >= 0.3 || containsAll(contentTokens, authorTokens)
      : false;

    if (hasBook && authorInItem) {
      score += 0.3;
      reasons.push("item_mentions_author_and_book");
    } else if (hasBook) {
      score += 0.15;
      reasons.push("item_mentions_book");
    }

    if (hasPub) {
      score += 0.05;
      reasons.push("publisher_term_presence");
    }

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
      reason: reasons,
    };
  } catch (e: unknown) {
    const msg = (e as Error)?.message || "error";
    return {
      feedUrl,
      ok: false,
      recentWithinWindow: false,
      source: sourceFromUrl(feedUrl),
      confidence: 0,
      reason: ["exception"],
      error: msg,
    };
  }
}

async function evaluateFeeds(feeds: string[], lookback: number, ctx: Context) {
  const limit = pLimit(CONCURRENCY);
  const results = await Promise.all(feeds.map((f) => limit(() => evaluateFeed(f, lookback, ctx))));

  // Prefer any feed with recent item; pick highest confidence; tie-break by newest
  const recent = results
    .filter((r: EvalResult) => r.ok && r.recentWithinWindow && r.latest)
    .sort((a: EvalResult, b: EvalResult) => b.confidence - a.confidence || b.latest!.date.getTime() - a.latest!.date.getTime());
  if (recent.length) return { choice: recent[0], results };

  // Otherwise pick best confidence
  const byConf = results
    .filter((r: EvalResult) => r.ok && (r.authorUrl || r.latest))
    .sort(
      (a: EvalResult, b: EvalResult) =>
        b.confidence - a.confidence || (b.latest?.date.getTime() || 0) - (a.latest?.date.getTime() || 0)
    );
  if (byConf.length) return { choice: byConf[0], results };

  const anyOk = results.find((r: EvalResult) => r.ok);
  if (anyOk) return { choice: anyOk, results };

  return { choice: null as EvalResult | null, results };
}

/* =============================
   Web search (Google CSE)
   ============================= */
function blockedByDomain(host: string): boolean {
  const h = host.toLowerCase();
  return BLOCKED_HOST_PARTS.some((p) => h.includes(p));
}

function isIndexLikeUrl(url: string): boolean {
  try {
    const u = new URL(url);
    const p = u.pathname.toLowerCase();
    if (p === "/" || p === "") return true;
    if (INDEX_PATH_HINTS.some((hint) => p.includes(hint))) return true;
    if (/\/(archive|archives|index|home|latest)\/?$/i.test(p)) return true;
    return false;
  } catch {
    return false;
  }
}

function isLikelyHomepage(url: string): boolean {
  try {
    const u = new URL(url);
    const p = u.pathname.replace(/\/+$/, "");
    return p === "" || p === "/";
  } catch {
    return false;
  }
}

function urlLooksArticleLike(url: string): boolean {
  try {
    const u = new URL(url);
    const p = u.pathname.toLowerCase();
    if (DATE_PATH_RE.test(p)) return true;
    const hyphenWords = p.split("/").pop()?.split("-").filter(Boolean) ?? [];
    if (hyphenWords.length >= 4) return true;
    if (ARTICLE_WORD_HINTS.some((w) => p.includes(w))) return true;
    return p.split("/").filter(Boolean).length >= 3;
  } catch {
    return false;
  }
}

function looksSchemaArticle(html: string): boolean {
  return /"@type"\s*:\s*"(Article|NewsArticle|BlogPosting)"/i.test(html);
}
function ogTypeIsArticle(html: string): boolean {
  return /<meta[^>]+property=["']og:type["'][^>]+content=["']article["']/i.test(html);
}
function paragraphCountFromHtml(html: string): number {
  const matches = html.match(/<p[\s>]/gi);
  return matches ? matches.length : 0;
}
function stripHtmlToText(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
function textLengthOk(text: string): boolean {
  return text.length >= 800; // basic “has substance”
}
function authorAppearsAsByline(html: string, author: string): boolean {
  const lower = html.toLowerCase();
  const authorLower = author.toLowerCase();
  if (new RegExp(`<meta[^>]+name=["']author["'][^>]+content=["'][^"']*${authorLower}[^"']*["']`, "i").test(lower)) {
    return true;
  }
  if (new RegExp(`<meta[^>]+property=["']article:author["'][^>]+content=["'][^"']*${authorLower}[^"']*["']`, "i").test(lower)) {
    return true;
  }
  if (new RegExp(`>\\s*(by|by:)\\s*${authorLower}\\b`).test(lower)) return true;
  // JSON-LD author
  if (new RegExp(`"author"\\s*:\\s*\\{[^}]*"name"\\s*:\\s*"[^"]*${authorLower}[^"]*"`, "i").test(lower)) return true;
  return false;
}
function titleHasAuthor(author: string, title?: string): boolean {
  if (!title) return false;
  const t = tokens(title);
  const a = tokens(author);
  return jaccard(a, t) >= 0.35 || containsAll(t, a);
}
function contentHasInterviewSignals(text: string): boolean {
  const lower = text.toLowerCase();
  return (
    /q\s*&\s*a/.test(lower) ||
    /\bq\/a\b/.test(lower) ||
    /\bin conversation\b/.test(lower) ||
    /\binterview\b/.test(lower) ||
    /\bconversation with\b/.test(lower)
  );
}
function isShoppingLike(url: string, html?: string): boolean {
  const h = hostOf(url);
  if (blockedByDomain(h)) {
    if (h.includes("amazon.") || h.includes("amzn.to")) return true;
    if (h.includes("barnesandnoble") || h.includes("bookshop.org")) return true;
    if (h.includes("walmart") || h.includes("target.com")) return true;
    if (h.includes("audible.com")) return true;
    if (h.includes("apps.apple.com")) return true;
  }
  if (html) {
    if (/(add to cart|buy now|add-to-cart|cart)/i.test(html)) return true;
    if (/(price|isbn[:\s])/i.test(html) && /<button/i.test(html)) return true;
  }
  return false;
}

function extractPublishDateISO(html: string): string | undefined {
  // JSON-LD datePublished
  const m1 = html.match(/"datePublished"\s*:\s*"([^"]+)"/i)?.[1];
  if (m1) return new Date(m1).toISOString();

  // meta article:published_time
  const m2 = html.match(/<meta[^>]+property=["']article:published_time["'][^>]+content=["']([^"']+)["']/i)?.[1];
  if (m2) return new Date(m2).toISOString();

  // time datetime
  const m3 = html.match(/<time[^>]+datetime=["']([^"']+)["']/i)?.[1];
  if (m3) return new Date(m3).toISOString();

  return undefined;
}

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
  if (authorMatch) {
    score += 0.35;
    reasons.push("author_in_title_or_snippet");
  }

  // book presence (when provided)
  if (tBook.length) {
    const bookMatch = jaccard(tBook, tCombined) >= 0.3 || containsAll(tCombined, tBook);
    if (bookMatch) {
      score += 0.35;
      reasons.push("book_in_title_or_snippet");
    }
  }

  // domain block filtering handled later; small bonus if not blocked (kept neutral here)

  // No explicit recency here; we’ll check against page date if present
  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

async function webSearchCSE(authorName: string, bookTitle: string, lookbackDays: number): Promise<WebHit[]> {
  if (!USE_SEARCH) return [];
  const quotedAuthor = `"${authorName}"`;
  const quotedBook = bookTitle ? ` "${bookTitle}"` : "";
  const q = `${quotedAuthor}${quotedBook}`;
  const dateRestrict = `d${Math.max(1, Math.min(365, lookbackDays))}`;

  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_CX);
  url.searchParams.set("q", q);
  url.searchParams.set("num", String(SEARCH_MAX_RESULTS));
  url.searchParams.set("dateRestrict", dateRestrict);

  const resp = await fetchWithTimeout(url.toString(), undefined, FETCH_TIMEOUT_MS);
  if (!resp?.ok) return [];
  const data: any = await resp.json();
  const items: any[] = Array.isArray(data?.items) ? data.items : [];

  const hits: WebHit[] = items.map((it: any) => {
    const title = String(it.title || "");
    const link = String(it.link || "");
    const snippet = String(it.snippet || "");
    const pagemap = it.pagemap || {};
    // Guess date from datePublished in metatags or similar
    const meta = Array.isArray(pagemap?.metatags) && pagemap.metatags.length ? pagemap.metatags[0] : undefined;
    const publishedAt =
      meta?.["article:published_time"] ||
      meta?.["og:updated_time"] ||
      meta?.["og:published_time"] ||
      undefined;
    const host = hostOf(link);
    return { title, url: link, snippet, publishedAt, host, confidence: 0, reason: [] };
  });

  return hits.map((h) => scoreWebHit(h, authorName, bookTitle, lookbackDays));
}

/* ===== Page fetch + content checks ===== */
async function fetchPageText(url: string): Promise<{ html?: string; text?: string }> {
  try {
    const r = await fetchWithTimeout(url, undefined, FETCH_TIMEOUT_MS);
    if (!r?.ok) return {};
    const html = await r.text();
    const text = stripHtmlToText(html);
    return { html, text };
  } catch {
    return {};
  }
}

function boostIfContentMatches(hit: WebHit, author: string, bookTitle: string, text: string): WebHit {
  const reasons = new Set<string>(hit.reason || []);
  let score = hit.confidence || 0;

  const tText = tokens(text);
  const tAuthor = tokens(author);
  const tBook = tokens(bookTitle);

  const authorInBody = tAuthor.length && (jaccard(tAuthor, tText) >= 0.25 || containsAll(tText, tAuthor));
  if (authorInBody) {
    score += 0.2;
    reasons.add("content_contains_author");
  }

  if (tBook.length) {
    const bookInBody = jaccard(tBook, tText) >= 0.25 || containsAll(tText, tBook);
    if (bookInBody) {
      score += 0.2;
      reasons.add("content_contains_book");
    }
    if (authorInBody && bookInBody) {
      score += 0.1;
      reasons.add("content_contains_author_and_book");
    }
  }

  // Interview bias
  if (contentHasInterviewSignals(text)) {
    score += 0.08;
    reasons.add("interview_signal");
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = Array.from(reasons);
  return hit;
}

function acceptIfAuthorBylineOrFeatured(params: {
  hit: WebHit;
  author: string;
  bookTitle: string;
  html: string;
  text: string;
  minSearchConfidence: number;
  lookback: number;
  allowNoBook: boolean;
}): { accept: boolean; boosted: WebHit; pageISO?: string } {
  const { hit, author, bookTitle, html, text, minSearchConfidence, lookback, allowNoBook } = params;

  const boosted = boostIfContentMatches(hit, author, bookTitle, text);

  // Blocklists
  if (blockedByDomain(hostOf(hit.url))) return { accept: false, boosted };
  if (isShoppingLike(hit.url, html)) return { accept: false, boosted };
  if (isLikelyHomepage(hit.url) || isIndexLikeUrl(hit.url) || !urlLooksArticleLike(hit.url)) {
    return { accept: false, boosted };
  }

  // Article-likeness
  const articleLike =
    looksSchemaArticle(html) || ogTypeIsArticle(html) || paragraphCountFromHtml(html) >= 3 || textLengthOk(text);
  if (!articleLike) return { accept: false, boosted };

  // Date freshness
  const pageISO = extractPublishDateISO(html);
  if (pageISO) {
    const d = new Date(pageISO);
    if (isFinite(d.getTime()) && daysAgo(d) > lookback) return { accept: false, boosted };
  }

  // Acceptance tiers
  const bylineOK = authorAppearsAsByline(html, author);
  const hasAuthorInTitle = titleHasAuthor(author, hit.title);
  const hasInterview = contentHasInterviewSignals(text);

  // When a bookTitle is provided, require its presence unless allowNoBook is true
  const needBook = !!bookTitle && !allowNoBook;
  const hasBookSignal =
    !bookTitle ||
    tokens(bookTitle).length === 0 ||
    tokens(text).includes(tokens(bookTitle)[0]) ||
    boosted.reason.includes("content_contains_book") ||
    boosted.reason.includes("book_in_title_or_snippet");

  // Tier A: byline match (content BY the author)
  if (bylineOK && boosted.confidence >= minSearchConfidence) {
    boosted.reason = Array.from(new Set([...(boosted.reason || []), "accept_byline"]));
    if (!needBook || hasBookSignal) return { accept: true, boosted, pageISO };
  }

  // Tier B: featured in title or interview shape
  if ((hasAuthorInTitle || hasInterview) && boosted.confidence >= Math.max(0, minSearchConfidence - 0.05)) {
    boosted.reason = Array.from(new Set([...(boosted.reason || []), "accept_featured_title"]));
    if (!needBook || hasBookSignal) return { accept: true, boosted, pageISO };
  }

  // Tier C: strong body match of both author + book (when book provided)
  if (bookTitle && boosted.reason.includes("content_contains_author_and_book")) {
    boosted.reason = Array.from(new Set([...(boosted.reason || []), "accept_topical_body"]));
    if (boosted.confidence >= Math.max(0, minSearchConfidence - 0.05)) {
      return { accept: true, boosted, pageISO };
    }
  }

  return { accept: false, boosted };
}

async function webSearchBest(author: string, bookTitle: string, lookback: number): Promise<WebHit[]> {
  const hits = await webSearchCSE(author, bookTitle, lookback);
  // Filter obvious trash early
  const filtered = hits.filter((h: WebHit) => {
    if (!h.url || !isHttpUrl(h.url)) return false;
    const host = hostOf(h.url);
    if (blockedByDomain(host)) return false;
    if (isLikelyHomepage(h.url) || isIndexLikeUrl(h.url)) return false;
    return true;
  });
  // Keep order by confidence (desc)
  return filtered.sort((a: WebHit, b: WebHit) => b.confidence - a.confidence).slice(0, SEARCH_MAX_RESULTS);
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
    const requireBookMatch: boolean = body.require_book_match === true;
    const fallbackAuthorOnly: boolean = body.fallback_author_only === true;

    const minConfidence: number =
      typeof body.min_confidence === "number" ? Math.max(0, Math.min(1, body.min_confidence)) : DEFAULT_MIN_CONFIDENCE;

    const includeSearch: boolean = body.include_search === true;
    const minSearchConfidence: number =
      typeof body.min_search_confidence === "number"
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
      isbn,
    };

    const cacheKey = makeCacheKey(
      author,
      knownUrls,
      lookback,
      hints,
      strictAuthorMatch,
      minConfidence,
      bookTitle,
      publisher,
      isbn,
      includeSearch,
      minSearchConfidence,
      requireBookMatch,
      fallbackAuthorOnly
    );
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // ---------- WEB SEARCH: primary pass (author + book) ----------
    let bestSearch: WebHit | null = null;
    if (includeSearch && USE_SEARCH) {
      const hits = await webSearchBest(author, bookTitle, lookback);
      for (const cand of hits) {
        const { html, text } = await fetchPageText(cand.url);
        if (!html || !text) continue;

        const verdict = acceptIfAuthorBylineOrFeatured({
          hit: cand,
          author,
          bookTitle,
          html,
          text,
          minSearchConfidence,
          lookback,
          allowNoBook: !requireBookMatch, // if require_book_match is false, allow acceptance without explicit book presence
        });

        if (verdict.accept) {
          const pageISO = verdict.pageISO;
          // If we have a date, ensure within lookback
          let within = true;
          if (pageISO) {
            const d = new Date(pageISO);
            within = isFinite(d.getTime()) && daysAgo(d) <= lookback;
          }
          if (within) {
            bestSearch = verdict.boosted;
            break;
          }
        }
      }
    }

    // ---------- Fallback: author-only pass (interview/feature acceptance) ----------
    if (!bestSearch && includeSearch && USE_SEARCH && fallbackAuthorOnly) {
      const hits2 = await webSearchBest(author, "", lookback);
      for (const cand of hits2) {
        const { html, text } = await fetchPageText(cand.url);
        if (!html || !text) continue;

        const verdict = acceptIfAuthorBylineOrFeatured({
          hit: cand,
          author,
          bookTitle: "", // author-only
          html,
          text,
          minSearchConfidence,
          lookback,
          allowNoBook: true,
        });

        if (verdict.accept) {
          const pageISO = verdict.pageISO;
          let within = true;
          if (pageISO) {
            const d = new Date(pageISO);
            within = isFinite(d.getTime()) && daysAgo(d) <= lookback;
          }
          if (within) {
            // Tag the reason for transparency
            verdict.boosted.reason = Array.from(new Set([...(verdict.boosted.reason || []), "accept_featured_fallback"]));
            bestSearch = verdict.boosted;
            break;
          }
        }
      }
    }

    // If web produced a confident, acceptable hit, return it
    if (bestSearch) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: bestSearch.title,
        latest_url: bestSearch.url,
        source: "web",
        author_url: `https://${bestSearch.host}`,
      };
      if (debug) {
        payload._debug = [
          {
            feedUrl: bestSearch.url,
            ok: true,
            source: "web",
            latest: null,
            recentWithinWindow: true,
            confidence: Number((bestSearch.confidence || 0).toFixed(2)),
            reason: bestSearch.reason || [],
            siteTitle: null,
            feedTitle: null,
            error: null,
          },
        ];
      }
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // ---------- RSS fallback (best-effort) ----------
    const baseFeeds = await findFeeds(author, knownUrls, hints);
    const feeds = Array.from(new Set(baseFeeds));
    const { choice, results } = await evaluateFeeds(feeds, lookback, ctx);

    // Strict author matching gate only applies to RSS path
    if (strictAuthorMatch && (!choice || choice.confidence < minConfidence)) {
      const out = { error: "no_confident_author_match" };
      setCached(cacheKey, out as any);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(out);
    }

    const buildDebug = (list: EvalResult[]) =>
      list.slice(0, 5).map((r: EvalResult) => ({
        feedUrl: r.feedUrl,
        ok: r.ok,
        source: r.source,
        latest: r.latest ? r.latest.date.toISOString() : null,
        recentWithinWindow: r.recentWithinWindow,
        confidence: Number(r.confidence.toFixed(2)),
        reason: r.reason,
        siteTitle: r.siteTitle || null,
        feedTitle: r.feedTitle || null,
        error: r.error || null,
      }));

    if (choice && choice.recentWithinWindow && choice.latest) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: choice.latest.title,
        latest_url: choice.latest.link,
        published_at: choice.latest.date.toISOString(),
        source: choice.source,
        author_url: choice.authorUrl,
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
        author_url: choice.authorUrl,
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
