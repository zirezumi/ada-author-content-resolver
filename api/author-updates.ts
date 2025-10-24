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
const LOOKBACK_DEFAULT = 30;                  // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 90;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;     // 24h
const FETCH_TIMEOUT_MS = 5000;                // per network call
const USER_AGENT = "AuthorUpdates/3.1 (+https://example.com)";
const CONCURRENCY = 4;                        // feed checks in parallel (bounded)
const MAX_FEED_CANDIDATES = 15;               // latency guard
const MAX_KNOWN_URLS = 5;                     // abuse guard

/* ===== Google Programmable Search / CSE ===== */
const USE_SEARCH = !!(process.env.GOOGLE_CSE_KEY && process.env.GOOGLE_CSE_CX);
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.7;
const SEARCH_MAX_RESULTS = 10;

/**
 * Strong global blocklist (negative site operators & runtime filtering)
 * NOTE: per user request, Substack is intentionally NOT blocked.
 */
const SEARCH_BLOCKLIST: string[] = (process.env.SEARCH_BLOCKLIST ||
  [
    // Socials/UGC
    "x.com","twitter.com","facebook.com","instagram.com","tiktok.com","linkedin.com",
    "youtube.com","youtu.be","reddit.com","news.ycombinator.com",
    "medium.com", "substack.com", "spotify.com",
    "quora.com","pinterest.com","tumblr.com","notion.site",
    "producthunt.com","dev.to","hashnode.com","stackexchange.com","stackoverflow.com","kaggle.com",
    "patreon.com","t.me","discord.com","discord.gg",

    // Shopping/marketplaces/booksellers
    "amazon.com","amazon.co.uk","amazon.ca","amazon.com.au","amazon.de","amazon.fr","amazon.es","amazon.it",
    "a.co","amzn.to",
    "ebay.com","walmart.com","target.com","bestbuy.com",
    "barnesandnoble.com","bookshop.org","books.google.com",
    "aliexpress.com","alibaba.com","etsy.com"
  ].join(","))
  .split(",")
  .map((s: string) => s.trim().toLowerCase())
  .filter(Boolean);

/* ===== Identity scoring tunables (RSS fallback) ===== */
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
  publishedAt?: string; // ISO (CSE often lacks this)
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
  const urlKey = urls.map((u) => u.trim().toLowerCase()).filter(Boolean).sort().join("|");
  const hintKey = hints
    ? JSON.stringify(Object.keys(hints as Record<string, unknown>).sort().reduce((o: Record<string, unknown>, k: string) => {
        (o as any)[k] = (hints as any)[k];
        return o;
      }, {}))
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
  const links = Array.from(html.matchAll(/<link[^>]+rel=["']alternate["'][^>]*>/gi)).map((m) => m[0]);
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
    const headers: HeadersInit = { "User-Agent": USER_AGENT, ...(init?.headers as any) };
    const res = await fetch(url, { ...init, headers, signal: ctrl.signal });
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
function jaccard(a: string[] | string, b: string[] | string) {
  const A = new Set(typeof a === "string" ? a.split(" ") : a);
  const B = new Set(typeof b === "string" ? b.split(" ") : b);
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
    it?.summary
  ].filter(Boolean);
  return String(bits.join(" ").slice(0, 5000));
}
function lastName(s: string): string {
  const parts = tokens(s);
  return parts.length ? parts[parts.length - 1] : "";
}

/* =============================
   URL/article heuristics
   ============================= */
function isLikelyHomepage(u: string): boolean {
  try {
    const { pathname, search } = new URL(u);
    const clean = pathname.replace(/\/+$/, "");
    const depth = clean.split("/").filter(Boolean).length;
    return (clean === "" || depth <= 1) && (!search || search === "");
  } catch { return false; }
}
function urlLooksArticleLike(u: string): boolean {
  try {
    const { pathname } = new URL(u);
    const parts = pathname.split("/").filter(Boolean);
    const depth = parts.length;
    const hasDateSeg = /\b(20\d{2})[\/\-]/.test(pathname);
    const longSlug = parts.some((p) => p.length >= 12);
    return depth >= 2 || hasDateSeg || longSlug;
  } catch { return false; }
}
function isIndexLikeUrl(u: string): boolean {
  try {
    const url = new URL(u);
    const p = url.pathname.toLowerCase();
    if (p === "/" || p === "") return true;
    const patterns = [
      "/topic/", "/topics/", "/tag/", "/tags/",
      "/category/", "/categories/", "/section/", "/sections/",
      "/search/", "/archive/", "/author/", "/authors/"
    ];
    if (patterns.some((seg) => p.includes(seg))) return true;
    const qp = Array.from(url.searchParams.keys()).map((k) => k.toLowerCase());
    if (qp.includes("page") || qp.includes("topic") || qp.includes("tag")) return true;
    const depth = p.split("/").filter(Boolean).length;
    if (depth <= 1 && url.search && url.search.length > 0) return true;
    return false;
  } catch { return false; }
}

function extractPublishDateISO(html: string): string | undefined {
  const meta = [
    /<meta[^>]+property=["']article:published_time["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+name=["']pubdate["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+itemprop=["']datePublished["'][^>]+content=["']([^"']+)["']/i,
    /<time[^>]+datetime=["']([^"']+)["']/i,
  ];
  for (const rx of meta) {
    const m = html.match(rx);
    if (m?.[1]) {
      const iso = new Date(m[1]).toISOString();
      if (!isNaN(new Date(iso).getTime())) return iso;
    }
  }
  const m2 = html.match(/\b(20\d{2})[-\/](0[1-9]|1[0-2])[-\/](0[1-9]|[12]\d|3[01])\b/);
  if (m2) {
    const iso = new Date(m2[0]).toISOString();
    if (!isNaN(new Date(iso).getTime())) return iso;
  }
  return undefined;
}

/* =============================
   Byline extraction & verification
   ============================= */
function normalizeSpaces(s: string) {
  return s.replace(/\s+/g, " ").trim();
}
function namesRoughMatch(target: string, candidate: string): boolean {
  const t = tokens(target).join(" ");
  const c = tokens(candidate).join(" ");
  if (!t || !c) return false;
  if (c.includes(t) || t.includes(c)) return true;
  const j = jaccard(t, c);
  return j >= 0.6;
}
function extractBylineFromHtml(html: string): string | null {
  const metas: RegExp[] = [
    /<meta[^>]+name=["']author["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+property=["']article:author["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+name=["']byline["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+name=["']byl["'][^>]+content=["']([^"']+)["']/i, // NYT
    /<meta[^>]+property=["']og:author["'][^>]+content=["']([^"']+)["']/i,
  ];
  for (const rx of metas) {
    const m = html.match(rx);
    if (m?.[1]) return normalizeSpaces(m[1]);
  }

  const scripts = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const tag of scripts) {
    const m = tag.match(/<script[^>]*>([\s\S]*?)<\/script>/i);
    const json = m?.[1]?.trim();
    if (!json) continue;
    try {
      const data = JSON.parse(json);
      const candidates: any[] = Array.isArray(data) ? data : [data];
      for (const obj of candidates) {
        const t = (obj?.["@type"] || obj?.type || "");
        if (/Article|NewsArticle|BlogPosting/i.test(String(t))) {
          const author = (obj as any).author;
          if (typeof author === "string") return normalizeSpaces(author);
          if (author && typeof author === "object") {
            if (Array.isArray(author)) {
              const first = author.find((a: any) => a?.name);
              if (first?.name) return normalizeSpaces(first.name);
            } else if ((author as any).name) {
              return normalizeSpaces((author as any).name);
            }
          }
        }
      }
    } catch { /* ignore */ }
  }

  const bylineMatch = html.match(/>By\s+([A-Z][A-Za-z'.\-]+\s+(?:[A-Z][A-Za-z'.\-]+\s*){0,3})</i);
  if (bylineMatch?.[1]) return normalizeSpaces(bylineMatch[1]);

  return null;
}
function authorAppearsAsByline(html: string, authorName: string): boolean {
  const byline = extractBylineFromHtml(html);
  if (!byline) return false;
  return namesRoughMatch(authorName, byline);
}

/* ===== Article-shape signals ===== */
function looksSchemaArticle(html: string): boolean {
  const scripts = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const tag of scripts) {
    const m = tag.match(/<script[^>]*>([\s\S]*?)<\/script>/i);
    const json = m?.[1]?.trim();
    if (!json) continue;
    try {
      const data = JSON.parse(json);
      const arr = Array.isArray(data) ? data : [data];
      for (const obj of arr) {
        const t = String(obj?.["@type"] || obj?.type || "");
        if (/Article|NewsArticle|BlogPosting/i.test(t)) return true;
      }
    } catch { /* ignore */ }
  }
  return false;
}
function ogTypeIsArticle(html: string): boolean {
  const m = html.match(/<meta[^>]+property=["']og:type["'][^>]+content=["']([^"']+)["']/i)?.[1];
  return !!m && /article/i.test(m);
}
function paragraphCountFromHtml(html: string): number {
  const ps = html.match(/<p\b[^>]*>[\s\S]*?<\/p>/gi);
  return ps ? ps.length : 0;
}
function textLengthOk(text: string, minChars = 1500): boolean {
  return text.replace(/\s+/g, " ").length >= minChars;
}
function contentHasAuthorAndBook(pageText: string, authorName: string, bookTitle: string): boolean {
  const t = tokens(pageText);
  const tAuthor = tokens(authorName);
  const tBook = tokens(bookTitle);
  if (!t.length || !tAuthor.length || !tBook.length) return false;

  const authorOK = jaccard(tAuthor, t) >= 0.25 || containsAll(t, tAuthor);
  const bookOK   = jaccard(tBook, t)   >= 0.25 || containsAll(t, tBook);
  return authorOK && bookOK;
}
function tokensWithinProximity(text: string, a: string[], b: string[], windowSize = 60): boolean {
  const t = tokens(text);
  if (!a.length || !b.length || !t.length) return false;
  const firstA = a[0];
  for (let i = 0; i < t.length; i++) {
    if (t[i] !== firstA) continue;
    const start = Math.max(0, i - windowSize);
    const end = Math.min(t.length, i + a.length + windowSize);
    const window = t.slice(start, end);
    const hasA = containsAll(window, a);
    const hasB = window.includes(b[0]) || jaccard(window, b) >= 0.2;
    if (hasA && hasB) return true;
  }
  return false;
}

/* ===== Shopping/product-page detection ===== */
function isShoppingLike(url: string, html?: string): boolean {
  const u = url.toLowerCase();
  if (
    /\b\/dp\/[a-z0-9]{6,}\b/.test(u) ||                 // amazon /dp/ASIN
    /\b\/gp\/product\b/.test(u) ||                      // amazon /gp/product
    /\/product(s)?\//.test(u) ||                        // generic product slug
    /\/cart\b|\/checkout\b|\/buy\b|\/add\-to\-cart\b/.test(u)
  ) {
    return true;
  }

  if (!html) return false;

  if (/<script[^>]+type=["']application\/ld\+json["'][^>]*>[\s\S]*?"@type"\s*:\s*"Product"/i.test(html)) {
    return true;
  }
  if (/<meta[^>]+property=["']og:type["'][^>]+content=["']product["']/i.test(html)) {
    return true;
  }

  const hasPrice = /itemprop=["']price["']|property=["']product:price["']|>\s*\$\s?\d{1,3}(?:[.,]\d{3})*(?:\.\d{2})?\s*<|Price:/i.test(html);
  const hasAddToCart = /add\s*to\s*cart|buy\s*now|checkout/i.test(html);
  if (hasPrice && hasAddToCart) return true;

  return false;
}

/* =============================
   Feed candidates (RSS fallback)
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
   Identity scoring + evaluation (RSS fallback)
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
  const results: EvalResult[] = await Promise.all(feeds.map((f: string) => limit(() => evaluateFeed(f, lookback, ctx))));

  // 1) Any recent within window? choose highest confidence; tie-break by newest
  const recent = results
    .filter((r: EvalResult) => r.ok && r.recentWithinWindow && r.latest)
    .sort((a: EvalResult, b: EvalResult) =>
      (b.confidence - a.confidence) ||
      (b.latest!.date.getTime() - a.latest!.date.getTime())
    );
  if (recent.length) return { choice: recent[0], results };

  // 2) Otherwise, pick highest confidence overall; tie-break by freshest latest
  const byConf = results
    .filter((r: EvalResult) => r.ok && (r.authorUrl || r.latest))
    .sort((a: EvalResult, b: EvalResult) =>
      (b.confidence - a.confidence) ||
      ((b.latest?.date.getTime() || 0) - (a.latest?.date.getTime() || 0))
    );
  if (byConf.length) return { choice: byConf[0], results };

  // 3) Fallback any OK result
  const anyOk = results.find((r: EvalResult) => r.ok);
  if (anyOk) return { choice: anyOk, results };

  // 4) Nothing usable
  return { choice: null, results };
}

/* =============================
   Google CSE search (no allowlist, strong negatives)
   ============================= */
function negativeSiteOperators(domains: string[]): string {
  return domains.map((d: string) => `-site:${d}`).join(" ");
}
function buildQuery(authorName: string, bookTitle: string, extra?: string) {
  const base = `"${authorName}" "${bookTitle}"`;
  const negatives = negativeSiteOperators(SEARCH_BLOCKLIST);
  return [base, extra, negatives].filter(Boolean).join(" ");
}

function scoreWebHit(hit: WebHit, authorName: string, bookTitle: string): WebHit {
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

  // penalties
  if (isLikelyHomepage(hit.url)) {
    score -= 0.25; reasons.push("penalty_homepage");
  } else if (!urlLooksArticleLike(hit.url)) {
    score -= 0.1; reasons.push("penalty_not_article_like");
  }
  if (isIndexLikeUrl(hit.url)) {
    score -= 0.4; reasons.push("penalty_index_like");
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

async function webSearchBest(authorName: string, bookTitle: string, lookbackDays: number): Promise<WebHit[]> {
  if (!process.env.GOOGLE_CSE_KEY || !process.env.GOOGLE_CSE_CX) return [];
  const dateRestrict = lookbackDays <= 7 ? "d7" : "d30";

  const variants: string[] = [
    buildQuery(authorName, bookTitle),
    buildQuery(authorName, bookTitle, "interview OR conversation OR transcript OR Q&A"),
    buildQuery(authorName, bookTitle, `intitle:"${lastName(authorName)}"`),
    buildQuery(authorName, bookTitle, `intitle:"${bookTitle}"`),
  ];

  for (const q of variants) {
    for (const start of [1, 11, 21]) { // fetch up to 3 pages
      const url = new URL("https://www.googleapis.com/customsearch/v1");
      url.searchParams.set("key", process.env.GOOGLE_CSE_KEY!);
      url.searchParams.set("cx", process.env.GOOGLE_CSE_CX!);
      url.searchParams.set("q", q);
      url.searchParams.set("num", String(SEARCH_MAX_RESULTS));
      url.searchParams.set("dateRestrict", dateRestrict);
      url.searchParams.set("start", String(start));

      const resp = await fetchWithTimeout(url.toString());
      if (!resp?.ok) continue;
      const data: any = await resp.json();
      const items: any[] = Array.isArray(data?.items) ? data.items : [];
      if (!items.length) continue;

      const hits: WebHit[] = items
        .map((it: any): WebHit => {
          const title = String(it.title || "");
          const link = String(it.link || "");
          const snippet = String(it.snippet || "");
          const host = hostOf(link);
          return { title, url: link, snippet, host, publishedAt: undefined, confidence: 0, reason: [] };
        })
        .filter((h: WebHit) => !SEARCH_BLOCKLIST.includes(h.host));

      if (hits.length) {
        const scored: WebHit[] = hits
          .map((h: WebHit) => scoreWebHit(h, authorName, bookTitle))
          .sort((a: WebHit, b: WebHit) => b.confidence - a.confidence)
          .slice(0, SEARCH_MAX_RESULTS);
        if (scored.length) return scored;
      }
    }
  }

  return [];
}

// Fetch page HTML -> plain text for token checks
async function fetchPageText(url: string): Promise<{ html: string; text: string }> {
  try {
    const r = await fetchWithTimeout(url, undefined, FETCH_TIMEOUT_MS);
    if (!r?.ok) return { html: "", text: "" };
    const html = await r.text();
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .slice(0, 200_000);
    return { html, text };
  } catch { return { html: "", text: "" }; }
}

function boostIfContentMatches(hit: WebHit, authorName: string, bookTitle: string, pageText: string): WebHit {
  if (!pageText) return hit;
  const t = tokens(pageText);
  const tAuthor = tokens(authorName);
  const tBook = tokens(bookTitle);
  const hasAuthor = tAuthor.length && (jaccard(tAuthor, t) >= 0.25 || containsAll(t, tAuthor));
  const hasBook = tBook.length && (jaccard(tBook, t) >= 0.25 || containsAll(t, tBook));
  if (hasAuthor && hasBook) {
    hit.confidence = Math.min(1, hit.confidence + 0.35);
    hit.reason = [...hit.reason, "content_contains_author_and_book"];
  } else if (hasAuthor) {
    hit.confidence = Math.min(1, hit.confidence + 0.15);
    hit.reason = [...hit.reason, "content_contains_author"];
  } else if (hasBook) {
    hit.confidence = Math.min(1, hit.confidence + 0.15);
    hit.reason = [...hit.reason, "content_contains_book"];
  }
  return hit;
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

    /* ------------------------------------
       Web search first (CSE), no allowlist
       ------------------------------------ */
    let bestSearch: WebHit | null = null;
    if (includeSearch && USE_SEARCH && bookTitle) {
      const hits = await webSearchBest(author, bookTitle, lookback);

      for (const cand of hits) {
        // Early URL filters
        if (isIndexLikeUrl(cand.url) || isLikelyHomepage(cand.url) || !urlLooksArticleLike(cand.url)) {
          continue;
        }
        // Fetch page
        const { html, text } = await fetchPageText(cand.url);
        if (!html || !text) continue;

        // Block shopping / product pages explicitly
        if (isShoppingLike(cand.url, html)) {
          continue;
        }

        // Article-shape checks
        const articleLike =
          looksSchemaArticle(html) ||
          ogTypeIsArticle(html) ||
          paragraphCountFromHtml(html) >= 3 ||
          textLengthOk(text);

        if (!articleLike) continue;

        // Score boost based on body content
        const boosted = boostIfContentMatches(cand, author, bookTitle, text);

        // Extract publish date & verify byline on page
        const pageISO = extractPublishDateISO(html);
        const pageDate = pageISO ? new Date(pageISO) : undefined;
        const pageFresh = pageDate && !isNaN(pageDate.getTime()) ? daysAgo(pageDate) <= lookback : false;

        const confOK = boosted.confidence >= minSearchConfidence;
        const bylineOK = authorAppearsAsByline(html, author);

        // TIER A: strictly content BY the author (byline present)
        if (bylineOK && confOK && (pageFresh || !pageISO)) {
          boosted.reason = Array.from(new Set([...(boosted.reason || []), "accept_byline"]));
          bestSearch = boosted;
          break; // stop at first acceptable article
        }

        // TIER B: participation (author featured, not credited as author)
        const titleHasLastName = (() => {
          const ln = lastName(author);
          return !!ln && tokens(boosted.title).includes(ln);
        })();

        const participationOK =
          articleLike &&
          pageFresh && // must be within window for Tier B
          titleHasLastName &&
          contentHasAuthorAndBook(text, author, bookTitle) &&
          tokensWithinProximity(text, tokens(author), tokens(bookTitle), 60) &&
          confOK;

        if (participationOK) {
          boosted.reason = Array.from(new Set([...(boosted.reason || []), "accept_participation"]));
          bestSearch = boosted;
          break;
        }
      }
    }

    if (bestSearch) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: bestSearch.title,
        latest_url: bestSearch.url,
        published_at: bestSearch.publishedAt, // often undefined with CSE
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

    /* ------------------------------------
       RSS/Atom fallback with identity scoring
       ------------------------------------ */
    const baseFeeds = await findFeeds(author, knownUrls, hints);
    const feeds = Array.from(new Set(baseFeeds));
    const { choice, results } = await evaluateFeeds(feeds, lookback, ctx);

    if (strictAuthorMatch && (!choice || choice.confidence < minConfidence)) {
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(STRICT_REJECT_PAYLOAD);
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
