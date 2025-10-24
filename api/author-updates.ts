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

/* =============================
   Tunables
   ============================= */
const LOOKBACK_DEFAULT = 30; // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 365;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 6500; // per network call
const USER_AGENT = "AuthorUpdates/2.0 (+https://example.com)";
const CONCURRENCY = 4; // feed checks in parallel (bounded)
const MAX_FEED_CANDIDATES = 15; // latency guard
const MAX_KNOWN_URLS = 5; // abuse guard

/* ===== Google Custom Search (CSE) ===== */
const USE_SEARCH = !!(process.env.GOOGLE_CSE_KEY && process.env.GOOGLE_CSE_CX);
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_CX = process.env.GOOGLE_CSE_CX || "";
// pull more results and paginate to widen candidate set
const SEARCH_MAX_RESULTS = 20; // results per page
const SEARCH_PAGES = 2; // total pages to fetch (max ~40)

/* ===== Identity / acceptance tunables ===== */
const DEFAULT_MIN_CONFIDENCE = 0.6; // for RSS identity scoring
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.55; // for CSE hit acceptance

// Block hosts that commonly produce social, shopping, podcast, or index pages
const BLOCKED_HOST_PARTS = [
  // Social
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "threads.net",
  "linkedin.com",
  "youtube.com",
  "youtu.be",
  "reddit.com",

  // Shopping / stores / audio books
  "amazon.",
  "barnesandnoble.com",
  "bookshop.org",
  "walmart.com",
  "target.com",
  "ebay.",
  "audible.com",

  // Podcasts / audio platforms
  "podcasts.apple.com",
  "apple.com", // podcast pages
  "open.spotify.com",
  "spotify.com",
  "soundcloud.com",
  "stitcher.com",
  "iheart.com",

  // Newsletter platforms (often not “third-party article” content for our use)
  "substack.com",
];

// URLs that look like non-article indexes we should avoid
const NON_ARTICLE_PATH_HINTS = [
  "/tag/",
  "/tags/",
  "/topic/",
  "/topics/",
  "/category/",
  "/catalog/",
  "/author/",
  "/authors/",
  "/page/",
  "/pages/",
  "/search?",
  "/shop",
  "/store",
  "/product",
  "/products",
  "/collections",
  "/podcast",
];

// words that hint interview/feature pieces
const INTERVIEW_HINTS = [
  "interview",
  "in conversation",
  "conversation",
  "q&a",
  "questions and answers",
  "in-conversation",
];

/* =============================
   Types
   ============================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string; // "rss" | "substack" | "medium" | "web" | etc.
  author_url?: string; // best site/feed homepage
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
  handle?: string; // e.g., substack/medium handle
  feed_url?: string; // exact feed, if known
  site_url?: string; // base site, if known
};

type FeedItem = { title: string; link: string; date: Date };

type Context = {
  authorName: string;
  knownHosts: string[]; // from known_urls
  bookTitleTokens: string[]; // from book_title
  publisherTokens: string[]; // from publisher
  isbn?: string; // raw isbn string if provided
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
  confidence: number; // 0..1
  reason: string[]; // why score reached value
  error?: string;
};

type WebHit = {
  title: string;
  url: string;
  snippet?: string;
  publishedAt?: string; // ISO
  host: string;
  confidence: number; // 0..1
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
  const urlKey = urls
    .map((u) => u.trim().toLowerCase())
    .filter(Boolean)
    .sort()
    .join("|");
  const hintKey = hints
    ? JSON.stringify(
        Object.keys(hints as Record<string, unknown>)
          .sort()
          .reduce(
            (o: any, k: string) => ((o[k] = (hints as any)[k]), o),
            {}
          )
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
    fallbackAuthorOnly ? "fallbackOn" : "fallbackOff",
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
function normalizeAuthor(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
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
  const links = Array.from(
    html.matchAll(/<link[^>]+rel=["']alternate["'][^>]*>/gi)
  ).map((m) => m[0]);
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
async function fetchWithTimeout(
  url: string,
  init?: RequestInit,
  ms = FETCH_TIMEOUT_MS
) {
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
  const raw: any[] = (feed as any)?.items ?? [];
  const items: FeedItem[] = raw
    .map((it: any) => {
      const d = new Date(
        it.isoDate || it.pubDate || it.published || it.date || 0
      );
      return { title: String(it.title ?? ""), link: String(it.link ?? ""), date: d };
    })
    .filter((x: FeedItem) => x.link && !isNaN(x.date.getTime()))
    .sort(
      (a: FeedItem, b: FeedItem) => b.date.getTime() - a.date.getTime()
    );
  return items.length ? items[0] : null;
}

/* =============================
   Light HTML inspectors (article checks)
   ============================= */
function isBlockedHost(host: string): boolean {
  const h = host.toLowerCase();
  return BLOCKED_HOST_PARTS.some((b) => h.includes(b));
}

function isLikelyArticleURL(u: URL): boolean {
  const p = u.pathname.toLowerCase();
  if (!p || p === "/" || p.length < 2) return false;
  if (NON_ARTICLE_PATH_HINTS.some((hint) => p.includes(hint))) return false;

  // Likely article patterns: dated paths or deep slugs
  const dated = /\b(20\d{2})\b/.test(p) || /\/(news|books|opinion|culture)\//.test(p);
  const deepSlug = p.split("/").filter(Boolean).length >= 3;
  return dated || deepSlug;
}

function pickMeta(content: string, names: string[]): string | undefined {
  for (const n of names) {
    const m =
      new RegExp(
        `<meta[^>]+(?:name|property)=["']${n}["'][^>]+content=["']([^"']+)["']`,
        "i"
      ).exec(content) || undefined;
    if (m?.[1]) return m[1].trim();
  }
  return undefined;
}

function extractPublishedISO(html: string): string | undefined {
  const metas = [
    "article:published_time",
    "og:updated_time",
    "og:published_time",
    "date",
    "dc.date",
  ];
  const m = pickMeta(html, metas);
  if (m) {
    const d = new Date(m);
    if (!isNaN(d.getTime())) return d.toISOString();
  }
  // try <time datetime="">
  const t = /<time[^>]+datetime=["']([^"']+)["'][^>]*>/i.exec(html)?.[1];
  if (t) {
    const d = new Date(t);
    if (!isNaN(d.getTime())) return d.toISOString();
  }
  return undefined;
}

function textHasAll(hay: string, needles: string[]): boolean {
  const lower = hay.toLowerCase();
  return needles.every((n) => lower.includes(n));
}

function textHasAny(hay: string, needles: string[]): boolean {
  const lower = hay.toLowerCase();
  return needles.some((n) => lower.includes(n));
}

function interviewy(title: string): boolean {
  const t = title.toLowerCase();
  return INTERVIEW_HINTS.some((h) => t.includes(h));
}

function isArticleByMeta(html: string): boolean {
  const ogType =
    pickMeta(html, ["og:type"])?.toLowerCase().includes("article") || false;
  const schemaArticle =
    /"@type"\s*:\s*"(NewsArticle|Article|BlogPosting)"/i.test(html);
  const hasArticleTag = /<article[\s>]/i.test(html);
  const hasManyPs = (html.match(/<p[\s>]/gi) || []).length >= 5;
  return ogType || schemaArticle || hasArticleTag || hasManyPs;
}

async function fetchAndInspectPage(
  url: string,
  authorName: string,
  bookTitle: string,
  lookback: number,
  requireBook: boolean
): Promise<{ accept: boolean; publishedAt?: string; reason: string[] }> {
  try {
    const u = new URL(url);
    if (isBlockedHost(u.host)) return { accept: false, reason: ["blocked_host"] };
    if (!isLikelyArticleURL(u)) return { accept: false, reason: ["not_articleish_url"] };

    const resp = await fetchWithTimeout(url, undefined, FETCH_TIMEOUT_MS);
    if (!resp?.ok) return { accept: false, reason: ["fetch_failed"] };

    // cap text to ~300KB to keep lightweight
    const html = (await resp.text()).slice(0, 300_000);
    const title = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1]?.trim() || "";

    // Published time
    const publishedISO = extractPublishedISO(html);
    if (publishedISO) {
      const d = new Date(publishedISO);
      if (isNaN(d.getTime()) || daysAgo(d) > lookback) {
        return { accept: false, reason: ["stale_or_bad_date"] };
      }
    } else {
      // Without a date we can't confirm recency ⇒ reject
      return { accept: false, reason: ["no_publish_date"] };
    }

    // Article-ish check
    if (!isArticleByMeta(html)) {
      return { accept: false, reason: ["not_articleish_meta"] };
    }

    // Content mentions
    const tAuthor = tokens(authorName);
    const tBook = tokens(bookTitle);
    const haySmall =
      (title + " " + pickMeta(html, ["og:description", "description"]) || "")
        .toLowerCase();
    const hayLarge = (title + " " + html).toLowerCase(); // fallback

    const authorInSmall =
      tAuthor.length > 0 && containsAll(tokens(haySmall), tAuthor);
    const authorInLarge =
      tAuthor.length > 0 && containsAll(tokens(hayLarge.slice(0, 20000)), tAuthor);

    const bookInSmall =
      tBook.length > 0 && containsAll(tokens(haySmall), tBook);
    const bookInLarge =
      tBook.length > 0 && containsAll(tokens(hayLarge.slice(0, 20000)), tBook);

    const authorOK = authorInSmall || authorInLarge;
    const bookOK = tBook.length === 0 || bookInSmall || bookInLarge;

    if (requireBook) {
      if (authorOK && bookOK) {
        return { accept: true, publishedAt: publishedISO, reason: ["author_and_book_in_text"] };
      }
      // If book is required but not present, still allow classic interview/feature patterns with strong author presence?
      if (authorOK && interviewy(title)) {
        return { accept: true, publishedAt: publishedISO, reason: ["interviewish_title_author_ok"] };
      }
      return { accept: false, reason: ["missing_book_match"] };
    } else {
      if (authorOK) {
        return { accept: true, publishedAt: publishedISO, reason: ["author_ok"] };
      }
      if (interviewy(title)) {
        return { accept: true, publishedAt: publishedISO, reason: ["interviewish_title"] };
      }
      return { accept: false, reason: ["missing_author_match"] };
    }
  } catch {
    return { accept: false, reason: ["inspect_exception"] };
  }
}

/* =============================
   Feed candidates (RSS)
   ============================= */
async function findFeeds(
  author: string,
  knownUrls: string[] = [],
  hints?: PlatformHints
): Promise<string[]> {
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
   Identity scoring (RSS) + evaluation
   ============================= */
async function fetchSiteTitle(url: string): Promise<string | null> {
  try {
    const origin = new URL(url).origin;
    const res = await fetchWithTimeout(origin);
    if (!res?.ok) return null;
    const html = await res.text();
    const mTitle = html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim();
    const mOG =
      html
        .match(
          /<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)["']/i
        )
        ?.[1]
        ?.trim() || undefined;
    return mOG || mTitle || null;
  } catch {
    return null;
  }
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

async function evaluateFeed(
  feedUrl: string,
  lookback: number,
  ctx: Context
): Promise<EvalResult> {
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
      (feed as any)?.link?.startsWith?.("http")
        ? (feed as any).link
        : new URL(feedUrl).origin;
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
    if (hostMatch) {
      score += 0.35;
      reasons.push(`host_match:${host}`);
    }

    // 2) Name appears in site or feed title
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

    // 3) Strong string inclusion
    if (startsWithAll(siteTokens, authorTokens) || containsAll(siteTokens, authorTokens)) {
      score += 0.15;
      reasons.push("site_contains_author");
    }
    if (containsAll(feedTokens, authorTokens)) {
      score += 0.15;
      reasons.push("feed_contains_author");
    }

    // 4) Platform handle similarity
    if (host.endsWith("substack.com") || host.endsWith("medium.com")) {
      const subdomain = host.split(".").slice(0, -2).join(".");
      const handleTokens = tokens(subdomain.replace(/^@/, ""));
      const jHandle = jaccard(authorTokens, handleTokens);
      if (jHandle >= 0.5) {
        score += 0.2;
        reasons.push(`handle_sim:${jHandle.toFixed(2)}`);
      }
    }

    // 5) Book/Publisher tokens in latest ITEM text (title + summary/body)
    const latestText = latest ? itemText((feed as any).items?.[0] ?? {}) : "";
    const contentTokens = tokens(
      [
        latest?.title || "",
        latestText || "",
        feedTitle || "",
        siteTitle || "",
      ].join(" ")
    );

    const hasBook = ctx.bookTitleTokens.length
      ? jaccard(ctx.bookTitleTokens, contentTokens) >= 0.2 ||
        containsAll(contentTokens, ctx.bookTitleTokens)
      : false;

    const hasPub = ctx.publisherTokens.length
      ? jaccard(ctx.publisherTokens, contentTokens) >= 0.2
      : false;

    const authorInItem = authorTokens.length
      ? jaccard(authorTokens, contentTokens) >= 0.3 ||
        containsAll(contentTokens, authorTokens)
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

async function evaluateFeeds(
  feeds: string[],
  lookback: number,
  ctx: Context
) {
  const limit = pLimit(CONCURRENCY);
  const results: EvalResult[] = await Promise.all(
    feeds.map((f) => limit(() => evaluateFeed(f, lookback, ctx)))
  );

  // Any OK result
  const anyOk = results.find((r: EvalResult) => r.ok);
  if (anyOk) return { choice: anyOk, results };

  return { choice: null as any, results };
}

/* =============================
   Web Search (Google CSE)
   ============================= */
function scoreWebHit(
  hit: WebHit,
  authorName: string,
  bookTitle: string,
  lookbackDays: number
): WebHit {
  const reasons: string[] = [];
  let score = 0;

  const tTitle = tokens(hit.title || "");
  const tSnippet = tokens(hit.snippet || "");
  const tCombined = Array.from(new Set([...tTitle, ...tSnippet]));
  const tAuthor = tokens(authorName);
  const tBook = tokens(bookTitle);

  // author presence
  const authorMatch =
    tAuthor.length &&
    (jaccard(tAuthor, tCombined) >= 0.3 || containsAll(tCombined, tAuthor));
  if (authorMatch) {
    score += 0.35;
    reasons.push("author_in_title_or_snippet");
  }

  // book presence
  const bookMatch =
    tBook.length &&
    (jaccard(tBook, tCombined) >= 0.3 || containsAll(tCombined, tBook));
  if (bookMatch) {
    score += 0.35;
    reasons.push("book_in_title_or_snippet");
  }

  // recency bonus
  if (hit.publishedAt) {
    const d = new Date(hit.publishedAt);
    if (!isNaN(d.getTime()) && daysAgo(d) <= lookbackDays) {
      score += 0.25;
      reasons.push("fresh_within_window");
    }
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

function cseExcludeQuery(): string {
  const roots = Array.from(
    new Set(
      BLOCKED_HOST_PARTS.map((h) => h.replace(/^www\./, "")).map((h) =>
        h.includes(".") ? h.split("/")[0] : h
      )
    )
  );
  return roots.map((d) => `-site:${d}`).join(" ");
}

async function webSearchCSE(
  authorName: string,
  bookTitle: string,
  lookbackDays: number
): Promise<WebHit[]> {
  if (!USE_SEARCH) return [];

  const dateRestrict = `d${Math.max(1, Math.min(365, lookbackDays))}`;
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
        const meta =
          Array.isArray(pagemap?.metatags) && pagemap.metatags.length
            ? pagemap.metatags[0]
            : undefined;
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

  const scored = all
    .map((h) => scoreWebHit(h, authorName, bookTitle, lookbackDays))
    .reduce((acc: WebHit[], h) => (acc.some((x) => x.url === h.url) ? acc : acc.concat(h)), [])
    .sort((a: WebHit, b: WebHit) => b.confidence - a.confidence);

  return scored;
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
    knownUrls = knownUrls
      .map((u: unknown) => String(u).trim())
      .filter(isHttpUrl)
      .slice(0, MAX_KNOWN_URLS);

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
      typeof body.min_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_confidence))
        : DEFAULT_MIN_CONFIDENCE;

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

    /* =============== Web search path =============== */
    let bestPayload: CacheValue | null = null;
    const debugEntries: CacheValue["_debug"] = [];

    if (includeSearch) {
      const hits: WebHit[] = await webSearchCSE(author, bookTitle, lookback);

      // Pass A: require book + author
      for (const hit of hits) {
        if (hit.confidence < minSearchConfidence) continue;
        const inspect = await fetchAndInspectPage(
          hit.url,
          author,
          bookTitle,
          lookback,
          /* requireBook */ requireBookMatch
        );
        if (inspect.accept) {
          bestPayload = {
            author_name: author,
            has_recent: true,
            latest_title: hit.title,
            latest_url: hit.url,
            published_at: inspect.publishedAt,
            source: "web",
            author_url: `https://${hit.host}`,
          };
          if (debug) {
            debugEntries.push({
              feedUrl: hit.url,
              ok: true,
              source: "web",
              latest: inspect.publishedAt || null,
              recentWithinWindow: true,
              confidence: Number(hit.confidence.toFixed(2)),
              reason: ["cse_pass_a", ...hit.reason],
              siteTitle: null,
              feedTitle: null,
              error: null,
            });
          }
          break;
        } else if (debug) {
          debugEntries.push({
            feedUrl: hit.url,
            ok: false,
            source: "web",
            latest: null,
            recentWithinWindow: false,
            confidence: Number(hit.confidence.toFixed(2)),
            reason: ["cse_pass_a_reject", ...hit.reason, ...inspect.reason],
            siteTitle: null,
            feedTitle: null,
            error: null,
          });
        }
      }

      // Pass B: fallback author-only (interviews/features) if requested and not found yet
      if (!bestPayload && fallbackAuthorOnly) {
        for (const hit of hits) {
          if (hit.confidence < minSearchConfidence) continue;
          const inspect = await fetchAndInspectPage(
            hit.url,
            author,
            /* bookTitle */ "",
            lookback,
            /* requireBook */ false
          );
          if (inspect.accept) {
            bestPayload = {
              author_name: author,
              has_recent: true,
              latest_title: hit.title,
              latest_url: hit.url,
              published_at: inspect.publishedAt,
              source: "web",
              author_url: `https://${hit.host}`,
            };
            if (debug) {
              debugEntries.push({
                feedUrl: hit.url,
                ok: true,
                source: "web",
                latest: inspect.publishedAt || null,
                recentWithinWindow: true,
                confidence: Number(hit.confidence.toFixed(2)),
                reason: ["cse_pass_b_fallback", ...hit.reason],
                siteTitle: null,
                feedTitle: null,
                error: null,
              });
            }
            break;
          } else if (debug) {
            debugEntries.push({
              feedUrl: hit.url,
              ok: false,
              source: "web",
              latest: null,
              recentWithinWindow: false,
              confidence: Number(hit.confidence.toFixed(2)),
              reason: ["cse_pass_b_reject", ...hit.reason, ...inspect.reason],
              siteTitle: null,
              feedTitle: null,
              error: null,
            });
          }
        }
      }
    }

    if (bestPayload) {
      if (debug) bestPayload._debug = debugEntries.slice(0, 8);
      setCached(cacheKey, bestPayload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(bestPayload);
    }

    /* =============== RSS fallback (best-effort) =============== */
    const feeds = await findFeeds(author, knownUrls, hints);
    const { choice, results } = await evaluateFeeds(feeds, lookback, ctx);

    // Strict author matching gate for RSS (identity score)
    if (strictAuthorMatch && (!choice || choice.confidence < minConfidence)) {
      if (debug) {
        const emptyDbg: CacheValue = {
          author_name: author,
          has_recent: false,
          _debug: (results || []).slice(0, 5).map((r: EvalResult) => ({
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
          })),
        };
        setCached(cacheKey, emptyDbg);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json({ error: "no_confident_author_match" });
      }
      return res.status(200).json({ error: "no_confident_author_match" });
    }

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
      if (debug) {
        payload._debug = (results || []).slice(0, 5).map((r: EvalResult) => ({
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
      }
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
      if (debug) {
        payload._debug = (results || []).slice(0, 5).map((r: EvalResult) => ({
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
      }
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Nothing at all
    if (debug) {
      const emptyDbg: CacheValue = {
        author_name: author,
        has_recent: false,
        _debug: (results || []).slice(0, 5).map((r: EvalResult) => ({
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
        })),
      };
      setCached(cacheKey, emptyDbg);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(emptyDbg);
    }

    setCached(cacheKey, { author_name: author, has_recent: false });
    res.setHeader("x-cache", "MISS");
    return res.status(200).json({ author_name: author, has_recent: false });
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
