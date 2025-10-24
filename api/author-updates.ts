// api/author-updates.ts
/// <reference types="node" />

/**
 * Author Updates Resolver (full-featured)
 *
 * Features:
 * - Node runtime (Vercel) + shared-secret auth (X-Auth)
 * - 24h in-memory cache
 * - RSS candidate discovery & evaluation (rss-parser)
 * - Web search via Google CSE with multi-pass scoring
 * - Identity scoring (author/book/publisher tokens)
 * - Domain blocklist (social, shopping, podcast, video, etc.)
 * - Configurable acceptance thresholds & toggles
 * - Strict boolean coercions for TypeScript
 */

import Parser from "rss-parser";
import pLimit from "p-limit";

/* =========================================================
   RUNTIME
   ========================================================= */
export const config = { runtime: "nodejs" } as const;

/* =========================================================
   AUTH
   ========================================================= */
const AUTH_SECRETS = (process.env.AUTHOR_UPDATES_SECRET || "")
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
  const ok = !!(provided && AUTH_SECRETS.includes(provided));
  if (!ok) {
    res.status(401).json({ error: "unauthorized" });
    return false;
  }
  return true;
}

/* =========================================================
   TUNABLES
   ========================================================= */
const LOOKBACK_DEFAULT = 30; // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 180;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 5000;
const USER_AGENT = "AuthorUpdates/2.0 (+https://example.com)";

const CONCURRENCY = 4;
const MAX_FEED_CANDIDATES = 15;
const MAX_KNOWN_URLS = 5;

const DEFAULT_MIN_CONFIDENCE = 0.6; // feed identity min for strict match
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.55;

/** Block noisy, non-author content (shopping/social/podcast/video). You asked to keep substack in blocklist. */
const BLOCKED_DOMAINS = [
  "amazon.", "open.spotify.com", "spotify.com", "apple.com", "podcasts.apple.com",
  "twitter.com", "x.com", "facebook.com", "instagram.com", "tiktok.com", "linkedin.com",
  "youtube.com", "youtu.be", "substack.com"
];

/* =========================================================
   TYPES
   ========================================================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string;     // "rss" | "web" | "substack" | "medium" | ...
  author_url?: string; // author's home/site
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
  error?: string | null;
};

type WebHit = {
  title: string;
  url: string;
  snippet?: string;
  publishedAt?: string; // ISO if in result
  host: string;
  confidence: number;
  reason: string[];
  accepted: boolean;
  authorParticipation: boolean; // signals explicit author presence
};

/* =========================================================
   CACHE
   ========================================================= */
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(
  author: string,
  urls: string[],
  lookback: number,
  hints?: unknown,
  strict?: boolean,
  requireBook?: boolean,
  fallbackAuthorOnly?: boolean,
  minConf?: number,
  bookTitle?: string,
  publisher?: string,
  isbn?: string,
  includeSearch?: boolean,
  minSearchConf?: number
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
    requireBook ? "requireBook" : "noBookReq",
    fallbackAuthorOnly ? "fallbackAuthor" : "noFallback",
    String(minConf ?? DEFAULT_MIN_CONFIDENCE),
    (bookTitle || "").toLowerCase().trim(),
    (publisher || "").toLowerCase().trim(),
    (isbn || "").replace(/[-\s]/g, ""),
    includeSearch ? "search" : "nosearch",
    String(minSearchConf ?? DEFAULT_MIN_SEARCH_CONFIDENCE),
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

/* =========================================================
   UTILS
   ========================================================= */
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

/* =========================================================
   FEED DISCOVERY
   ========================================================= */
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

async function findFeeds(author: string, knownUrls: string[] = [], hints?: PlatformHints): Promise<string[]> {
  const candidates = new Set<string>();

  if (hints?.feed_url && isHttpUrl(hints.feed_url)) candidates.add(hints.feed_url);
  if (hints?.site_url && isHttpUrl(hints.site_url)) {
    for (const f of guessFeedsFromSite(hints.site_url)) candidates.add(f);
  }

  if (hints?.platform && hints?.handle) {
    const h = hints.handle.replace(/^@/, "");
    if (hints.platform === "substack") candidates.add(`https://${h}.substack.com/feed`);
    if (hints.platform === "medium") {
      candidates.add(`https://medium.com/feed/@${h}`);
      candidates.add(`https://medium.com/feed/${h}`);
    }
  }

  const handles = [
    author.toLowerCase().replace(/\s+/g, ""),
    author.toLowerCase().replace(/\s+/g, "-"),
  ];
  for (const h of handles) {
    candidates.add(`https://${h}.substack.com/feed`);
    candidates.add(`https://medium.com/feed/@${h}`);
    candidates.add(`https://medium.com/feed/${h}`);
  }

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
      // ignore
    }
  }

  return Array.from(candidates).slice(0, MAX_FEED_CANDIDATES);
}

/* =========================================================
   FEED EVALUATION + IDENTITY SCORING
   ========================================================= */
async function fetchSiteTitle(url: string): Promise<string | null> {
  try {
    const origin = new URL(url).origin;
    const res = await fetchWithTimeout(origin);
    if (!res?.ok) return null;
    const html = await res.text();
    const mTitle = html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim();
    const mOG = html.match(/<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)["']/i)?.[1]?.trim();
    return (mOG || mTitle || null);
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
    const contentTokens = tokens(
      [latest?.title || "", latestText || "", feedTitle || "", siteTitle || ""].join(" ")
    );

    const hasBook =
      ctx.bookTitleTokens.length
        ? jaccard(ctx.bookTitleTokens, contentTokens) >= 0.2 ||
          containsAll(contentTokens, ctx.bookTitleTokens)
        : false;

    const hasPub =
      ctx.publisherTokens.length ? jaccard(ctx.publisherTokens, contentTokens) >= 0.2 : false;

    const authorInItem =
      authorTokens.length
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
      error: null,
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

  // Strategy:
  // a) Prefer any "recent within window" by confidence, tie-break by recency
  const recent = results
    .filter((r: EvalResult) => r.ok && r.recentWithinWindow && r.latest)
    .sort(
      (a: EvalResult, b: EvalResult) =>
        b.confidence - a.confidence || (b.latest!.date.getTime() - a.latest!.date.getTime())
    );
  if (recent.length) return { choice: recent[0], results };

  // b) Otherwise, pick highest confidence with any latest
  const byConf = results
    .filter((r: EvalResult) => r.ok && (r.authorUrl || r.latest))
    .sort(
      (a: EvalResult, b: EvalResult) =>
        b.confidence - a.confidence || ((b.latest?.date.getTime() || 0) - (a.latest?.date.getTime() || 0))
    );
  if (byConf.length) return { choice: byConf[0], results };

  // c) Fallback: any OK
  const anyOk = results.find((r) => r.ok);
  if (anyOk) return { choice: anyOk, results };

  return { choice: null, results };
}

/* =========================================================
   GOOGLE CSE (WEB SEARCH)
   ========================================================= */
type SearchPass = "primary" | "broad" | "authorOnly";
type SearchOptions = {
  author: string;
  book?: string;
  lookbackDays: number;
  requireBook: boolean;
  authorOnly: boolean;
  minSearchConfidence: number;
};

function blockedDomain(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return BLOCKED_DOMAINS.some((d) => host.includes(d));
  } catch {
    return true;
  }
}

/** Assign a confidence and acceptance for a single CSE item */
function scoreItem(
  item: any,
  pass: SearchPass,
  opts: SearchOptions
): WebHit {
  const title = String(item.title || "");
  const snippet = String(item.snippet || "");
  const url = String(item.link || "");
  const host = hostOf(url);

  const reasons: string[] = [];
  let conf = 0;

  const tTitle = tokens(title);
  const tSnippet = tokens(snippet);
  const tCombined = Array.from(new Set([...tTitle, ...tSnippet]));
  const tAuthor = tokens(opts.author);
  const tBook = tokens(opts.book || "");

  const authorMatch = tAuthor.length && (jaccard(tAuthor, tCombined) >= 0.25 || containsAll(tCombined, tAuthor));
  if (authorMatch) {
    conf += 0.35;
    reasons.push("author_in_title_or_snippet");
  }

  const bookMatch = !!opts.book && tBook.length && (jaccard(tBook, tCombined) >= 0.22 || containsAll(tCombined, tBook));
  if (bookMatch) {
    conf += 0.35;
    reasons.push("book_in_title_or_snippet");
  }

  if (title.toLowerCase().includes("interview") || title.toLowerCase().includes("in conversation") || snippet.toLowerCase().includes("interview")) {
    conf += 0.1;
    reasons.push("conversation_hint");
  }

  // If it's a homepage (title contains author name strongly) and we have book match in snippet, accept
  const homepageLike =
    !/\/.+/.test(new URL(url).pathname) && authorMatch;
  if (homepageLike && bookMatch) {
    conf += 0.15;
    reasons.push("homepage_book_combo");
  }

  // Recency: Google CSE already constrained by dateRestrict, but some results still older; accept if no obvious older date.
  // If CSE gives a date (rare), enforce. Else treat as within window.
  const publishedAt: string | undefined = item?.pagemap?.metatags?.[0]?.["article:published_time"]
    || item?.pagemap?.metatags?.[0]?.["og:updated_time"]
    || item?.pagemap?.metatags?.[0]?.["article:modified_time"]
    || item?.pagemap?.newsarticle?.[0]?.datepublished
    || undefined;

  const withinWindow: boolean = !!(publishedAt ? (daysAgo(new Date(publishedAt)) <= opts.lookbackDays) : true);

  // Blocked domain hard fail
  if (blockedDomain(url)) {
    reasons.push("blocked_domain");
    return {
      title,
      url,
      snippet,
      publishedAt,
      host,
      confidence: 0,
      reason: reasons,
      accepted: false,
      authorParticipation: false,
    };
  }

  // Require book?
  const requireBookGate = !!(opts.requireBook ? bookMatch : true);

  // Pass-specific boosts/gates
  if (pass === "primary") {
    // expect both author + book
    if (authorMatch && bookMatch) {
      conf += 0.1;
      reasons.push("primary_pass_combo");
    }
  } else if (pass === "authorOnly") {
    // author-focused
    if (authorMatch) {
      conf += 0.05;
      reasons.push("author_only_pass");
    }
  } else {
    // broad
    // no extra
  }

  const accepted: boolean = !!(
    withinWindow &&
    requireBookGate &&
    conf >= opts.minSearchConfidence
  );

  // Minimal author participation heuristic (not foolproof):
  const authorParticipation =
    !!(authorMatch && /interview|conversation|q&a|opinion|column|essay|by\s+/i.test(title + " " + snippet));

  return {
    title,
    url,
    snippet,
    publishedAt,
    host,
    confidence: Math.max(0, Math.min(1, conf)),
    reason: reasons,
    accepted,
    authorParticipation,
  };
}

/** Run Google CSE with a given query */
async function googleCSE(query: string, lookbackDays: number): Promise<any[]> {
  const key = process.env.GOOGLE_CSE_KEY;
  const cx = process.env.GOOGLE_CSE_CX;
  if (!key || !cx) return [];

  const url =
    `https://www.googleapis.com/customsearch/v1?key=${encodeURIComponent(key)}&cx=${encodeURIComponent(cx)}&q=${encodeURIComponent(query)}&dateRestrict=d${lookbackDays}&num=10`;

  try {
    const res = await fetchWithTimeout(url);
    if (!res?.ok) return [];
    const data = await res.json();
    return Array.isArray(data?.items) ? data.items : [];
  } catch {
    return [];
  }
}

/** Build queries for each pass and score the hits */
async function runWebSearch(author: string, book: string | undefined, lookback: number, minSearchConfidence: number) {
  // Blocklist to omit noisy sites
  const minus = " -site:" + BLOCKED_DOMAINS.join(" -site:");

  const base = `"${author}"`;
  const bookQ = book ? ` "${book}"` : "";
  const convQ = ` (interview OR "in conversation" OR "Q&A" OR conversation OR profile OR column OR essay)`;

  // Primary: author + book + conversation hint
  const q1 = `${base}${bookQ}${convQ}${minus}`;
  // Broad: author + book (no conversation terms)
  const q2 = `${base}${bookQ}${minus}`;
  // Author-only fallback: author + conversation hint
  const q3 = `${base}${convQ}${minus}`;

  return { q1, q2, q3, minSearchConfidence };
}

/* =========================================================
   CORS
   ========================================================= */
function cors(res: any, origin?: string) {
  res.setHeader("Access-Control-Allow-Origin", origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Auth");
}

/* =========================================================
   HANDLER
   ========================================================= */
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

    const debug: boolean = !!body.debug;

    const strictAuthorMatch: boolean = !!body.strict_author_match;
    const requireBookMatch: boolean = !!body.require_book_match;
    const fallbackAuthorOnly: boolean = !!body.fallback_author_only;

    const minConfidence: number =
      typeof body.min_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_confidence))
        : DEFAULT_MIN_CONFIDENCE;

    const includeSearch: boolean = !!body.include_search;

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
      requireBookMatch,
      fallbackAuthorOnly,
      minConfidence,
      bookTitle,
      publisher,
      isbn,
      includeSearch,
      minSearchConfidence
    );
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    /* ---------------------------
       1) FEED PATH (RSS)
       --------------------------- */
    const feeds = await findFeeds(author, knownUrls, hints);
    const { choice, results } = await evaluateFeeds(feeds, lookback, ctx);

    const addDebug = (acc: any[], list: EvalResult[]) =>
      acc.concat(
        list.slice(0, 10).map((r) => ({
          feedUrl: r.feedUrl,
          ok: !!r.ok,
          source: r.source,
          latest: r.latest ? r.latest.date.toISOString() : null,
          recentWithinWindow: !!r.recentWithinWindow,
          confidence: Number(r.confidence.toFixed(2)),
          reason: r.reason,
          siteTitle: r.siteTitle || null,
          feedTitle: r.feedTitle || null,
          error: r.error || null,
        }))
      );

    let examined: any[] = [];
    examined = addDebug(examined, results);

    // If strict, gate on confidence
    if (strictAuthorMatch && choice && choice.confidence < minConfidence) {
      // do nothing yet, proceed to web path if enabled
    } else if (choice && choice.recentWithinWindow && choice.latest && (!requireBookMatch || bookTitle)) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: choice.latest.title,
        latest_url: choice.latest.link,
        published_at: choice.latest.date.toISOString(),
        source: choice.source,
        author_url: choice.authorUrl,
      };
      if (debug) payload._debug = examined;
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    } else if (choice && choice.authorUrl && !strictAuthorMatch) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: false,
        source: choice.source,
        author_url: choice.authorUrl,
      };
      if (debug) payload._debug = examined;
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    /* ---------------------------
       2) WEB SEARCH (Google CSE)
       --------------------------- */
    if (includeSearch) {
      const { q1, q2, q3 } = await runWebSearch(author, bookTitle || undefined, lookback, minSearchConfidence);

      // Helper to fetch & score a pass
      const doPass = async (q: string, pass: SearchPass, requireBook: boolean, authorOnly: boolean) => {
        const items = await googleCSE(q, lookback);
        const hits = items.map((it) =>
          scoreItem(it, pass, {
            author,
            book: bookTitle || undefined,
            lookbackDays: lookback,
            requireBook,
            authorOnly,
            minSearchConfidence: minSearchConfidence,
          })
        );

        // Add pass results to debug
        examined.push(
          ...hits.map((h) => ({
            feedUrl: h.url,
            ok: !!h.accepted,
            source: "web",
            latest: h.publishedAt || null,
            recentWithinWindow: !!(h.publishedAt ? daysAgo(new Date(h.publishedAt)) <= lookback : true),
            confidence: Number(h.confidence.toFixed(2)),
            reason: h.reason,
            siteTitle: null,
            feedTitle: null,
            error: null,
          }))
        );

        // Filter to accepted hits
        const accepted = hits.filter((h) => !!h.accepted);

        // Prefer hits with authorParticipation signal
        accepted.sort((a, b) => {
          if (a.authorParticipation !== b.authorParticipation) {
            return a.authorParticipation ? -1 : 1;
          }
          return b.confidence - a.confidence;
        });

        return accepted;
      };

      // Pass 1: primary (author + book + conversation terms) — requires book
      let accepted = await doPass(q1, "primary", /*requireBook*/ !!bookTitle || requireBookMatch, /*authorOnly*/ false);

      // Pass 2: broad (author + book) — still prefer book
      if (!accepted.length) {
        accepted = await doPass(q2, "broad", /*requireBook*/ !!bookTitle || requireBookMatch, /*authorOnly*/ false);
      }

      // Pass 3: author-only fallback (if allowed)
      if (!accepted.length && fallbackAuthorOnly) {
        accepted = await doPass(q3, "authorOnly", /*requireBook*/ false, /*authorOnly*/ true);
      }

      if (accepted.length) {
        const top = accepted[0];
        const payload: CacheValue = {
          author_name: author,
          has_recent: true,
          latest_title: top.title || top.url,
          latest_url: top.url,
          published_at: top.publishedAt,
          source: "web",
          author_url: `https://${top.host}`,
        };
        if (debug) payload._debug = examined;
        setCached(cacheKey, payload);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      }
    }

    // Nothing acceptable
    const empty: CacheValue = { author_name: author, has_recent: false };
    if (debug) empty._debug = examined;
    setCached(cacheKey, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(empty);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
