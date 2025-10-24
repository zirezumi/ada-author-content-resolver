// api/author-updates.ts
/// <reference types="node" />

import Parser from "rss-parser";
import pLimit from "p-limit";

// Ensure Node runtime on Vercel (NOT edge)
export const config = { runtime: "nodejs" } as const;

/* =============================
   Tunables
   ============================= */
const LOOKBACK_DEFAULT = 30;                  // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 90;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;     // 24h
const FETCH_TIMEOUT_MS = 4500;                // per network call
const USER_AGENT = "AuthorUpdates/1.2 (+https://example.com)";
const CONCURRENCY = 4;                        // feed checks in parallel (bounded)
const MAX_FEED_CANDIDATES = 15;               // latency guard
const MAX_KNOWN_URLS = 5;                     // abuse guard

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
  // debug field only when requested
  _debug?: Array<{
    feedUrl: string;
    ok: boolean;
    source: string;
    latest: string | null;
    recentWithinWindow: boolean;
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

type EvalResult = {
  feedUrl: string;
  authorUrl?: string;
  latest?: FeedItem;
  recentWithinWindow: boolean;
  source: string;
  ok: boolean;
  error?: string;
};

/* =============================
   In-memory cache (per instance)
   ============================= */
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(author: string, urls: string[], lookback: number, hints?: unknown) {
  const urlKey = urls
    .map((u) => u.trim().toLowerCase())
    .filter(Boolean)
    .sort()
    .join("|");

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

  return `${author.trim().toLowerCase()}::${lookback}::${urlKey}::${hintKey}`;
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

function guessFeedsFromSite(site: string): string[] {
  const clean = site.replace(/\/+$/, "");
  return [
    `${clean}/feed`,      // WordPress/Ghost/Hugo often expose this
    `${clean}/rss.xml`,
    `${clean}/atom.xml`,
    `${clean}/index.xml`,
    `${clean}/?feed=rss2`, // WordPress
  ];
}

// very light RSS <link> discovery
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

// Return newest item if any in feed (ignores lookback here)
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

/* =============================
   Feed candidate generation
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
      // ignore site fetch errors
    }
  }

  return Array.from(candidates).slice(0, MAX_FEED_CANDIDATES);
}

/* =============================
   Evaluation across feeds
   ============================= */
async function evaluateFeed(feedUrl: string, lookback: number): Promise<EvalResult> {
  try {
    const feed = await parseFeedURL(feedUrl);
    if (!feed) {
      return {
        feedUrl,
        ok: false,
        recentWithinWindow: false,
        source: sourceFromUrl(feedUrl),
        error: "parse_failed",
      };
    }

    const authorUrl =
      (feed as any)?.link?.startsWith?.("http") ? (feed as any).link : new URL(feedUrl).origin;

    const latest = newestItem(feed) || undefined;
    const recentWithinWindow = !!(latest && daysAgo(latest.date) <= lookback);

    return {
      feedUrl,
      authorUrl,
      latest,
      recentWithinWindow,
      source: sourceFromUrl(feedUrl),
      ok: true,
    };
  } catch (e: unknown) {
    const msg = (e as Error)?.message || "error";
    return {
      feedUrl,
      ok: false,
      recentWithinWindow: false,
      source: sourceFromUrl(feedUrl),
      error: msg,
    };
  }
}

async function evaluateFeeds(feeds: string[], lookback: number) {
  const limit = pLimit(CONCURRENCY);
  const results = await Promise.all(feeds.map((f) => limit(() => evaluateFeed(f, lookback))));

  // Prefer any feed with a recent item; pick newest among those
  const recent = results
    .filter((r) => r.ok && r.recentWithinWindow && r.latest)
    .sort((a, b) => (b.latest!.date.getTime() - a.latest!.date.getTime()));
  if (recent.length) return { choice: recent[0], results };

  // Otherwise pick freshest overall (better fallback author_url)
  const allWithLatest = results
    .filter((r) => r.ok && r.latest)
    .sort((a, b) => (b.latest!.date.getTime() - a.latest!.date.getTime()));
  if (allWithLatest.length) return { choice: allWithLatest[0], results };

  // Otherwise any OK with an authorUrl
  const anyOk = results.find((r) => r.ok && r.authorUrl);
  if (anyOk) return { choice: anyOk, results };

  return { choice: null, results };
}

/* =============================
   Node-style handler for Vercel
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

    if (req.method !== "POST") {
      return res.status(405).json({ error: "POST required" });
    }

    // Optional shared-secret auth
    const expect = process.env.AUTHOR_UPDATES_SECRET;
    const gotHeader = (req.headers["x-auth"] ||
      (req.headers["X-Auth"] as string) ||
      "") as string;
    if (expect && gotHeader !== expect) {
      return res.status(401).json({ error: "unauthorized" });
    }

    // Parse body (Vercel often already parsed JSON)
    const bodyRaw = req.body ?? {};
    const body = typeof bodyRaw === "string" ? JSON.parse(bodyRaw || "{}") : bodyRaw;

    const rawAuthor = (body.author_name ?? "").toString();
    const author = normalizeAuthor(rawAuthor);

    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    if (!author) {
      return res.status(400).json({ error: "author_name required" });
    }

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

    // Cache
    const key = makeCacheKey(author, knownUrls, lookback, hints);
    const cached = getCached(key);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // Find candidate feeds & evaluate
    const feeds = await findFeeds(author, knownUrls, hints);
    const { choice, results } = await evaluateFeeds(feeds, lookback);

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
        payload._debug = results.slice(0, 5).map((r) => ({
          feedUrl: r.feedUrl,
          ok: r.ok,
          source: r.source,
          latest: r.latest ? r.latest.date.toISOString() : null,
          recentWithinWindow: r.recentWithinWindow,
          error: r.error || null,
        }));
      }
      setCached(key, payload);
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
        payload._debug = results.slice(0, 5).map((r) => ({
          feedUrl: r.feedUrl,
          ok: r.ok,
          source: r.source,
          latest: r.latest ? r.latest.date.toISOString() : null,
          recentWithinWindow: r.recentWithinWindow,
          error: r.error || null,
        }));
      }
      setCached(key, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Nothing usable at all
    const empty: CacheValue = { author_name: author, has_recent: false };
    if (debug) {
      empty._debug = results.slice(0, 5).map((r) => ({
        feedUrl: r.feedUrl,
        ok: r.ok,
        source: r.source,
        latest: r.latest ? r.latest.date.toISOString() : null,
        recentWithinWindow: r.recentWithinWindow,
        error: r.error || null,
      }));
    }
    setCached(key, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(empty);
  } catch (err: unknown) {
    // Ensure JSON error response on unexpected failures
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
