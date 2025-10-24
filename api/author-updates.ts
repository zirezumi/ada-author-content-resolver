// api/author-updates.ts
import Parser from "rss-parser";
// import type { VercelRequest, VercelResponse } from '@vercel/node'  // optional types if you add @vercel/node

// Ensure Node runtime on Vercel (NOT edge)
export const config = { runtime: "nodejs" };

// -----------------------------
// Tunables
// -----------------------------
const LOOKBACK_DEFAULT = 30;        // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 90;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 4500;      // per network call
const USER_AGENT = "AuthorUpdates/1.1 (+https://example.com)";

// -----------------------------
// In-memory Cache (per instance)
// -----------------------------
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: string;
  author_url?: string;
};
type CacheEntry = { expiresAt: number; value: CacheValue };
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(author: string, urls: string[], lookback: number, hints?: unknown) {
  const urlKey = urls
    .map(u => u.trim().toLowerCase())
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
  if (Date.now() > hit.expiresAt) { CACHE.delete(key); return null; }
  return hit.value;
}
function setCached(key: string, value: CacheValue) {
  CACHE.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
}

// -----------------------------
// Utilities
// -----------------------------
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
type FeedItem = { title: string; link: string; date: Date };
function pickFreshest(feed: unknown, lookback: number) {
  const rawItems: any[] = (feed as any)?.items ?? [];
  const items: FeedItem[] = rawItems
    .map((it: any) => {
      const d = new Date(it.isoDate || it.pubDate || it.published || it.date || 0);
      return { title: String(it.title ?? ""), link: String(it.link ?? ""), date: d };
    })
    .filter((x: FeedItem) => x.link && !isNaN(x.date.getTime()))
    .sort((a: FeedItem, b: FeedItem) => b.date.getTime() - a.date.getTime());
  if (!items.length) return null;
  const latest = items[0];
  return daysAgo(latest.date) <= lookback ? latest : null;
}
function normalizeAuthor(s: string) { return s.normalize("NFKC").replace(/\s+/g, " ").trim(); }
function isHttpUrl(s: string) { try { const u = new URL(s); return u.protocol === "http:" || u.protocol === "https:"; } catch { return false; } }

// -----------------------------
// Feed candidate generation
// -----------------------------
type PlatformHints = {
  platform?: "substack" | "medium" | "wordpress" | "ghost" | "blogger";
  handle?: string;
  feed_url?: string;
  site_url?: string;
};
async function findFeeds(author: string, knownUrls: string[] = [], hints?: PlatformHints): Promise<string[]> {
  const candidates = new Set<string>();
  if (hints?.feed_url && isHttpUrl(hints.feed_url)) candidates.add(hints.feed_url);
  if (hints?.site_url && isHttpUrl(hints.site_url)) for (const f of guessFeedsFromSite(hints.site_url)) candidates.add(f);
  if (hints?.platform && hints?.handle) {
    const h = hints.handle.replace(/^@/, "");
    if (hints.platform === "substack") candidates.add(`https://${h}.substack.com/feed`);
    if (hints.platform === "medium") { candidates.add(`https://medium.com/feed/@${h}`); candidates.add(`https://medium.com/feed/${h}`); }
  }
  const handles = [author.toLowerCase().replace(/\s+/g, ""), author.toLowerCase().replace(/\s+/g, "-")];
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
    } catch { /* ignore site fetch errors */ }
  }
  return Array.from(candidates).slice(0, 15);
}

// -----------------------------
// Node-style handler for Vercel
// -----------------------------
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
    const got = (req.headers["x-auth"] || req.headers["X-Auth"] || "") as string;
    if (expect && got !== expect) {
      return res.status(401).json({ error: "unauthorized" });
    }

    // Parse body (Vercel usually parses JSON; fall back if string)
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const rawAuthor = (body.author_name ?? "").toString();
    const author = normalizeAuthor(rawAuthor);
    const lookback = clamp(Number(body.lookback_days ?? LOOKBACK_DEFAULT), LOOKBACK_MIN, LOOKBACK_MAX);
    if (!author) return res.status(400).json({ error: "author_name required" });

    let knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls : [];
    knownUrls = knownUrls.map((u: unknown) => String(u).trim()).filter(isHttpUrl).slice(0, 5);

    const hints: PlatformHints | undefined = body.hints && typeof body.hints === "object"
      ? {
          platform: (body.hints.platform as PlatformHints["platform"]),
          handle: typeof body.hints.handle === "string" ? body.hints.handle : undefined,
          feed_url: typeof body.hints.feed_url === "string" ? body.hints.feed_url : undefined,
          site_url: typeof body.hints.site_url === "string" ? body.hints.site_url : undefined
        }
      : undefined;

    // Cache
    const key = makeCacheKey(author, knownUrls, lookback, hints);
    const cached = getCached(key);
    if (cached) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // Resolve feeds
    const feeds = await findFeeds(author, knownUrls, hints);

    // Try feeds
    for (const feedUrl of feeds) {
      try {
        const feed = await parseFeedURL(feedUrl);
        if (!feed) continue;

        const authorUrl = (feed as any)?.link?.startsWith?.("http")
          ? (feed as any).link
          : new URL(feedUrl).origin;

        const latest = pickFreshest(feed, lookback);
        let payload: CacheValue;

        if (latest) {
          payload = {
            author_name: author,
            has_recent: true,
            latest_title: latest.title,
            latest_url: latest.link,
            published_at: latest.date.toISOString(),
            source: sourceFromUrl(feedUrl),
            author_url: authorUrl
          };
        } else {
          payload = {
            author_name: author,
            has_recent: false,
            source: sourceFromUrl(feedUrl),
            author_url: authorUrl
          };
        }

        setCached(key, payload);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      } catch (e) {
        // Try next candidate
        console.error("feed_error", feedUrl, e);
      }
    }

    // Nothing found
    const empty: CacheValue = { author_name: author, has_recent: false };
    setCached(key, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(empty);
  } catch (err: any) {
    console.error("handler_error", err?.stack || err);
    return res.status(500).json({ error: "internal_error" });
  }
}
