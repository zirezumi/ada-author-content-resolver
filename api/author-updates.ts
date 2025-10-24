// api/author-updates.ts
import Parser from "rss-parser";

export const config = { runtime: "nodejs" }; // ensure Node runtime on Vercel

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

function makeCacheKey(author: string, urls: string[], lookback: number, hints?: any) {
  const urlKey = urls
    .map(u => u.trim().toLowerCase())
    .filter(Boolean)
    .sort()
    .join("|");
  const hintKey = hints
    ? JSON.stringify(Object.keys(hints)
        .sort()
        .reduce((o: any, k) => (o[k] = hints[k], o), {}))
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

// -----------------------------
// Utilities
// -----------------------------
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
    `${clean}/feed`,
    `${clean}/rss.xml`,
    `${clean}/atom.xml`,
    `${clean}/index.xml`,
    `${clean}/?feed=rss2`
  ];
}

// very light RSS <link> discovery
function extractRssLinks(html: string, base: string): string[] {
  const links = Array.from(html.matchAll(/<link[^>]+rel=["']alternate["'][^>]*>/gi))
    .map(m => m[0]);
  const hrefs = links.flatMap(tag => {
    const href = /href=["']([^"']+)["']/i.exec(tag)?.[1];
    const type = /type=["']([^"']+)["']/i.exec(tag)?.[1] ?? "";
    const looksRss =
      /rss|atom|application\/(rss|atom)\+xml/i.test(type) ||
      /rss|atom/i.test(href ?? "");
    if (!href || !looksRss) return [];
    try { return [new URL(href, base).toString()]; } catch { return []; }
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
      headers: { "User-Agent": USER_AGENT, ...(init?.headers || {}) }
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function parseFeedURL(feedUrl: string) {
  // We fetch the XML ourselves (to control timeout/headers) then parse string
  const res = await fetchWithTimeout(feedUrl);
  if (!res || !res.ok) return null;
  const xml = await res.text();
  try {
    return await parser.parseString(xml);
  } catch {
    return null;
  }
}

function pickFreshest(feed: any, lookback: number) {
  const items = (feed?.items ?? [])
    .map((it: any) => {
      const d = new Date(it.isoDate || it.pubDate || it.published || it.date || 0);
      return { title: it.title, link: it.link, date: d };
    })
    .filter(x => x.link && !isNaN(x.date.getTime()))
    .sort((a, b) => b.date.getTime() - a.date.getTime());

  if (!items.length) return null;
  const latest = items[0];
  return daysAgo(latest.date) <= lookback ? latest : null;
}

function normalizeAuthor(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
}

function isHttpUrl(s: string) {
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch { return false; }
}

// -----------------------------
// Feed candidate generation
// -----------------------------
type PlatformHints = {
  platform?: "substack" | "medium" | "wordpress" | "ghost" | "blogger";
  handle?: string;           // e.g., substack/medium handle
  feed_url?: string;         // exact feed, if known
  site_url?: string;         // base site, if known
};

async function findFeeds(
  author: string,
  knownUrls: string[] = [],
  hints?: PlatformHints
): Promise<string[]> {
  const candidates = new Set<string>();

  // 0) Trust explicit hint URLs first
  if (hints?.feed_url && isHttpUrl(hints.feed_url)) candidates.add(hints.feed_url);
  if (hints?.site_url && isHttpUrl(hints.site_url)) {
    for (const f of guessFeedsFromSite(hints.site_url)) candidates.add(f);
  }

  // 1) Known platform patterns via hints
  if (hints?.platform && hints?.handle) {
    const h = hints.handle.replace(/^@/, "");
    if (hints.platform === "substack") candidates.add(`https://${h}.substack.com/feed`);
    if (hints.platform === "medium") {
      candidates.add(`https://medium.com/feed/@${h}`);
      candidates.add(`https://medium.com/feed/${h}`);
    }
  }

  // 2) Heuristics from author name → likely handles (best-effort)
  const handles = [
    author.toLowerCase().replace(/\s+/g, ""),
    author.toLowerCase().replace(/\s+/g, "-")
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

  // Limit to a reasonable number to keep latency bounded
  return Array.from(candidates).slice(0, 15);
}

// -----------------------------
// Handler
// -----------------------------
async function toJsonSafe(req: Request) {
  try { return await req.json(); } catch { return {}; }
}

function corsHeaders(origin?: string) {
  return {
    "Access-Control-Allow-Origin": origin || "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

export default async function handler(req: Request): Promise<Response> {
  const origin = (req.headers.get("origin") || undefined);

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders(origin) });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "POST required" }), {
      status: 405,
      headers: { "content-type": "application/json", ...corsHeaders(origin) }
    });
  }

  // Parse & validate input
  const body = await toJsonSafe(req);
  const rawAuthor = (body.author_name ?? "").toString();
  const author = normalizeAuthor(rawAuthor);
  const lookback = clamp(Number(body.lookback_days ?? LOOKBACK_DEFAULT), LOOKBACK_MIN, LOOKBACK_MAX);

  if (!author) {
    return new Response(JSON.stringify({ error: "author_name required" }), {
      status: 400,
      headers: { "content-type": "application/json", ...corsHeaders(origin) }
    });
  }

  let knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls : [];
  knownUrls = knownUrls
    .map((u: any) => String(u).trim())
    .filter(isHttpUrl)
    .slice(0, 5); // cap to avoid abuse

  const hints: PlatformHints | undefined = body.hints && typeof body.hints === "object"
    ? {
        platform: body.hints.platform,
        handle: body.hints.handle,
        feed_url: body.hints.feed_url,
        site_url: body.hints.site_url
      }
    : undefined;

  // Cache check
  const key = makeCacheKey(author, knownUrls, lookback, hints);
  const cached = getCached(key);
  if (cached) {
    return new Response(JSON.stringify(cached), {
      status: 200,
      headers: {
        "content-type": "application/json",
        "x-cache": "HIT",
        ...corsHeaders(origin)
      }
    });
  }

  // Find candidate feeds
  const feeds = await findFeeds(author, knownUrls, hints);

  // Try feeds in order
  for (const feedUrl of feeds) {
    try {
      const feed = await parseFeedURL(feedUrl);
      if (!feed) continue;

      const authorUrl = feed?.link?.startsWith("http") ? feed.link : new URL(feedUrl).origin;
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
      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "x-cache": "MISS",
          ...corsHeaders(origin)
        }
      });
    } catch {
      // try next candidate
    }
  }

  // Nothing found
  const empty: CacheValue = { author_name: author, has_recent: false };
  setCached(key, empty);
  return new Response(JSON.stringify(empty), {
    status: 200,
    headers: {
      "content-type": "application/json",
      "x-cache": "MISS",
      ...corsHeaders(origin)
    }
  });
}
