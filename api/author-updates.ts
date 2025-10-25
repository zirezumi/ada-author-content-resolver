// api/author-updates.ts
/// <reference types="node" />

import Parser from "rss-parser";
import pLimit from "p-limit";

/* =============================
   Vercel runtime
   ============================= */
export const config = { runtime: "nodejs" } as const;

/* =============================
   Auth
   ============================= */
const AUTH_SECRETS: string[] = (process.env.AUTHOR_UPDATES_SECRET || "")
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
  if (!provided || !AUTH_SECRETS.includes(provided)) {
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
const FETCH_TIMEOUT_MS = 5500;
const USER_AGENT = "AuthorUpdates/2.2 (+https://example.com)";

const CONCURRENCY = 4;

const MAX_FEED_CANDIDATES = 15;
const MAX_KNOWN_URLS = 6;

/* ====== Google CSE config ====== */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_ID = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "";
const USE_SEARCH_BASE = !!(CSE_KEY && CSE_ID);

/* ====== Search scoring defaults ====== */
const DEFAULT_MIN_CONFIDENCE = 0.5;
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.5;

/* ====== Domain filters ====== */
const DEFAULT_DOMAIN_BLOCKLIST = [
  // social / video
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "youtube.com",
  "youtu.be",
  // shopping
  "amazon.",
  "ebay.",
  "barnesandnoble.com",
  "bookshop.org",
  "audible.",
  "itunes.apple.com",
  // podcast/audio
  "open.spotify.com",
  "podcasts.apple.com",
  "soundcloud.com",
  "stitcher.com",
  "podbean.com",
  "simplecast.com",
  "megaphone.fm",
  // newsletters often not “hosted content by author” (can be toggled off)
  "substack.com",
];

const PARTICIPATION_PHRASES = [
  "interview with",
  "in conversation",
  "q&a",
  "q & a",
  "talks to",
  "talks with",
  "speaks to",
  "speaks with",
  "byline",
  "by ",
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
  source?: string; // "web" | "rss" | platform guess
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
  _search_diag?: any;
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type PlatformHints = {
  platform?: "substack" | "medium" | "wordpress" | "ghost" | "blogger";
  handle?: string;
  feed_url?: string;
  site_url?: string;
};

type FeedItem = { title: string; link: string; date: Date };

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
  host: string;
  publishedAt?: string;
  confidence: number;
  reason: string[];
};

type Context = {
  authorName: string;
  authorTokens: string[];
  bookTokens: string[];
  lookbackDays: number;
  requireBookMatch: boolean;
  fallbackAuthorOnly: boolean;
  unsafeDisableDomainFilters: boolean;
  unsafeDisableParticipationFilter: boolean;
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(parts: Record<string, any>) {
  return Object.keys(parts)
    .sort()
    .map((k) => `${k}=${JSON.stringify(parts[k])}`)
    .join("&");
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
   Utils
   ============================= */
const parser = new Parser();

function daysAgo(d: Date) {
  return (Date.now() - d.getTime()) / 86_400_000;
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function isHttpUrl(s: string) {
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function normalizeAuthor(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
}

function hostOf(u: string): string {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}

function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
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

function hasContiguousPhrase(text: string, phrase: string) {
  return new RegExp(`\\b${phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(text);
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

function sourceFromUrl(url?: string) {
  if (!url) return "rss";
  const u = url.toLowerCase();
  if (u.includes("substack.com")) return "substack";
  if (u.includes("medium.com")) return "medium";
  if (u.includes("blogspot.") || u.includes("blogger.")) return "blogger";
  if (u.includes("wordpress")) return "wordpress";
  return "rss";
}

/* =============================
   RSS helpers (fallback)
   ============================= */
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
      /rss|atom|application\/(rss|atom)\+xml/i.test(type) || /rss|atom/i.test(href ?? "");
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
  if (!res?.ok) return null;
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
      const d = new Date(it.isoDate || it.pubDate || it.published || it.date || 0);
      return { title: String(it.title ?? ""), link: String(it.link ?? ""), date: d };
    })
    .filter((x) => x.link && !isNaN(x.date.getTime()))
    .sort((a, b) => b.date.getTime() - a.date.getTime());
  return items.length ? items[0] : null;
}

async function fetchSiteTitle(url: string): Promise<string | null> {
  try {
    const origin = new URL(url).origin;
    const res = await fetchWithTimeout(origin);
    if (!res?.ok) return null;
    const html = await res.text();
    const mTitle = html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim();
    const mOG = html.match(/<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)["']/i)?.[1]?.trim();
    return mOG || mTitle || null;
  } catch {
    return null;
  }
}

async function evaluateFeed(feedUrl: string, lookback: number, authorTokens: string[]): Promise<EvalResult> {
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

    let score = 0;
    const reasons: string[] = [];
    const siteTokens = tokens(siteTitle || "");
    const feedTokens = tokens(feedTitle || "");

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
      /* ignore */
    }
  }

  return Array.from(candidates).slice(0, MAX_FEED_CANDIDATES);
}

/* =============================
   Google CSE Search
   ============================= */
function isBlockedHost(host: string, disableFilters: boolean): boolean {
  if (disableFilters) return false;
  const h = host.toLowerCase();
  return DEFAULT_DOMAIN_BLOCKLIST.some((bad) => h.includes(bad));
}

async function googleCSE(
  query: string,
  lookbackDays: number
): Promise<{ items: any[]; raw: any }> {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", "10");
  url.searchParams.set("dateRestrict", `d${Math.max(1, Math.min(365, lookbackDays))}`);

  const resp = await fetchWithTimeout(url.toString());
  if (!resp?.ok) {
    const text = await resp?.text?.();
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  const items: any[] = Array.isArray(data?.items) ? data.items : [];
  return { items, raw: data };
}

async function fetchHtmlAndValidate(url: string, ctx: Context): Promise<{
  authorInHtml: boolean;
  bookInHtml: boolean;
  participationHit: boolean;
  textSample: string;
}> {
  try {
    const res = await fetchWithTimeout(url, {}, FETCH_TIMEOUT_MS);
    if (!res?.ok) return { authorInHtml: false, bookInHtml: false, participationHit: false, textSample: "" };
    const html = await res.text();
    const plain = html.replace(/\s+/g, " ").slice(0, 20000);
    const t = tokens(plain);

    const authorIn = containsAll(t, ctx.authorTokens) || jaccard(t, ctx.authorTokens) >= 0.25;
    const bookIn = ctx.bookTokens.length
      ? containsAll(t, ctx.bookTokens) || jaccard(t, ctx.bookTokens) >= 0.2
      : false;

    const lower = plain.toLowerCase();
    const authorFullLower = ctx.authorTokens.join(" ");
    const participation =
      PARTICIPATION_PHRASES.some((p) => lower.includes(p)) ||
      hasContiguousPhrase(lower, `by ${authorFullLower}`) ||
      hasContiguousPhrase(lower, `${authorFullLower} in conversation`) ||
      hasContiguousPhrase(lower, `${authorFullLower} interviewed`);

    return {
      authorInHtml: authorIn,
      bookInHtml: bookIn,
      participationHit: participation,
      textSample: plain.slice(0, 400),
    };
  } catch {
    return { authorInHtml: false, bookInHtml: false, participationHit: false, textSample: "" };
  }
}

function scoreHitBase(hit: WebHit, ctx: Context): WebHit {
  let score = 0;
  const reasons: string[] = [];

  const titleT = tokens(hit.title || "");
  const snipT = tokens(hit.snippet || "");
  const combined = Array.from(new Set([...titleT, ...snipT]));

  if (containsAll(combined, ctx.authorTokens) || jaccard(combined, ctx.authorTokens) >= 0.35) {
    score += 0.35;
    reasons.push("author_in_title_or_snippet");
  }
  if (ctx.bookTokens.length) {
    if (containsAll(combined, ctx.bookTokens) || jaccard(combined, ctx.bookTokens) >= 0.3) {
      score += 0.35;
      reasons.push("book_in_title_or_snippet");
    }
  }

  if (!ctx.unsafeDisableDomainFilters && isBlockedHost(hit.host, false)) {
    score -= 1.0; // hard block
    reasons.push("blocked_domain");
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reason = reasons;
  return hit;
}

async function enrichWithHtmlSignals(hit: WebHit, ctx: Context): Promise<WebHit> {
  const { authorInHtml, bookInHtml, participationHit } = await fetchHtmlAndValidate(hit.url, ctx);

  if (authorInHtml) {
    hit.confidence = Math.min(1, hit.confidence + 0.25);
    hit.reason.push("content_contains_author");
  }
  if (ctx.bookTokens.length && bookInHtml) {
    hit.confidence = Math.min(1, hit.confidence + 0.25);
    hit.reason.push("content_contains_book");
  }
  if (!ctx.unsafeDisableParticipationFilter && participationHit) {
    hit.confidence = Math.min(1, hit.confidence + 0.20);
    hit.reason.push("accept_participation");
  }

  return hit;
}

async function searchAuthorContent(
  authorName: string,
  bookTitle: string | "",
  ctx: Context,
  debug: boolean
) {
  const authorPhrase = `"${authorName}"`;
  const bookPart = bookTitle ? ` "${bookTitle}"` : "";
  const participationBoost = `(interview OR "in conversation" OR "Q&A" OR conversation)`;

  // Domain removals in the query (still also enforced by code)
  const minusSites = DEFAULT_DOMAIN_BLOCKLIST
    .map((d) => (d.endsWith(".") ? `-site:${d}*` : `-site:${d}`))
    .join(" ");

  const baseQ = `${authorPhrase}${bookPart} ${participationBoost} ${minusSites}`.trim();

  const diag: any = { query: baseQ };

  const { items, raw } = await googleCSE(baseQ, ctx.lookbackDays);
  diag.totalResults = Number(raw?.searchInformation?.totalResults || 0);
  diag.returned = items.length;

  const prelim: WebHit[] = items.map((it: any) => {
    const url = String(it.link || "");
    return scoreHitBase(
      {
        title: String(it.title || ""),
        url,
        snippet: String(it.snippet || it.htmlSnippet || ""),
        host: hostOf(url),
        publishedAt: undefined,
        confidence: 0,
        reason: [],
      },
      ctx
    );
  });

  // Filter out blocked hosts after base score
  const filtered = prelim.filter((h) =>
    ctx.unsafeDisableDomainFilters ? true : !isBlockedHost(h.host, false)
  );

  // Enrich with HTML signals (author/book/participation)
  const limit = pLimit(CONCURRENCY);
  const enriched = await Promise.all(filtered.map((h) => limit(() => enrichWithHtmlSignals(h, ctx))));

  // If require book match, drop those that don't contain the book anywhere
  const finalHits = enriched.filter((h) => {
    if (!ctx.requireBookMatch) return true;
    const hasBook =
      containsAll(tokens(h.title + " " + (h.snippet || "")), ctx.bookTokens) ||
      h.reason.includes("content_contains_book");
    return hasBook;
  });

  finalHits.sort((a, b) => b.confidence - a.confidence);

  return { hits: finalHits, _diag: diag };
}

/* =============================
   CORS
   ============================= */
function cors(res: any, origin?: string) {
  res.setHeader("Access-Control-Allow-Origin", origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Auth");
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

    const author = normalizeAuthor(String(body.author_name || ""));
    if (!author) return res.status(400).json({ error: "author_name required" });

    const bookTitle: string = typeof body.book_title === "string" ? body.book_title : "";
    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    let knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls : [];
    knownUrls = knownUrls.map((u) => String(u).trim()).filter(isHttpUrl).slice(0, MAX_KNOWN_URLS);

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
    const includeSearchRequested: boolean = body.include_search === true;

    const strictAuthorMatch: boolean = body.strict_author_match === true;
    const requireBookMatch: boolean = body.require_book_match === true;
    const fallbackAuthorOnly: boolean = body.fallback_author_only === true;

    const minConfidence: number =
      typeof body.min_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_confidence))
        : DEFAULT_MIN_CONFIDENCE;

    const minSearchConfidence: number =
      typeof body.min_search_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_search_confidence))
        : DEFAULT_MIN_SEARCH_CONFIDENCE;

    const unsafeDisableDomainFilters: boolean = body.unsafe_disable_domain_filters === true;
    const unsafeDisableParticipationFilter: boolean =
      body.unsafe_disable_participation_filter === true;

    // set header diagnostics about search env
    const USE_SEARCH = includeSearchRequested && USE_SEARCH_BASE;
    res.setHeader("x-use-search", String(USE_SEARCH));
    res.setHeader("x-cse-id-present", String(!!CSE_ID));
    res.setHeader("x-cse-key-present", String(!!CSE_KEY));

    const authorTokens = tokens(author);
    const bookTokens = tokens(bookTitle);

    const ctx: Context = {
      authorName: author,
      authorTokens,
      bookTokens,
      lookbackDays: lookback,
      requireBookMatch,
      fallbackAuthorOnly,
      unsafeDisableDomainFilters,
      unsafeDisableParticipationFilter,
    };

    // cache key
    const cacheKey = makeCacheKey({
      author,
      bookTitle,
      lookback,
      knownUrls,
      hints,
      strictAuthorMatch,
      requireBookMatch,
      fallbackAuthorOnly,
      minConfidence,
      minSearchConfidence,
      USE_SEARCH,
      unsafeDisableDomainFilters,
      unsafeDisableParticipationFilter,
    });

    const hit = getCached(cacheKey);
    if (hit && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(hit);
    }

    /* ---------- Primary path: Google CSE ---------- */
    let bestSearch: WebHit | null = null;
    let searchDiag: any = undefined;

    if (USE_SEARCH) {
      try {
        const { hits, _diag } = await searchAuthorContent(author, bookTitle, ctx, debug);
        searchDiag = _diag;

        // Filter by thresholds with fallbacks
        const accepted = hits.find((h) => h.confidence >= minSearchConfidence);
        if (accepted) {
          bestSearch = accepted;
        } else if (!requireBookMatch && fallbackAuthorOnly) {
          const fallback = hits.find(
            (h) =>
              h.confidence >= Math.min(minSearchConfidence, 0.4) &&
              h.reason.includes("author_in_title_or_snippet")
          );
          if (fallback) bestSearch = fallback;
        }
      } catch (e: any) {
        searchDiag = { error: String(e?.message || e) };
      }
    }

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
        payload._search_diag = {
          use_search: USE_SEARCH,
          cse_id_present: !!CSE_ID,
          cse_key_present: !!CSE_KEY,
          ...searchDiag,
          picked: {
            url: bestSearch.url,
            confidence: Number(bestSearch.confidence.toFixed(2)),
            reason: bestSearch.reason,
          },
        };
      }
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    /* ---------- Fallback: RSS discovery (best-effort) ---------- */
    const feeds = await findFeeds(author, knownUrls, hints);
    const limit = pLimit(CONCURRENCY);
    const results = await Promise.all(
      feeds.map((f) => limit(() => evaluateFeed(f, lookback, authorTokens)))
    );

    const recent = results
      .filter((r) => r.ok && r.recentWithinWindow && r.latest)
      .sort(
        (a, b) =>
          (b.latest!.date.getTime() - a.latest!.date.getTime()) ||
          (b.confidence - a.confidence)
      );

    const buildDebug = (list: EvalResult[]) =>
      list.slice(0, 10).map((r) => ({
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

    if (recent.length) {
      const top = recent[0];
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: top.latest!.title,
        latest_url: top.latest!.link,
        published_at: top.latest!.date.toISOString(),
        source: top.source,
        author_url: top.authorUrl,
      };
      if (debug) {
        payload._debug = buildDebug(results);
        if (searchDiag) payload._search_diag = { use_search: USE_SEARCH, ...searchDiag };
      }
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // If strict author match requested and nothing qualified, return explicit error
    if (strictAuthorMatch) {
      const fail: CacheValue = { author_name: author, has_recent: false };
      if (debug) {
        (fail as any)._debug = buildDebug(results);
        if (searchDiag) (fail as any)._search_diag = { use_search: USE_SEARCH, ...searchDiag };
      }
      res.setHeader("x-cache", "MISS");
      return res.status(200).json({ error: "no_confident_author_match", ...(debug ? fail : {}) });
    }

    // Soft fallback: not recent, but we can still hand back an author_url if any
    const anyOk = results.find((r) => r.ok && r.authorUrl);
    if (anyOk) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: false,
        source: anyOk.source,
        author_url: anyOk.authorUrl,
      };
      if (debug) {
        payload._debug = buildDebug(results);
        if (searchDiag) payload._search_diag = { use_search: USE_SEARCH, ...searchDiag };
      }
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Nothing
    const empty: CacheValue = { author_name: author, has_recent: false };
    if (debug) {
      (empty as any)._debug = buildDebug(results);
      if (searchDiag) (empty as any)._search_diag = { use_search: USE_SEARCH, ...searchDiag };
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
