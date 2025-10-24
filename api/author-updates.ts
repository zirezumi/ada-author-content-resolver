// api/author-updates.ts
/// <reference types="node" />

/**
 * Author Updates Resolver (Google CSE only)
 * - Auth: X-Auth header matched against AUTHOR_UPDATES_SECRET (comma-separated allowed)
 * - Query Google CSE for "author + book" within lookback window
 * - Filter social/shopping/podcast hosts (blocked list)
 * - Prefer official author site and reputable news hosts
 * - For non-author, non-news: require byline/author meta or "By {author}" in body
 * - Options:
 *    include_search, strict_author_match, require_book_match, fallback_author_only,
 *    min_confidence, min_search_confidence, lookback_days, debug
 */

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
const FETCH_TIMEOUT_MS = 5000;
const USER_AGENT = "AuthorUpdates/2.3 (+https://example.com)";
const CONCURRENCY = 4;

const SEARCH_MAX_RESULTS = 10; // keep small for latency
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.6; // 0..1
const DEFAULT_MIN_CONFIDENCE = 0.5;

const BLOCKED_DOMAINS = new Set<string>([
  // social
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "youtube.com",
  "youtu.be",
  // shopping
  "amazon.com",
  "amazon.co.uk",
  "amazon.de",
  "amazon.fr",
  "amazon.ca",
  "amazon.com.au",
  // podcasts / audio directories
  "open.spotify.com",
  "spotify.com",
  "apple.com",
  "podcasts.apple.com",
  // newsletters you asked to block
  "substack.com",
]);

// Recognizably “news-ish” hosts (add more as needed)
const NEWSISH_HOSTS = new Set<string>([
  "theguardian.com",
  "guardian.co.uk",
  "nytimes.com",
  "ft.com",
  "washingtonpost.com",
  "wsj.com",
  "bbc.com",
  "bloomberg.com",
  "reuters.com",
  "apnews.com",
  "npr.org",
  "theatlantic.com",
  "newyorker.com",
  "vox.com",
  "forbes.com",
  "economist.com",
]);

/* =============================
   Types
   ============================= */
type CacheValue = {
  author_name: string;
  has_recent: boolean;
  latest_title?: string;
  latest_url?: string;
  published_at?: string;
  source?: "web";
  author_url?: string;
  _debug?: Array<{
    feedUrl: string;
    ok: boolean;
    source: "web";
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
  publishedAt?: string; // ISO if available (CSE often lacks it)
  host: string;
  confidence: number; // 0..1
  reason: string[];
};

type PageSnapshot = {
  url: string;
  html: string;
  text: string;
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();
function makeCacheKey(obj: Record<string, unknown>): string {
  const sorted = Object.keys(obj)
    .sort()
    .reduce((o: any, k) => ((o[k] = obj[k]), o), {});
  return JSON.stringify(sorted);
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
function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}
function daysAgo(d: Date) {
  return (Date.now() - d.getTime()) / 86_400_000;
}
function normalizeAuthor(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
}
function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s.-]/gu, " ")
    .split(/[\s.-]+/)
    .filter(Boolean);
}
function containsAll(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.every((n) => H.has(n));
}
function jaccard(a: string[], b: string[]) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}
function hostOf(u: string) {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}
function isBlockedHost(host: string): boolean {
  const h = host.toLowerCase();
  const bare = h.replace(/^(www\.|m\.)/, ""); // strip common prefixes

  // exact or subdomain match
  for (const d of BLOCKED_DOMAINS) {
    if (bare === d) return true;
    if (bare.endsWith("." + d)) return true;
  }
  return false;
}
function looksLikeNewsHost(host: string): boolean {
  const bare = host.toLowerCase().replace(/^(www\.|m\.)/, "");
  if (NEWSISH_HOSTS.has(bare)) return true;
  // simple heuristic for large media networks
  if (/\bnews|press|guardian|times|post|journal|bloomberg|reuters|apnews\b/.test(bare)) return true;
  return false;
}
function looksLikeOfficialAuthorSite(title: string, host: string, authorName: string): boolean {
  const aTok = tokens(authorName);
  const tTok = tokens(title || "");
  const hTok = tokens(host.split(".")[0]); // subdomain
  return jaccard(aTok, tTok) >= 0.5 || jaccard(aTok, hTok) >= 0.5;
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

function stripHtml(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
function extractMetaContent(html: string, nameOrProp: RegExp): string | null {
  // name="author", property="article:author" etc.
  const metaRe = new RegExp(
    `<meta[^>]+(?:name|property)=["']([^"']+)["'][^>]*content=["']([^"']+)["'][^>]*>`,
    "gi"
  );
  let m: RegExpExecArray | null;
  while ((m = metaRe.exec(html))) {
    const key = m[1];
    const val = m[2];
    if (nameOrProp.test(key)) return val;
  }
  return null;
}
function extractPublishedISO(html: string): string | undefined {
  // try common meta/date patterns
  const candidates: Array<[RegExp, (m: RegExpExecArray) => string | undefined]> = [
    [
      /<meta[^>]+property=["']article:published_time["'][^>]+content=["']([^"']+)["']/i,
      (m) => m[1],
    ],
    [
      /<meta[^>]+name=["']parsely-pub-date["'][^>]+content=["']([^"']+)["']/i,
      (m) => m[1],
    ],
    [
      /<time[^>]+datetime=["']([^"']+)["']/i,
      (m) => m[1],
    ],
    [
      /<meta[^>]+itemprop=["']datePublished["'][^>]+content=["']([^"']+)["']/i,
      (m) => m[1],
    ],
  ];
  for (const [re, pick] of candidates) {
    const m = re.exec(html);
    if (m) {
      const iso = pick(m);
      if (iso) {
        const d = new Date(iso);
        if (!isNaN(d.getTime())) return d.toISOString();
      }
    }
  }
  return undefined;
}

function pageHasBylineForAuthor(html: string, text: string, authorName: string): boolean {
  const a = authorName.toLowerCase();

  const metaAuthor =
    extractMetaContent(html, /author/i) ||
    extractMetaContent(html, /article:author/i) ||
    extractMetaContent(html, /og:author/i) ||
    extractMetaContent(html, /parsely-author/i) ||
    null;
  if (metaAuthor && metaAuthor.toLowerCase().includes(a)) return true;

  const ld = html.match(
    /<script[^>]*application\/ld\+json[^>]*>([\s\S]*?)<\/script>/i
  );
  if (ld?.[1]) {
    try {
      const obj = JSON.parse(ld[1]);
      const findAuthor = (o: any): string[] => {
        if (!o || typeof o !== "object") return [];
        const out: string[] = [];
        const pushIf = (v: any) => {
          if (typeof v === "string") out.push(v);
          else if (v && typeof v.name === "string") out.push(v.name);
        };
        if (o.author) {
          if (Array.isArray(o.author)) o.author.forEach(pushIf);
          else pushIf(o.author);
        }
        if (Array.isArray(o["@graph"])) o["@graph"].forEach((x: any) => out.push(...findAuthor(x)));
        return out;
      };
      const authors = findAuthor(obj).map((s) => s.toLowerCase());
      if (authors.some((n) => n.includes(a))) return true;
    } catch {
      // ignore
    }
  }

  const bylineRe = new RegExp(`\\bby\\s+${authorName.replace(/\s+/g, "\\s+")}\\b`, "i");
  if (bylineRe.test(text)) return true;

  return false;
}

/* =============================
   Google CSE
   ============================= */
function buildQuery(author: string, book: string, lookbackDays: number) {
  // Encourage interview/feature pages; filter out obvious social/shopping/podcast hosts
  const interviewHint = '(interview OR "in conversation" OR "Q&A" OR conversation)';
  const blocked = [
    "x.com",
    "twitter.com",
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "linkedin.com",
    "youtube.com",
    "amazon.com",
    "amazon.co.uk",
    "amazon.de",
    "amazon.fr",
    "amazon.ca",
    "amazon.com.au",
    "open.spotify.com",
    "spotify.com",
    "apple.com",
    "podcasts.apple.com",
    "substack.com",
  ]
    .map((d) => `-site:${d}`)
    .join(" ");

  // Allow looser author/book co-occurrence (not necessarily adjacent)
  const q = `"${author}" ${book ? `"${book}"` : ""} ${interviewHint} ${blocked}`.trim();
  const dateRestrict = `d${lookbackDays}`;

  return { q, dateRestrict };
}

async function googleSearchHits(
  authorName: string,
  bookTitle: string,
  lookbackDays: number
): Promise<WebHit[]> {
  const key = process.env.GOOGLE_CSE_KEY;
  const cx = process.env.GOOGLE_CSE_CX;
  if (!key || !cx) return [];

  const { q, dateRestrict } = buildQuery(authorName, bookTitle, lookbackDays);

  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", key);
  url.searchParams.set("cx", cx);
  url.searchParams.set("q", q);
  url.searchParams.set("num", String(SEARCH_MAX_RESULTS));
  url.searchParams.set("dateRestrict", dateRestrict);
  url.searchParams.set("safe", "off");

  const resp = await fetchWithTimeout(url.toString(), undefined, FETCH_TIMEOUT_MS);
  if (!resp?.ok) return [];

  const data: any = await resp.json();
  const items: any[] = Array.isArray(data?.items) ? data.items : [];
  const hits: WebHit[] = [];

  for (const it of items) {
    const title = String(it.title || "");
    const link = String(it.link || "");
    const snippet = String(it.snippet || "");
    const host = hostOf(link);

    // Skip blocked hosts early (keeps them out of debug too)
    if (!host || isBlockedHost(host)) continue;

    // simple confidence from co-occurrence in title/snippet
    const tAuthor = tokens(authorName);
    const tBook = tokens(bookTitle || "");
    const tCombined = tokens(`${title} ${snippet}`);

    let score = 0;
    const reasons: string[] = [];

    const authorMatch =
      tAuthor.length && (jaccard(tAuthor, tCombined) >= 0.3 || containsAll(tCombined, tAuthor));
    if (authorMatch) {
      score += 0.4;
      reasons.push("author_in_title_or_snippet");
    }

    const bookMatch =
      tBook.length && (jaccard(tBook, tCombined) >= 0.25 || containsAll(tCombined, tBook));
    if (bookMatch) {
      score += 0.35;
      reasons.push("book_in_title_or_snippet");
    }

    // small boost for newsish
    if (looksLikeNewsHost(host)) {
      score += 0.15;
      reasons.push("newsish_host");
    }

    hits.push({
      title,
      url: link,
      snippet,
      publishedAt: undefined, // CSE often lacks per-result dates reliably; we’ll parse page below
      host,
      confidence: Math.max(0, Math.min(1, score)),
      reason: reasons,
    });
  }

  // best first
  hits.sort((a, b) => b.confidence - a.confidence);
  return hits.slice(0, SEARCH_MAX_RESULTS);
}

/* =============================
   Page verification / acceptance
   ============================= */
function acceptArticleByBody(text: string, author: string, bookTitle: string, requireBook: boolean) {
  const t = tokens(text);
  const aTok = tokens(author);
  const bTok = tokens(bookTitle || "");

  const authorOk = aTok.length ? containsAll(t, aTok) || jaccard(t, aTok) >= 0.25 : false;
  const bookOk = bTok.length ? containsAll(t, bTok) || jaccard(t, bTok) >= 0.18 : false;

  if (requireBook) {
    if (authorOk && bookOk) return { pass: true, reason: ["content_contains_author_and_book"] };
    return { pass: false, reason: ["content_missing_required_terms"] };
  }

  if (authorOk && bookOk) return { pass: true, reason: ["content_contains_author_and_book"] };
  if (authorOk) return { pass: true, reason: ["content_contains_author"] };
  return { pass: false, reason: ["content_too_weak"] };
}

async function fetchPage(url: string): Promise<PageSnapshot | null> {
  const r = await fetchWithTimeout(url, undefined, FETCH_TIMEOUT_MS);
  if (!r?.ok) return null;
  const html = await r.text();
  const text = stripHtml(html);
  return { url, html, text };
}

/* =============================
   Main selection logic
   ============================= */
async function verifyTopCandidates(
  hits: WebHit[],
  author: string,
  bookTitle: string,
  lookbackDays: number,
  requireBookMatch: boolean,
  minSearchConfidence: number,
  debugArr: any[]
): Promise<CacheValue | null> {
  // take top N to verify deeply
  const top = hits.filter((h) => h.confidence >= minSearchConfidence).slice(0, 6);
  const limit = pLimit(CONCURRENCY);

  // 1) Direct verification of each candidate
  const verified = await Promise.all(
    top.map((h) =>
      limit(async () => {
        // Fetch candidate page
        const pg = await fetchPage(h.url);
        if (!pg) {
          debugArr.push({
            feedUrl: h.url,
            ok: false,
            source: "web",
            latest: null,
            recentWithinWindow: false,
            confidence: Number(h.confidence.toFixed(2)),
            reason: [...h.reason, "fetch_failed"],
            error: "fetch_failed",
          });
          return null;
        }

        // Date freshness
        const pageISO = extractPublishedISO(pg.html);
        let recentOK = true; // default permissive if no date available
        if (pageISO) {
          const d = new Date(pageISO);
          recentOK = !isNaN(d.getTime()) && daysAgo(d) <= lookbackDays;
        }

        // Content acceptance
        const verdict = acceptArticleByBody(pg.text, author, bookTitle, requireBookMatch);

        // Origin guard:
        // - Accept if official author site
        // - OR recognized news host
        // - OR page shows byline/meta author matching the author
        const isOfficial = looksLikeOfficialAuthorSite(h.title || "", h.host, author);
        const isNewsish = looksLikeNewsHost(h.host);
        const bylineOK = pageHasBylineForAuthor(pg.html, pg.text, author);
        const originOK = isOfficial || isNewsish || bylineOK;

        const pass = verdict.pass && recentOK && originOK;

        debugArr.push({
          feedUrl: h.url,
          ok: pass,
          source: "web",
          latest: pageISO || null,
          recentWithinWindow: recentOK,
          confidence: Number(h.confidence.toFixed(2)),
          reason: [...h.reason, ...verdict.reason, originOK ? "origin_ok" : "origin_missing"],
          error: null,
        });

        if (pass) {
          return {
            author_name: author,
            has_recent: true,
            latest_title: h.title,
            latest_url: h.url,
            published_at: pageISO || undefined,
            source: "web" as const,
            author_url: `https://${h.host}`,
          };
        }
        return null;
      })
    )
  );

  const winner = verified.find(Boolean) as CacheValue | undefined;
  return winner || null;
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

    const bookTitle = typeof body.book_title === "string" ? body.book_title : "";

    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    const includeSearch: boolean = body.include_search !== false; // default true
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

    const debug: boolean = body.debug === true;

    // cache key
    const cacheKey = makeCacheKey({
      author,
      bookTitle,
      lookback,
      includeSearch,
      strictAuthorMatch,
      requireBookMatch,
      fallbackAuthorOnly,
      minConfidence,
      minSearchConfidence,
    });
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    const debugArr: CacheValue["_debug"] = [];

    if (!includeSearch) {
      const empty: CacheValue = { author_name: author, has_recent: false };
      if (debug) empty._debug = debugArr;
      setCached(cacheKey, empty);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(empty);
    }

    // 1) Search
    const hits = await googleSearchHits(author, bookTitle, lookback);

    // 2) Verify & pick winner
    let winner = await verifyTopCandidates(
      hits,
      author,
      bookTitle,
      lookback,
      requireBookMatch,
      minSearchConfidence,
      debugArr
    );

    // 3) Optional fallback to author-only if requested
    if (!winner && fallbackAuthorOnly && !requireBookMatch) {
      // re-score with book disabled but still apply origin guard
      const altHits = await googleSearchHits(author, "", lookback);
      winner = await verifyTopCandidates(
        altHits,
        author,
        "", // no book required
        lookback,
        false, // relax book requirement
        minSearchConfidence,
        debugArr
      );
    }

    // 4) Strict author gate
    if (strictAuthorMatch && !winner) {
      const reject = { error: "no_confident_author_match" };
      if (debug) {
        res.setHeader("x-cache", "MISS");
        return res.status(200).json({ ...reject, _debug: debugArr });
      }
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(reject);
    }

    // 5) Nothing
    if (!winner) {
      const empty: CacheValue = { author_name: author, has_recent: false };
      if (debug) empty._debug = debugArr;
      setCached(cacheKey, empty);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(empty);
    }

    if (debug) winner._debug = debugArr;
    setCached(cacheKey, winner);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(winner);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
