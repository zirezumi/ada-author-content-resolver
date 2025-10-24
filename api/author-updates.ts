// api/author-updates.ts
/// <reference types="node" />

/**
 * Author Updates Resolver (Google CSE single-path)
 *
 * - Auth via X-Auth and env AUTHOR_UPDATES_SECRET (comma-separated allowed)
 * - 24h in-memory cache
 * - Google CSE only:
 *   • primary:  author + book + (interview|conversation|q&a) + domain exclusions
 *   • broad:    author + book + exclusions (if primary found nothing acceptable)
 *   • fallback: author + exclusions (if still nothing and fallback_author_only=true)
 * - Scoring & acceptance:
 *   • Blocks social/shopping domains
 *   • Prefers official/author-authored content
 *   • Allows featured interviews/conversations
 *   • Enforces require_book_match (if requested)
 *   • Requires freshness within lookback_days when a date is present
 * - Returns _debug rows on failure if debug=true
 */

export const config = { runtime: "nodejs" } as const;

/* =============================
   Auth
   ============================= */
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
const LOOKBACK_DEFAULT = 60; // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 120;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 6000;
const USER_AGENT = "AuthorUpdates/2.0 (+https://example.com)";

const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const GOOGLE_CSE_CX = process.env.GOOGLE_CSE_CX || "";
const CSE_COUNT = 10;

/** Domains to exclude (social, shopping, aggregator noise). */
const HARD_BLOCK_HOSTS = new Set<string>([
  // social
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "youtube.com",
  "www.youtube.com",
  "m.youtube.com",
  "youtu.be",

  // shopping/retail
  "amazon.com",
  "www.amazon.com",
  "amazon.co.uk",
  "amazon.de",
  "amazon.fr",
  "amazon.ca",
  "amazon.com.au",
  "a.co",
  "bookshop.org",
  "www.bookshop.org",
  "barnesandnoble.com",
  "www.barnesandnoble.com",
  "books.google.com",
  "www.books.google.com",
  "indigo.ca",
  "www.indigo.ca",

  // podcast platforms (often non-authored content)
  "open.spotify.com",
  "spotify.com",
  "podcasts.apple.com",
  "music.apple.com",
  "apple.com",

  // newsletter platforms that are often noisy in search (you can remove if you want)
  "substack.com",
  "www.substack.com",
]);

/** Signals for interview/conversation content. */
const CONVERSATION_HINTS = [
  "interview",
  "in conversation",
  "conversation",
  "q&a",
  "q & a",
  "ask me anything",
  "fireside chat",
  "panel",
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
  source?: string; // "web"
  author_url?: string;
  _debug?: Array<DebugRow>;
};

type DebugRow = {
  feedUrl: string;
  ok: boolean;
  source: string; // "web"
  latest: string | null; // publish iso if parsed
  recentWithinWindow: boolean;
  confidence: number;
  reason: string[];
  siteTitle?: string | null;
  feedTitle?: string | null;
  error: string | null;
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type ScoreOptions = {
  authorName: string;
  bookTitle?: string;
  lookbackDays: number;
  requireBookMatch: boolean;
  minSearchConfidence: number;
  knownHosts: string[];
  authorOnly?: boolean;
};

type ScoredHit = {
  title: string;
  url: string;
  host: string;
  snippet: string;
  publishedAt?: string;
  publishedTs?: number;
  confidence: number;
  accepted: boolean;
  reason: string[];
  category?: "authored" | "official" | "featured" | "participation";
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(body: Record<string, unknown>) {
  // Build a stable key from the meaningful knobs
  const {
    author_name,
    book_title = "",
    known_urls = [],
    lookback_days,
    include_search,
    strict_author_match,
    require_book_match,
    fallback_author_only,
    min_confidence,
    min_search_confidence,
  } = body;

  const urls = Array.isArray(known_urls)
    ? [...known_urls].map((u) => String(u).trim().toLowerCase()).sort().join("|")
    : "";

  return [
    String(author_name || "").trim().toLowerCase(),
    String(book_title || "").trim().toLowerCase(),
    urls,
    String(lookback_days || LOOKBACK_DEFAULT),
    include_search ? "search" : "nosearch",
    strict_author_match ? "strict" : "loose",
    require_book_match ? "book" : "nobook",
    fallback_author_only ? "fallbk" : "nofallbk",
    String(min_confidence ?? ""),
    String(min_search_confidence ?? ""),
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
function daysAgo(d: Date) {
  return (Date.now() - d.getTime()) / 86_400_000;
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
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

function hostOf(u: string) {
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
    .replace(/[^\p{L}\p{N}\s.'’-]/gu, " ")
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

function someOverlap(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.some((n) => H.has(n));
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

/* =============================
   Google CSE
   ============================= */
function dateRestrictFromLookback(days: number) {
  const d = clamp(Math.round(days), LOOKBACK_MIN, LOOKBACK_MAX);
  return `d${d}`;
}

type CseItem = {
  title?: string;
  link?: string;
  displayLink?: string;
  snippet?: string;
  htmlSnippet?: string;
  pagemap?: any;
};

async function googleCseSearch(query: string, dateRestrict: string): Promise<CseItem[]> {
  if (!GOOGLE_CSE_KEY || !GOOGLE_CSE_CX) return [];
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", GOOGLE_CSE_KEY);
  url.searchParams.set("cx", GOOGLE_CSE_CX);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(CSE_COUNT));
  url.searchParams.set("dateRestrict", dateRestrict);
  url.searchParams.set("safe", "off");

  const resp = await fetchWithTimeout(url.toString(), undefined, FETCH_TIMEOUT_MS);
  if (!resp?.ok) return [];
  const json: any = await resp.json();
  const items: any[] = Array.isArray(json?.items) ? json.items : [];
  return items as CseItem[];
}

function parsePublishedAt(item: CseItem): { publishedAt?: string; ts?: number } {
  // Try common metatag slots
  const meta = item.pagemap?.metatags?.[0] || {};
  const candidates: string[] = [
    meta["article:published_time"],
    meta["og:updated_time"],
    meta["article:modified_time"],
    meta["date"],
    meta["dcterms.date"],
    meta["dc.date"],
  ].filter(Boolean);
  for (const iso of candidates) {
    const d = new Date(iso);
    if (!isNaN(d.getTime())) return { publishedAt: d.toISOString(), ts: d.getTime() };
  }
  // Fallback: sometimes snippet has a "X hours ago" format; ignoring for simplicity
  return {};
}

/* =============================
   Scoring
   ============================= */
function scoreItem(item: CseItem, opts: ScoreOptions): ScoredHit | null {
  const title = String(item.title || "").trim();
  const url = String(item.link || "").trim();
  if (!url || !isHttpUrl(url) || !title) return null;

  const host = hostOf(url);
  const snippet = String(item.snippet || item.htmlSnippet || "").trim();

  // Hard block noisy domains
  if (HARD_BLOCK_HOSTS.has(host)) {
    return {
      title,
      url,
      host,
      snippet,
      confidence: 0,
      accepted: false,
      reason: ["blocked:domain"],
    };
  }

  const { publishedAt, ts } = parsePublishedAt(item);
  const withinWindow =
    publishedAt && ts ? daysAgo(new Date(ts)) <= opts.lookbackDays : true; // if unknown date, don't reject solely for that

  const tTitle = tokens(title);
  const tSnip = tokens(snippet);
  const tBoth = Array.from(new Set([...tTitle, ...tSnip]));

  const tAuthor = tokens(opts.authorName);
  const tBook = tokens(opts.bookTitle || "");

  let confidence = 0;
  const reason: string[] = [];

  // Book requirement (if enabled)
  const hasBook =
    tBook.length > 0 && (jaccard(tBook, tBoth) >= 0.25 || containsAll(tBoth, tBook));
  if (opts.requireBookMatch && !hasBook && !opts.authorOnly) {
    // allow scorer to continue gathering reasons/confidence, but will fail acceptance gate
    reason.push("missing:book_match");
  } else if (hasBook) {
    confidence += 0.25;
    reason.push("book_match");
  }

  // Author presence
  const authorIn = tAuthor.length > 0 && (jaccard(tAuthor, tBoth) >= 0.25 || containsAll(tBoth, tAuthor));
  if (authorIn) {
    confidence += 0.35;
    reason.push("author_match");
  } else if (!opts.authorOnly) {
    reason.push("missing:author_match");
  }

  // Official / author-authored bias
  const knownHostHit = opts.knownHosts.includes(host);
  if (knownHostHit) {
    confidence += 0.25;
    reason.push("official_domain");
  } else {
    // Detect "officialness" by displayLink == host or site name in title
    const displayHost = (item as any).displayLink ? String((item as any).displayLink).toLowerCase() : "";
    if (displayHost && displayHost === host) {
      confidence += 0.05;
      reason.push("display_host_match");
    }
  }

  // Featured/interview/conversation hints in title or snippet
  if (someOverlap(tBoth, CONVERSATION_HINTS.flatMap(tokens))) {
    confidence += 0.2;
    reason.push("conversation_hint");
  }

  // Freshness bonus when date present
  if (publishedAt && withinWindow) {
    confidence += 0.2;
    reason.push("fresh_within_window");
  }

  // Minimal identity guard: require at least some author signal unless authorOnly=false and book match is very strong
  const identityOK = authorIn || (opts.authorOnly ? true : (opts.requireBookMatch && hasBook));
  if (!identityOK) reason.push("identity_guard_fail");

  // Accept thresholds
  const accepted =
    identityOK &&
    withinWindow &&
    confidence >= opts.minSearchConfidence &&
    (!opts.requireBookMatch || hasBook || opts.authorOnly);

  return {
    title,
    url,
    host,
    snippet,
    publishedAt,
    publishedTs: ts,
    confidence: Number(confidence.toFixed(3)),
    accepted,
    reason,
    category: knownHostHit ? "official" : someOverlap(tBoth, CONVERSATION_HINTS.flatMap(tokens)) ? "featured" : authorIn ? "participation" : undefined,
  };
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

    if (!GOOGLE_CSE_KEY || !GOOGLE_CSE_CX) {
      return res.status(500).json({ error: "server_misconfigured: missing GOOGLE_CSE_KEY/GOOGLE_CSE_CX" });
    }

    const bodyRaw = req.body ?? {};
    const body = typeof bodyRaw === "string" ? JSON.parse(bodyRaw || "{}") : bodyRaw;

    const rawAuthor = (body.author_name ?? "").toString();
    const authorName = normalizeAuthor(rawAuthor);
    if (!authorName) return res.status(400).json({ error: "author_name required" });

    const bookTitle = typeof body.book_title === "string" ? body.book_title : "";

    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    let knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls : [];
    knownUrls = knownUrls.map((u: unknown) => String(u).trim()).filter(isHttpUrl);
    const knownHosts = Array.from(new Set(knownUrls.map(hostOf).filter(Boolean)));

    const includeSearch: boolean = body.include_search !== false; // default true
    const strictAuthorMatch: boolean = body.strict_author_match === true;
    const requireBookMatch: boolean = body.require_book_match === true;
    const fallbackAuthorOnly: boolean = body.fallback_author_only === true;

    const minConfidence: number =
      typeof body.min_confidence === "number" ? Math.max(0, Math.min(1, body.min_confidence)) : 0.5;

    const minSearchConfidence: number =
      typeof body.min_search_confidence === "number" ? Math.max(0, Math.min(1, body.min_search_confidence)) : 0.45;

    const debug: boolean = body.debug === true;

    // Cache
    const cacheKey = makeCacheKey(body);
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    if (!includeSearch) {
      const empty: CacheValue = { author_name: authorName, has_recent: false };
      if (debug) empty._debug = [];
      setCached(cacheKey, empty);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(empty);
    }

    const dateRestrict = dateRestrictFromLookback(lookback);

    // Build queries
    const baseQuoted = `"${authorName}"`;
    const bookQuoted = bookTitle ? ` "${bookTitle}"` : "";

    const exclusions =
      "-site:x.com -site:twitter.com -site:facebook.com -site:instagram.com -site:tiktok.com -site:linkedin.com -site:youtube.com -site:amazon.com -site:amazon.co.uk -site:amazon.de -site:amazon.fr -site:amazon.ca -site:amazon.com.au -site:bookshop.org -site:barnesandnoble.com -site:books.google.com -site:open.spotify.com -site:podcasts.apple.com -site:music.apple.com -site:apple.com -site:substack.com";

    const convoOr = `(${CONVERSATION_HINTS.map((h) => `"${h}"`).join(" OR ")})`;

    const primaryQ = `${baseQuoted}${bookQuoted} ${convoOr} ${exclusions}`.trim();
    const broadQ = `${baseQuoted}${bookQuoted} ${exclusions}`.trim();
    const authorOnlyQ = `${baseQuoted} ${exclusions}`.trim();

    // Score options
    const scorer = (items: CseItem[], opts: Partial<ScoreOptions> = {}) => {
      const list: ScoredHit[] = [];
      for (const it of items) {
        const scored = scoreItem(it, {
          authorName,
          bookTitle,
          lookbackDays: lookback,
          requireBookMatch: requireBookMatch && !opts.authorOnly, // if author-only pass, don't require book
          minSearchConfidence: minSearchConfidence,
          knownHosts,
          authorOnly: !!opts.authorOnly,
        });
        if (scored) list.push(scored);
      }
      return list;
    };

    const examined: DebugRow[] = [];

    // Pass 1: primary
    const p1 = await googleCseSearch(primaryQ, dateRestrict);
    const s1 = scorer(p1);
    examined.push(
      ...s1.map((h) => ({
        feedUrl: h.url,
        ok: h.accepted,
        source: "web",
        latest: h.publishedAt || null,
        recentWithinWindow: !!(h.publishedAt ? daysAgo(new Date(h.publishedAt)) <= lookback : true),
        confidence: h.confidence,
        reason: h.reason,
        siteTitle: null,
        feedTitle: null,
        error: null,
      }))
    );

    // If nothing accepted, Pass 2: broad
    let pool = s1;
    if (!pool.some((h) => h.accepted)) {
      const p2 = await googleCseSearch(broadQ, dateRestrict);
      const s2 = scorer(p2);
      pool = pool.concat(s2);
      examined.push(
        ...s2.map((h) => ({
          feedUrl: h.url,
          ok: h.accepted,
          source: "web",
          latest: h.publishedAt || null,
          recentWithinWindow: !!(h.publishedAt ? daysAgo(new Date(h.publishedAt)) <= lookback : true),
          confidence: h.confidence,
          reason: h.reason,
          siteTitle: null,
          feedTitle: null,
          error: null,
        }))
      );
    }

    // If still nothing accepted and fallback allowed, Pass 3: author-only
    if (!pool.some((h) => h.accepted) && fallbackAuthorOnly) {
      const p3 = await googleCseSearch(authorOnlyQ, dateRestrict);
      const s3 = scorer(p3, { authorOnly: true });
      pool = pool.concat(s3);
      examined.push(
        ...s3.map((h) => ({
          feedUrl: h.url,
          ok: h.accepted,
          source: "web",
          latest: h.publishedAt || null,
          recentWithinWindow: !!(h.publishedAt ? daysAgo(new Date(h.publishedAt)) <= lookback : true),
          confidence: h.confidence,
          reason: h.reason,
          siteTitle: null,
          feedTitle: null,
          error: null,
        }))
      );
    }

    // Choose best accepted
    const accepted = pool
      .filter((h) => h.accepted)
      .sort((a, b) => (b.confidence - a.confidence) || ((b.publishedTs || 0) - (a.publishedTs || 0)));

    const top = accepted[0];

    if (top) {
      const payload: CacheValue = {
        author_name: authorName,
        has_recent: true,
        latest_title: top.title,
        latest_url: top.url,
        published_at: top.publishedAt,
        source: "web",
        author_url: `https://${top.host}`,
      };
      if (debug) payload._debug = examined.slice(0, 30);
      setCached(cacheKey, payload);
      res.setHeader("x-cache", cached ? "HIT" : "MISS");
      return res.status(200).json(payload);
    }

    // STRICT author gate: if requested, reject when nothing above minConfidence
    if (strictAuthorMatch) {
      const goodIdentity = pool
        .filter((h) => h.confidence >= minConfidence)
        .sort((a, b) => (b.confidence - a.confidence) || ((b.publishedTs || 0) - (a.publishedTs || 0)))[0];

      if (!goodIdentity) {
        const empty: CacheValue = { author_name: authorName, has_recent: false };
        if (debug) empty._debug = examined.slice(0, 30);
        setCached(cacheKey, empty);
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(debug ? empty : { error: "no_confident_author_match" });
      }
    }

    // Nothing acceptable
    const empty: CacheValue = { author_name: authorName, has_recent: false };
    if (debug) empty._debug = examined.slice(0, 30);
    setCached(cacheKey, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(debug ? empty : { error: "no_confident_author_match" });
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
