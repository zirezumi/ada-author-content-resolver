// api/author-updates.ts
/// <reference types="node" />

/**
 * Author Updates Resolver (Google CSE, debug-rich)
 * - POST only
 * - Auth via X-Auth against AUTHOR_UPDATES_SECRET (comma-separated allowed)
 * - 24h in-memory cache
 * - Google Custom Search (CSE) pagination (up to 30 results)
 * - Scoring tuned for author/book, with interview/conversation hints
 * - Social & shopping blocklists
 * - Optional fallback to strong author + official domain when allowed
 */

export const config = { runtime: "nodejs" } as const;

/* =============================
   Tunables
   ============================= */
const LOOKBACK_DEFAULT = 30;                  // days
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 120;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;     // 24h
const FETCH_TIMEOUT_MS = 6000;
const USER_AGENT = "AuthorUpdates/2.0 (+https://example.com)";

const GOOGLE_CSE_PAGES = [1, 11, 21];         // 3 pages * 10 items = up to 30
const GOOGLE_CSE_NUM = 10;                    // Google CSE limit per page

// Soft boosts (0..1) — tune here
const SCORE = {
  titleHasAuthor: 0.35,
  titleOrSnippetHasBook: 0.25,
  snippetHasAuthor: 0.15,
  conversationHint: 0.15,
  officialDomainBonus: 0.25,
};

const CONVERSATION_HINTS = [
  "interview",
  "in conversation",
  "q&a",
  "conversation",
  "live",
];

const SHOPPING_HOSTS = new Set([
  "amazon.com", "amazon.co.uk", "amazon.ca", "amazon.com.au", "amazon.de", "amazon.fr", "amazon.es", "amazon.it",
  "barnesandnoble.com", "bookshop.org", "books.google.com",
  "ebay.com", "etsy.com",
  "walmart.com", "target.com",
  "audible.com", "audible.co.uk"
]);

const SOCIAL_HOSTS = new Set([
  "x.com", "twitter.com",
  "facebook.com", "www.facebook.com", "m.facebook.com",
  "instagram.com", "www.instagram.com",
  "tiktok.com", "www.tiktok.com",
  "linkedin.com", "www.linkedin.com",
  "youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be",
  "open.spotify.com",
  "music.apple.com", "podcasts.apple.com", "apple.com"
]);

// You can override/add to blocklists via env (comma-separated hosts)
const EXTRA_SHOPPING = (process.env.EXTRA_SHOPPING_BLOCKLIST || "")
  .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
EXTRA_SHOPPING.forEach(h => SHOPPING_HOSTS.add(h));

const EXTRA_SOCIAL = (process.env.EXTRA_SOCIAL_BLOCKLIST || "")
  .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
EXTRA_SOCIAL.forEach(h => SOCIAL_HOSTS.add(h));

/* =============================
   Auth
   ============================= */
const AUTH_SECRETS = (process.env.AUTHOR_UPDATES_SECRET || "")
  .split(",").map(s => s.trim()).filter(Boolean);
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
    ok: boolean; // accepted by policy
    source: string; // "web"
    latest: string | null; // publish date if known (CSE usually lacks)
    recentWithinWindow: boolean; // true because of dateRestrict if used
    confidence: number;
    reason: string[];
    siteTitle?: string | null;
    feedTitle?: string | null;
    error: string | null;
  }>;
};

type CacheEntry = { expiresAt: number; value: CacheValue };

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();

/* =============================
   Utilities
   ============================= */
function isHttpUrl(s: string) {
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch { return false; }
}

function hostOf(u: string) {
  try { return new URL(u).host.toLowerCase(); } catch { return ""; }
}

function tokens(s: string): string[] {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function jaccard(a: string[], b: string[]) {
  const A = new Set(a), B = new Set(b);
  const inter = [...A].filter(x => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}

function containsAny(hay: string[], needles: string[]) {
  if (!needles.length) return false;
  const H = new Set(hay);
  return needles.some(n => H.has(n));
}

function containsAll(hay: string[], needles: string[]) {
  if (!needles.length) return false;
  const H = new Set(hay);
  return needles.every(n => H.has(n));
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function daysToGoogleDateRestrict(days: number) {
  const d = clamp(days, LOOKBACK_MIN, LOOKBACK_MAX);
  return `d${d}`;
}

function makeCacheKey(input: any) {
  return JSON.stringify({
    a: (input.author_name || "").toLowerCase().trim(),
    b: (input.book_title || "").toLowerCase().trim(),
    lb: clamp(Number(input.lookback_days ?? LOOKBACK_DEFAULT), LOOKBACK_MIN, LOOKBACK_MAX),
    inc: !!input.include_search,
    strict: !!input.strict_author_match,
    reqBook: !!input.require_book_match,
    fbAuthor: !!input.fallback_author_only,
    minC: typeof input.min_confidence === "number" ? input.min_confidence : 0.5,
    minSC: typeof input.min_search_confidence === "number" ? input.min_search_confidence : 0.45,
    known: Array.isArray(input.known_urls) ? input.known_urls : [],
    cx: process.env.GOOGLE_CSE_CX || "",
  });
}

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

/* =============================
   Google CSE
   ============================= */
async function googleCseSearchAll(
  q: string,
  dateRestrict: string,
  cx: string,
  apiKey: string
): Promise<any[]> {
  const items: any[] = [];
  for (const start of GOOGLE_CSE_PAGES) {
    const url = new URL("https://www.googleapis.com/customsearch/v1");
    url.searchParams.set("key", apiKey);
    url.searchParams.set("cx", cx);
    url.searchParams.set("q", q);
    url.searchParams.set("dateRestrict", dateRestrict);
    url.searchParams.set("num", String(GOOGLE_CSE_NUM));
    url.searchParams.set("start", String(start));
    // Avoid lr/hl for maximum recall

    const r = await fetchWithTimeout(url.toString());
    if (!r?.ok) break;
    const j = await r.json();
    if (Array.isArray(j?.items)) items.push(...j.items);
  }
  return items;
}

/* =============================
   Scoring
   ============================= */
type Scored = {
  url: string;
  title: string;
  snippet: string;
  host: string;
  score: number;
  reasons: string[];
  withinWindow: boolean; // true when dateRestrict is used; CSE items rarely have dates
  isOfficialDomain: boolean;
  hasAuthor: boolean;
  hasBook: boolean;
  hasConversationHint: boolean;
  accepted: boolean;
  error?: string;
};

function isShopping(host: string) {
  if (!host) return false;
  const parts = host.split(".").slice(-2).join(".");
  return SHOPPING_HOSTS.has(host) || SHOPPING_HOSTS.has(parts);
}

function isSocial(host: string) {
  if (!host) return false;
  const parts = host.split(".").slice(-2).join(".");
  return SOCIAL_HOSTS.has(host) || SOCIAL_HOSTS.has(parts);
}

function scoreCseItem(
  it: any,
  opts: {
    authorName: string;
    bookTitle: string;
    requireBookMatch: boolean;
    strictAuthorMatch: boolean;
    minSearchConfidence: number;
    officialHosts: string[];
  }
): Scored {
  const url = String(it?.link || it?.url || "");
  const title = String(it?.title || it?.name || "");
  const snippet = String(it?.snippet || it?.description || "");
  const host = hostOf(url);
  const tTitle = tokens(title);
  const tSnip = tokens(snippet);
  const tCombined = Array.from(new Set([...tTitle, ...tSnip]));
  const tAuthor = tokens(opts.authorName);
  const tBook = tokens(opts.bookTitle);

  const reasons: string[] = [];
  let score = 0;

  // Blocklists first
  if (isSocial(host)) {
    return {
      url, title, snippet, host,
      score: 0, reasons: ["rejected:social_site"],
      withinWindow: true,
      isOfficialDomain: false,
      hasAuthor: false,
      hasBook: false,
      hasConversationHint: false,
      accepted: false,
    };
  }
  if (isShopping(host)) {
    return {
      url, title, snippet, host,
      score: 0, reasons: ["rejected:shopping_site"],
      withinWindow: true,
      isOfficialDomain: false,
      hasAuthor: false,
      hasBook: false,
      hasConversationHint: false,
      accepted: false,
    };
  }

  const isOfficial = opts.officialHosts.includes(host);
  if (isOfficial) {
    score += SCORE.officialDomainBonus;
    reasons.push(`official_domain:${host}`);
  }

  // Author presence
  const titleHasAuthor = tAuthor.length && (jaccard(tAuthor, tTitle) >= 0.3 || containsAll(tTitle, tAuthor));
  const snippetHasAuthor = tAuthor.length && (jaccard(tAuthor, tSnip) >= 0.3 || containsAll(tSnip, tAuthor));
  if (titleHasAuthor) { score += SCORE.titleHasAuthor; reasons.push("title_has_author"); }
  if (snippetHasAuthor) { score += SCORE.snippetHasAuthor; reasons.push("snippet_has_author"); }
  const hasAuthor = titleHasAuthor || snippetHasAuthor;

  // Book presence
  const titleHasBook = tBook.length && (jaccard(tBook, tTitle) >= 0.25 || containsAll(tTitle, tBook));
  const snippetHasBook = tBook.length && (jaccard(tBook, tSnip) >= 0.25 || containsAll(tSnip, tBook));
  if (titleHasBook || snippetHasBook) { score += SCORE.titleOrSnippetHasBook; reasons.push("title_or_snippet_has_book"); }
  const hasBook = titleHasBook || snippetHasBook;

  // Conversation hint
  const hasConvHint = containsAny(tCombined, CONVERSATION_HINTS.map(h => h.toLowerCase()));
  if (hasConvHint) { score += SCORE.conversationHint; reasons.push("conversation_hint"); }

  // Decision gates
  const requireBook = !!opts.requireBookMatch;
  const strict = !!opts.strictAuthorMatch;

  // Strict author match: must have author (title or snippet)
  if (strict && !hasAuthor) {
    return {
      url, title, snippet, host,
      score, reasons: [...reasons, "rejected:strict_author_missing"],
      withinWindow: true,
      isOfficialDomain: isOfficial,
      hasAuthor, hasBook, hasConversationHint: hasConvHint,
      accepted: false,
    };
  }

  // Require book: must have book (title or snippet)
  if (requireBook && !hasBook) {
    return {
      url, title, snippet, host,
      score, reasons: [...reasons, "rejected:require_book_missing"],
      withinWindow: true,
      isOfficialDomain: isOfficial,
      hasAuthor, hasBook, hasConversationHint: hasConvHint,
      accepted: false,
    };
  }

  // Accept if above threshold (the handler applies final numeric threshold)
  return {
    url, title, snippet, host,
    score,
    reasons,
    withinWindow: true, // CSE dateRestrict ensures freshness
    isOfficialDomain: isOfficial,
    hasAuthor, hasBook, hasConversationHint: hasConvHint,
    accepted: true,
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

    const bodyRaw = req.body ?? {};
    const body = typeof bodyRaw === "string" ? JSON.parse(bodyRaw || "{}") : bodyRaw;

    const author = String(body.author_name || "").trim();
    if (!author) return res.status(400).json({ error: "author_name required" });

    const bookTitle = String(body.book_title || "").trim();
    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);

    const includeSearch = body.include_search === true;
    const strictAuthorMatch = body.strict_author_match === true;
    const requireBookMatch = body.require_book_match === true;
    const fallbackAuthorOnly = body.fallback_author_only === true;

    const minConfidence = typeof body.min_confidence === "number" ? body.min_confidence : 0.5;
    const minSearchConfidence = typeof body.min_search_confidence === "number" ? body.min_search_confidence : 0.45;

    const knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls.filter(isHttpUrl) : [];
    const officialHosts = Array.from(new Set(knownUrls.map(hostOf).filter(Boolean)));

    const debug = body.debug === true;

    const cacheKey = makeCacheKey({
      author_name: author,
      book_title: bookTitle,
      lookback_days: lookback,
      include_search: includeSearch,
      strict_author_match: strictAuthorMatch,
      require_book_match: requireBookMatch,
      fallback_author_only: fallbackAuthorOnly,
      min_confidence: minConfidence,
      min_search_confidence: minSearchConfidence,
      known_urls: knownUrls
    });

    const cached = CACHE.get(cacheKey);
    if (cached && Date.now() < cached.expiresAt && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached.value);
    }

    const examined: CacheValue["_debug"] = [];

    // If search disabled or CSE not configured
    const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
    const CSE_CX = process.env.GOOGLE_CSE_CX || "";
    if (!includeSearch || !CSE_KEY || !CSE_CX) {
      const empty: CacheValue = { author_name: author, has_recent: false };
      if (debug) empty._debug = examined;
      CACHE.set(cacheKey, { value: empty, expiresAt: Date.now() + CACHE_TTL_MS });
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(empty);
    }

    // Build query with soft filters (avoid social/shopping via -site: where possible)
    // We’ll still apply hard host-level rejections in scoring.
    const qParts: string[] = [];
    if (author) qParts.push(`"${author}"`);
    if (bookTitle) qParts.push(`"${bookTitle}"`);
    qParts.push('(interview OR "in conversation" OR "Q&A" OR conversation)');

    // Add -site filters for big socials/shopping to reduce noise
    const siteExcludes = [
      "x.com", "twitter.com",
      "facebook.com", "instagram.com", "tiktok.com", "linkedin.com",
      "youtube.com", "amazon.com", "open.spotify.com",
      "apple.com", "podcasts.apple.com"
    ].map(h => `-site:${h}`);

    const q = `${qParts.join(" ")} ${siteExcludes.join(" ")}`.trim();
    const dateRestrict = daysToGoogleDateRestrict(lookback);

    // Pull up to 30 results
    const items = await googleCseSearchAll(q, dateRestrict, CSE_CX, CSE_KEY);

    // Score and capture ALL (even rejects) into examined
    const scored: Scored[] = items.map(it =>
      scoreCseItem(it, {
        authorName: author,
        bookTitle,
        requireBookMatch,
        strictAuthorMatch,
        minSearchConfidence,
        officialHosts
      })
    );

    // Log to debug
    for (const s of scored) {
      examined.push({
        feedUrl: s.url,
        ok: s.accepted && s.score >= minSearchConfidence,
        source: "web",
        latest: null,
        recentWithinWindow: true,
        confidence: Number(s.score.toFixed(2)),
        reason: s.reasons,
        siteTitle: null,
        feedTitle: null,
        error: s.accepted ? null : (s.reasons.find(r => r.startsWith("rejected:")) || null)
      });
    }

    // Filter accepteds, then threshold
    let accepteds = scored.filter(s => s.accepted && s.score >= minSearchConfidence);

    // Prefer items that have BOTH author & book
    const both = accepteds.filter(s => s.hasAuthor && s.hasBook);
    const onlyAuthor = accepteds.filter(s => s.hasAuthor && !s.hasBook);
    const onlyBook = accepteds.filter(s => s.hasBook && !s.hasAuthor);

    const sortByScore = (a: Scored, b: Scored) => b.score - a.score;

    // choose best
    let winner: Scored | undefined;

    if (both.length) {
      winner = both.sort(sortByScore)[0];
    } else if (requireBookMatch) {
      // require book but we didn't get BOTH — try book-only first
      if (onlyBook.length) winner = onlyBook.sort(sortByScore)[0];
      // optional fallback: strong author + official domain (even if book missing)
      if (!winner && fallbackAuthorOnly) {
        const strongAuthorOfficial = onlyAuthor
          .filter(s => s.isOfficialDomain && s.score >= Math.max(minConfidence, minSearchConfidence))
          .sort(sortByScore)[0];
        if (strongAuthorOfficial) {
          winner = strongAuthorOfficial;
          examined?.push({
            feedUrl: strongAuthorOfficial.url,
            ok: true,
            source: "web",
            latest: null,
            recentWithinWindow: true,
            confidence: Number(strongAuthorOfficial.score.toFixed(2)),
            reason: [...strongAuthorOfficial.reasons, "fallback_author_only"],
            siteTitle: null,
            feedTitle: null,
            error: null
          });
        }
      }
    } else {
      // Book not required — prefer author-only if strong, else any accepted
      if (!winner && onlyAuthor.length) winner = onlyAuthor.sort(sortByScore)[0];
      if (!winner && accepteds.length) winner = accepteds.sort(sortByScore)[0];
    }

    if (winner) {
      const payload: CacheValue = {
        author_name: author,
        has_recent: true,
        latest_title: winner.title,
        latest_url: winner.url,
        source: "web",
        author_url: `https://${winner.host}`
      };
      if (debug) payload._debug = examined.slice(0, 40);
      CACHE.set(cacheKey, { value: payload, expiresAt: Date.now() + CACHE_TTL_MS });
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // If nothing acceptable, but maybe an official domain result exists with strong author presence:
    if (fallbackAuthorOnly) {
      const officialStrong = scored
        .filter(s => s.isOfficialDomain && s.hasAuthor && s.score >= Math.max(minConfidence, minSearchConfidence))
        .sort(sortByScore)[0];
      if (officialStrong) {
        const payload: CacheValue = {
          author_name: author,
          has_recent: true,
          latest_title: officialStrong.title,
          latest_url: officialStrong.url,
          source: "web",
          author_url: `https://${officialStrong.host}`
        };
        if (debug) {
          payload._debug = [
            ...(examined || []),
            {
              feedUrl: officialStrong.url,
              ok: true,
              source: "web",
              latest: null,
              recentWithinWindow: true,
              confidence: Number(officialStrong.score.toFixed(2)),
              reason: [...officialStrong.reasons, "fallback_author_only_terminal"],
              siteTitle: null,
              feedTitle: null,
              error: null
            }
          ].slice(0, 40);
        }
        CACHE.set(cacheKey, { value: payload, expiresAt: Date.now() + CACHE_TTL_MS });
        res.setHeader("x-cache", "MISS");
        return res.status(200).json(payload);
      }
    }

    const empty: CacheValue = { author_name: author, has_recent: false };
    if (debug) empty._debug = examined.slice(0, 60);
    CACHE.set(cacheKey, { value: empty, expiresAt: Date.now() + CACHE_TTL_MS });
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(empty);

  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
