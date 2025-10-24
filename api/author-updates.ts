// api/author-updates.ts
/// <reference types="node" />

/**
 * Single-call resolver that uses Google Custom Search (CSE) to find
 * fresh, relevant links involving an author and optionally a book title.
 *
 * Features:
 * - Auth via X-Auth header (AUTHOR_UPDATES_SECRET supports comma-separated values)
 * - 24h in-memory cache
 * - Google CSE pagination (1..30 results)
 * - Domain blocklists (social, shopping, obvious noise)
 * - Scoring with relaxed signals (author/book in title/snippet, conversation hints, official domain bonus)
 * - Require-book-match and strict-author-match modes
 * - Fallback to author-only when enabled and signal is strong
 * - Full debug: every evaluated candidate is recorded with reasons, even rejections
 */

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
const LOOKBACK_DEFAULT = 30; // days (drives dateRestrict for CSE)
const LOOKBACK_MIN = 1;
const LOOKBACK_MAX = 120;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 4500; // per network call
const USER_AGENT = "AuthorUpdates/2.0 (+https://example.com)";

/* ===== Google CSE ===== */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || process.env.GOOGLE_API_KEY || "";
const CSE_CX = process.env.GOOGLE_CSE_CX || process.env.GOOGLE_CX || "";
const CSE_PAGE_STARTS = [1, 11, 21]; // up to 30 results

/* ===== Scoring weights (0..1 sum-ish) ===== */
const W_TITLE_AUTHOR = 0.35;
const W_SNIPPET_AUTHOR = 0.15;
const W_TITLE_OR_SNIPPET_BOOK = 0.25;
const W_CONVERSATION_HINT = 0.15;
const W_OFFICIAL_DOMAIN = 0.25;
const W_BOOKISH_PATH = 0.15;

/* ===== Threshold defaults ===== */
const DEFAULT_MIN_CONFIDENCE = 0.4;
const DEFAULT_MIN_SEARCH_CONFIDENCE = 0.35;

/* ===== Policy flags ===== */
const STRICT_REJECT_PAYLOAD = { error: "no_confident_author_match" };

/* ===== Domain filtering ===== */
const SOCIAL_DOMAINS = [
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "youtube.com",
  "youtu.be",
  "open.spotify.com",
  "podcasts.apple.com",
  "soundcloud.com",
  "reddit.com",
  "medium.com/@", // noisy profiles; keep generic medium.com allowed
  "substack.com", // newsletter platforms: often not "new content from author" depending on use; keep blocked per current policy
];

const SHOPPING_DOMAINS = [
  "amazon.",
  "amzn.to",
  "barnesandnoble.com",
  "bookshop.org",
  "books.google.",
  "play.google.com/store/books",
  "audible.",
  "walmart.",
  "target.com",
  "apple.com/shop",
  "ebay.",
];

const NOISY_DOMAINS = [
  "pinterest.",
  "imdb.com",
  "goodreads.com", // reviews not author-created
  "genius.com",
  "quora.com",
  "wikidata.org",
  "wikipedia.org",
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
  source?: string;
  author_url?: string;
  _debug?: Array<DebugRow>;
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type DebugRow = {
  feedUrl: string;
  ok: boolean; // accepted after scoring/policy
  source: string; // "web"
  latest: string | null; // not used (CSE has no date); still present for parity
  recentWithinWindow: boolean; // assumed true due to dateRestrict
  confidence: number;
  reason: string[];
  siteTitle?: string | null;
  feedTitle?: string | null;
  error?: string | null;
};

type WebHit = {
  title: string;
  url: string;
  snippet: string;
  host: string;
};

/* =============================
   Cache
   ============================= */
const CACHE = new Map<string, CacheEntry>();

function makeCacheKey(payload: Record<string, unknown>): string {
  // Stable stringify with sorted keys
  const stable = (obj: any) =>
    JSON.stringify(
      Object.keys(obj)
        .sort()
        .reduce((acc: any, k) => {
          const v = obj[k];
          acc[k] = v && typeof v === "object" && !Array.isArray(v) ? stable(v) : v;
          return acc;
        }, {}),
    );

  return "v2::" + stable(payload);
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

function hostOf(u: string) {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}

function isBlockedHost(host: string): string | null {
  const h = host.toLowerCase();

  // Social (exact or starts-with match)
  for (const s of SOCIAL_DOMAINS) {
    if (s.includes("@")) {
      // special "medium.com/@" marker: match path later
      if (h.startsWith("medium.com")) return "social_profile";
      continue;
    }
    if (h === s || h.endsWith("." + s) || s.endsWith(".")) {
      if (h.includes(s.replace(/^\./, ""))) return "social";
    } else if (h === s.replace(/\.$/, "")) {
      return "social";
    }
  }

  // Shopping
  for (const s of SHOPPING_DOMAINS) {
    const base = s.replace(/\.$/, "");
    if (h === base || h.endsWith("." + base) || h.includes(base)) return "shopping";
  }

  // Noisy
  for (const s of NOISY_DOMAINS) {
    const base = s.replace(/\.$/, "");
    if (h === base || h.endsWith("." + base) || h.includes(base)) return "noise";
  }

  return null;
}

function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function containsAll(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.every((n) => H.has(n));
}

function containsAny(hay: string[], needles: string[]) {
  const H = new Set(hay);
  return needles.some((n) => H.has(n));
}

function anyPhrasePresent(text: string, phrases: string[]) {
  const t = text.toLowerCase();
  return phrases.some((p) => t.includes(p.toLowerCase()));
}

function jaccard(a: string[], b: string[]) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}

async function fetchWithTimeout(url: string, init?: RequestInit, ms = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, ...(init?.headers || {}) } as any,
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

/* =============================
   Google CSE
   ============================= */
type CseItem = {
  title?: string;
  link?: string;
  snippet?: string;
  displayLink?: string;
};

async function googleCsePage(q: string, dateRestrict: string, start: number): Promise<CseItem[]> {
  if (!CSE_KEY || !CSE_CX) return [];
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_CX);
  url.searchParams.set("q", q);
  url.searchParams.set("dateRestrict", dateRestrict);
  url.searchParams.set("num", "10");
  url.searchParams.set("start", String(start));
  // Avoid language or country constraints; keep broad
  const resp = await fetchWithTimeout(url.toString());
  if (!resp?.ok) return [];
  const data: any = await resp.json();
  return Array.isArray(data?.items) ? data.items : [];
}

async function googleCseSearch(q: string, dateRestrict: string): Promise<CseItem[]> {
  const pages: CseItem[] = [];
  for (const start of CSE_PAGE_STARTS) {
    // Serial to avoid rate spikiness/429s
    const items = await googleCsePage(q, dateRestrict, start);
    pages.push(...items);
  }
  return pages;
}

/* =============================
   Scoring + Policy
   ============================= */
const CONVO_HINTS = [
  "interview",
  "in conversation",
  "q&a",
  "conversation",
  "talks to",
  "speaks with",
  "fireside",
  "on stage",
  "book talk",
  "in-conversation",
];

function isBookishPath(u: string) {
  try {
    const p = new URL(u).pathname.toLowerCase();
    return (
      p.includes("/books") ||
      p.includes("/book/") ||
      p.includes("/culture/books") ||
      p.includes("/literature") ||
      p.includes("/review") ||
      p.includes("/interview")
    );
  } catch {
    return false;
  }
}

function looksOfficialDomain(host: string, authorName: string, knownHosts: string[]) {
  if (knownHosts.includes(host)) return true;
  // Heuristic: author tokens overlap with host parts
  const aTok = tokens(authorName).filter((t) => t.length >= 3); // ignore tiny tokens
  const hostParts = host.split(".").join(" ").split("-").join(" ").split(" ");
  const hTok = hostParts.map((s) => s.toLowerCase()).filter(Boolean);
  return jaccard(aTok, hTok) >= 0.5;
}

type Scored = {
  url: string;
  title: string;
  snippet: string;
  host: string;
  score: number;
  reasons: string[];
  withinWindow: boolean;
  accepted: boolean;
  error?: string;
};

function scoreItem(
  item: CseItem,
  authorName: string,
  bookTitle: string,
  knownHosts: string[],
  policy: {
    requireBookMatch: boolean;
    strictAuthorMatch: boolean;
    minSearchConfidence: number;
    fallbackAuthorOnly: boolean;
  },
): Scored {
  const title = String(item.title || "");
  const url = String(item.link || "");
  const snippet = String(item.snippet || "");
  const host = hostOf(url);

  const reasons: string[] = [];
  let score = 0;

  // Blocked host?
  const blocked = isBlockedHost(host);
  if (blocked) {
    return {
      url,
      title,
      snippet,
      host,
      score: 0,
      reasons: [`blocked:${blocked}`],
      withinWindow: true, // dateRestrict assumed
      accepted: false,
    };
  }

  // Signals
  const tTitle = tokens(title);
  const tSnippet = tokens(snippet);
  const tAuthor = tokens(authorName);
  const tBook = tokens(bookTitle);

  const authorInTitle = containsAll(tTitle, tAuthor) || jaccard(tTitle, tAuthor) >= 0.4;
  const authorInSnippet = containsAll(tSnippet, tAuthor) || jaccard(tSnippet, tAuthor) >= 0.35;

  const bookInTitleOrSnippet =
    (tBook.length > 0 && (containsAll(tTitle, tBook) || containsAll(tSnippet, tBook))) ||
    jaccard([...tTitle, ...tSnippet], tBook) >= 0.25;

  const conversationHint =
    anyPhrasePresent(title, CONVO_HINTS) || anyPhrasePresent(snippet, CONVO_HINTS);

  const officialDomain = looksOfficialDomain(host, authorName, knownHosts);
  const bookishContext = isBookishPath(url);

  if (authorInTitle) {
    score += W_TITLE_AUTHOR;
    reasons.push("author_in_title");
  }
  if (authorInSnippet) {
    score += W_SNIPPET_AUTHOR;
    reasons.push("author_in_snippet");
  }
  if (bookInTitleOrSnippet) {
    score += W_TITLE_OR_SNIPPET_BOOK;
    reasons.push("book_in_title_or_snippet");
  }
  if (conversationHint) {
    score += W_CONVERSATION_HINT;
    reasons.push("conversation_hint");
  }
  if (officialDomain) {
    score += W_OFFICIAL_DOMAIN;
    reasons.push("official_domain");
  }
  if (bookishContext) {
    score += W_BOOKISH_PATH;
    reasons.push("bookish_path");
  }

  // Acceptance logic
  const meetsAuthor = policy.strictAuthorMatch ? authorInTitle || authorInSnippet : authorInTitle || authorInSnippet;
  let meetsBook = true;
  if (policy.requireBookMatch && tBook.length) {
    // Allow inferred book context if path looks bookish
    meetsBook = bookInTitleOrSnippet || bookishContext;
  }

  // Fallback: if book is required but missing, allow when author is strong & official
  if (policy.requireBookMatch && !meetsBook && policy.fallbackAuthorOnly) {
    const authorStrong = (authorInTitle ? W_TITLE_AUTHOR : 0) + (authorInSnippet ? W_SNIPPET_AUTHOR : 0) >= 0.35;
    if (authorStrong && officialDomain) {
      meetsBook = true;
      reasons.push("fallback_author_only");
    }
  }

  const accepted =
    meetsAuthor && meetsBook && score >= (policy.minSearchConfidence || DEFAULT_MIN_SEARCH_CONFIDENCE);

  return {
    url,
    title,
    snippet,
    host,
    score: Math.max(0, Math.min(1, score)),
    reasons: reasons.length ? reasons : ["no_signal"],
    withinWindow: true, // enforced via dateRestrict
    accepted,
  };
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

    // Parse body
    const raw = req.body ?? {};
    const body = typeof raw === "string" ? JSON.parse(raw || "{}") : raw;

    const authorName = String(body.author_name || "").trim();
    if (!authorName) return res.status(400).json({ error: "author_name required" });

    const bookTitle = String(body.book_title || "").trim();

    let lookback = Number(body.lookback_days ?? LOOKBACK_DEFAULT);
    lookback = clamp(isFinite(lookback) ? lookback : LOOKBACK_DEFAULT, LOOKBACK_MIN, LOOKBACK_MAX);
    const dateRestrict = `d${lookback}`;

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

    // Known hosts from known_urls (optional)
    let knownUrls: string[] = Array.isArray(body.known_urls) ? body.known_urls : [];
    knownUrls = knownUrls
      .map((u: unknown) => String(u).trim())
      .filter((u) => {
        try {
          const parsed = new URL(u);
          return parsed.protocol === "http:" || parsed.protocol === "https:";
        } catch {
          return false;
        }
      })
      .slice(0, 8);
    const knownHosts = Array.from(
      new Set(knownUrls.map((u) => hostOf(u)).filter(Boolean)),
    );

    // Cache key includes inputs + policy knobs
    const cacheKey = makeCacheKey({
      authorName,
      bookTitle,
      lookback,
      includeSearch,
      strictAuthorMatch,
      requireBookMatch,
      fallbackAuthorOnly,
      minConfidence,
      minSearchConfidence,
      knownHosts,
    });
    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    // If search disabled or missing CSE creds, return no result
    if (!includeSearch || !CSE_KEY || !CSE_CX) {
      const payload: CacheValue = { author_name: authorName, has_recent: false };
      if (debug) payload._debug = [];
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Query build: author + (optional) book + conversation hints + domain exclusions
    const baseQuoted = `"${authorName}"`;
    const bookQuoted = bookTitle ? ` "${bookTitle}"` : "";
    const convoOr = '(interview OR "in conversation" OR "Q&A" OR conversation)';
    const exclusions =
      "-site:x.com -site:twitter.com -site:facebook.com -site:instagram.com -site:tiktok.com -site:linkedin.com -site:youtube.com -site:amazon.com -site:open.spotify.com -site:apple.com -site:podcasts.apple.com -site:substack.com";

    const query = `${baseQuoted}${bookQuoted} ${convoOr} ${exclusions}`.trim();

    // Fetch up to 30 results
    const items = await googleCseSearch(query, dateRestrict);

    const examined: DebugRow[] = [];
    const scored: Scored[] = [];

    for (const it of items) {
      const title = String(it.title || "");
      const link = String(it.link || "");
      const snippet = String(it.snippet || "");
      const host = hostOf(link);

      if (!title || !link) {
        examined.push({
          feedUrl: link || "(missing)",
          ok: false,
          source: "web",
          latest: null,
          recentWithinWindow: true,
          confidence: 0,
          reason: ["missing_title_or_link"],
          error: null,
        });
        continue;
      }

      const s = scoreItem(it, authorName, bookTitle, knownHosts, {
        requireBookMatch,
        strictAuthorMatch,
        minSearchConfidence,
        fallbackAuthorOnly,
      });

      examined.push({
        feedUrl: link,
        ok: s.accepted,
        source: "web",
        latest: null, // no date in CSE general
        recentWithinWindow: true,
        confidence: Number(s.score.toFixed(2)),
        reason: s.reasons,
        error: s.accepted ? null : null,
      });

      if (s.accepted) scored.push(s);
    }

    // Choose best by confidence, then by a mild author-title preference
    scored.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const aAuthorTitle = a.reasons.includes("author_in_title") ? 1 : 0;
      const bAuthorTitle = b.reasons.includes("author_in_title") ? 1 : 0;
      return bAuthorTitle - aAuthorTitle;
    });

    if (scored.length) {
      const top = scored[0];
      const payload: CacheValue = {
        author_name: authorName,
        has_recent: true,
        latest_title: top.title,
        latest_url: top.url,
        source: "web",
        author_url: `https://${top.host}`,
      };
      if (debug) payload._debug = examined.slice(0, 30);
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Nothing acceptable → strict reject
    const empty: CacheValue = { author_name: authorName, has_recent: false };
    if (debug) empty._debug = examined.slice(0, 30);
    setCached(cacheKey, empty);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(STRICT_REJECT_PAYLOAD);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
