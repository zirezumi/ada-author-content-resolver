// api/author-updates.ts
/// <reference types="node" />

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
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const FETCH_TIMEOUT_MS = 5500;
const USER_AGENT = "AuthorWebsite/1.0 (+https://example.com)";

const CONCURRENCY = 4;

/* ====== Google CSE config ====== */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_ID = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "";
const USE_SEARCH_BASE = !!(CSE_KEY && CSE_ID);

/* ====== Scoring defaults ====== */
const DEFAULT_MIN_SITE_CONFIDENCE = 0.55;

/* ====== Domain filters (for website finding) ======
   We block obvious non-home sites but ALLOW platforms many authors use
   (Substack/Medium/WordPress), scoring them slightly lower than custom domains. */
const WEBSITE_BLOCKLIST = [
  // social / aggregators
  "x.com",
  "twitter.com",
  "facebook.com",
  "instagram.com",
  "tiktok.com",
  "linkedin.com",
  "goodreads.com",
  "imdb.com",
  "wikipedia.org",
  "wikidata.org",
  // shopping / retailers
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
  // newsrooms and general magazines (typically not "the author's site")
  "nytimes.com",
  "theguardian.com",
  "wsj.com",
  "washingtonpost.com",
  "newyorker.com",
];

type CacheValue = {
  author_name: string;
  author_url?: string;        // The best guess at the author's official site (origin or canonical)
  site_title?: string | null;
  canonical_url?: string | null;
  confidence: number;         // 0..1 confidence score
  source: "web";
  _diag?: any;                // optional debugging info
};

type CacheEntry = { expiresAt: number; value: CacheValue };

type WebHit = {
  title: string;
  url: string;
  host: string;
  snippet?: string;
  confidence: number;
  reasons: string[];
};

type Context = {
  authorName: string;
  authorTokens: string[];
  bookTokens: string[];
  unsafeDisableDomainFilters: boolean;
};

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

function jaccard(a: string[], b: string[]) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}

function isHttpUrl(s: string) {
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function hostOf(u: string): string {
  try {
    return new URL(u).host.toLowerCase();
  } catch {
    return "";
  }
}

function originOf(u: string): string | null {
  try {
    return new URL(u).origin;
  } catch {
    return null;
  }
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

function isBlockedHost(host: string, disableFilters: boolean): boolean {
  if (disableFilters) return false;
  const h = host.toLowerCase();
  return WEBSITE_BLOCKLIST.some((bad) => h.includes(bad));
}

function normalizeAuthor(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
}

/* =============================
   HTML signals
   ============================= */
async function fetchHtmlSignals(url: string, ctx: Context): Promise<{
  titleTokens: string[];
  hasCopyrightName: boolean;
  hasSchemaPerson: boolean;
  hasOfficialWords: boolean;
  bookMentioned: boolean;
}> {
  try {
    const res = await fetchWithTimeout(url);
    if (!res?.ok) {
      return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false };
    }
    const html = await res.text();
    const head = html.slice(0, 100_000); // examine only head/early body

    const title = /<title[^>]*>([^<]*)<\/title>/i.exec(head)?.[1] ?? "";
    const tt = tokens(title);

    const lower = head.toLowerCase();

    const copyrightName =
      /\u00A9|&copy;|copyright/i.test(head) &&
      ctx.authorTokens.length > 0 &&
      (containsAll(tokens(head), ctx.authorTokens) || head.toLowerCase().includes(normalizeAuthor(ctx.authorName).toLowerCase()));

    const schemaPerson = /"@type"\s*:\s*"(?:Person|Author)"/i.test(head);

    const officialWords = /\bofficial (site|website)\b/i.test(title) || /\bofficial (site|website)\b/i.test(head);

    const bookMentioned =
      ctx.bookTokens.length > 0 &&
      (containsAll(tokens(head), ctx.bookTokens) || jaccard(tokens(head), ctx.bookTokens) >= 0.25);

    return {
      titleTokens: tt,
      hasCopyrightName: !!copyrightName,
      hasSchemaPerson: !!schemaPerson,
      hasOfficialWords: !!officialWords,
      bookMentioned: !!bookMentioned,
    };
  } catch {
    return { titleTokens: [], hasCopyrightName: false, hasSchemaPerson: false, hasOfficialWords: false, bookMentioned: false };
  }
}

/* =============================
   Google CSE
   ============================= */
async function googleCSE(query: string): Promise<any[]> {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", "10");

  const resp = await fetchWithTimeout(url.toString());
  if (!resp?.ok) {
    const text = await resp?.text?.();
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  return Array.isArray(data?.items) ? data.items : [];
}

/* =============================
   Website search & scoring
   ============================= */
function baseHit(it: any): WebHit {
  const url = String(it.link || "");
  return {
    title: String(it.title || ""),
    url,
    host: hostOf(url),
    snippet: String(it.snippet || it.htmlSnippet || ""),
    confidence: 0,
    reasons: [],
  };
}

function scoreBaseSignals(hit: WebHit, ctx: Context): WebHit {
  let score = 0;
  const reasons: string[] = [];

  // Title/snippet contain full author name
  const t = tokens(hit.title + " " + (hit.snippet || ""));
  if (containsAll(t, ctx.authorTokens) || jaccard(t, ctx.authorTokens) >= 0.35) {
    score += 0.35;
    reasons.push("author_in_title_or_snippet");
  }

  // If book tokens exist and appear, small boost (helps disambiguate same-name authors)
  if (ctx.bookTokens.length) {
    if (containsAll(t, ctx.bookTokens) || jaccard(t, ctx.bookTokens) >= 0.25) {
      score += 0.15;
      reasons.push("book_in_title_or_snippet");
    }
  }

  // Block obvious non-home hosts
  if (isBlockedHost(hit.host, false)) {
    score -= 1.0;
    reasons.push("blocked_domain");
  }

  // Prefer likely author-owned domains (vanity domains—name matches host sans TLD)
  const compactName = normalizeAuthor(ctx.authorName).toLowerCase().replace(/\s+/g, "");
  const hostBare = hit.host.replace(/^www\./, "");
  if (hostBare.startsWith(compactName) || hostBare.includes(compactName)) {
    score += 0.25;
    reasons.push("vanity_domain_match");
  }

  // Slight preference for custom domains over platforms
  if (
    !/substack\.com|medium\.com|wordpress\.com|blogspot\.|ghost\.io/i.test(hit.host)
  ) {
    score += 0.05;
    reasons.push("custom_domain_preferred");
  } else {
    reasons.push("platform_host");
  }

  hit.confidence = Math.max(0, Math.min(1, score));
  hit.reasons = reasons;
  return hit;
}

async function enrichWithHtml(hit: WebHit, ctx: Context): Promise<WebHit> {
  const origin = originOf(hit.url);
  if (!origin) return hit;

  const [home, about] = await Promise.all([
    fetchHtmlSignals(origin, ctx),
    fetchHtmlSignals(origin + "/about", ctx),
  ]);

  const signals = [
    home,
    about, // reading /about often yields clearer "official" hints
  ];

  for (const s of signals) {
    if (!s) continue;
    if (containsAll(s.titleTokens, ctx.authorTokens) || jaccard(s.titleTokens, ctx.authorTokens) >= 0.4) {
      hit.confidence = Math.min(1, hit.confidence + 0.15);
      hit.reasons.push("title_matches_author");
    }
    if (s.hasOfficialWords) {
      hit.confidence = Math.min(1, hit.confidence + 0.20);
      hit.reasons.push("official_wording");
    }
    if (s.hasCopyrightName) {
      hit.confidence = Math.min(1, hit.confidence + 0.15);
      hit.reasons.push("copyright_name_match");
    }
    if (s.hasSchemaPerson) {
      hit.confidence = Math.min(1, hit.confidence + 0.10);
      hit.reasons.push("schema_person_present");
    }
    if (s.bookMentioned) {
      hit.confidence = Math.min(1, hit.confidence + 0.10);
      hit.reasons.push("book_mentioned_on_site");
    }
  }

  return hit;
}

async function searchAuthorWebsite(authorName: string, bookTitle: string, ctx: Context, debug: boolean) {
  const quotedAuthor = `"${authorName}"`;
  const bookPart = bookTitle ? ` "${bookTitle}"` : "";

  // Try multiple phrasings; union results (de-duped by URL)
  const queries = [
    `${quotedAuthor}${bookPart} official site`,
    `${quotedAuthor}${bookPart} official website`,
    `${quotedAuthor}${bookPart} author website`,
    `${quotedAuthor} site`,
    `${quotedAuthor} website`,
  ];

  const diag: any = { queries, rawCounts: [] as number[], picked: undefined };

  const limit = pLimit(CONCURRENCY);
  const allItems: any[] = (
    await Promise.all(
      queries.map((q) =>
        limit(async () => {
          const items = USE_SEARCH_BASE ? await googleCSE(q) : [];
          diag.rawCounts.push(items.length);
          return items;
        })
      )
    )
  ).flat();

  // Deduplicate by URL
  const seen = new Set<string>();
  const prelim: WebHit[] = [];
  for (const it of allItems) {
    const hit = baseHit(it);
    if (!isHttpUrl(hit.url)) continue;
    if (seen.has(hit.url)) continue;
    seen.add(hit.url);
    prelim.push(scoreBaseSignals(hit, ctx));
  }

  // Remove blocked after scoring
  const filtered = prelim.filter((h) =>
    ctx.unsafeDisableDomainFilters ? true : !isBlockedHost(h.host, false)
  );

  // Enrich with HTML signals (home + /about)
  const enriched = await Promise.all(filtered.map((h) => limit(() => enrichWithHtml(h, ctx))));
  enriched.sort((a, b) => b.confidence - a.confidence);

  if (debug) diag.candidates = enriched.slice(0, 10).map((h) => ({
    url: h.url,
    host: h.host,
    confidence: Number(h.confidence.toFixed(2)),
    reasons: h.reasons,
  }));

  const best = enriched[0] || null;
  if (debug && best) diag.picked = { url: best.url, confidence: Number(best.confidence.toFixed(2)), reasons: best.reasons };

  return { best, _diag: debug ? diag : undefined };
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
    const debug: boolean = body.debug === true;

    const minSiteConfidence: number =
      typeof body.min_site_confidence === "number"
        ? Math.max(0, Math.min(1, body.min_site_confidence))
        : DEFAULT_MIN_SITE_CONFIDENCE;

    const unsafeDisableDomainFilters: boolean = body.unsafe_disable_domain_filters === true;

    // search enablement
    const includeSearchRequested: boolean = body.include_search !== false; // default true
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
      unsafeDisableDomainFilters,
    };

    const cacheKey = makeCacheKey({
      author, bookTitle, minSiteConfidence, USE_SEARCH, unsafeDisableDomainFilters,
    });

    const cached = getCached(cacheKey);
    if (cached && !debug) {
      res.setHeader("x-cache", "HIT");
      return res.status(200).json(cached);
    }

    if (!USE_SEARCH) {
      const fail: CacheValue = {
        author_name: author,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        source: "web",
        _diag: debug ? { reason: "search_disabled_or_not_configured" } : undefined,
      };
      setCached(cacheKey, fail);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(fail);
    }

    const { best, _diag } = await searchAuthorWebsite(author, bookTitle, ctx, debug);

    if (!best || best.confidence < minSiteConfidence) {
      const payload: CacheValue = {
        author_name: author,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: best ? Number(best.confidence.toFixed(2)) : 0,
        source: "web",
        _diag,
      };
      setCached(cacheKey, payload);
      res.setHeader("x-cache", "MISS");
      return res.status(200).json(payload);
    }

    // Normalize to origin, fetch canonical & title for nicety
    const origin = originOf(best.url)!;

    let siteTitle: string | null = null;
    let canonical: string | null = null;
    try {
      const resp = await fetchWithTimeout(origin);
      if (resp?.ok) {
        const html = await resp.text();
        siteTitle = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1]?.trim() || null;
        const mCanon = html.match(/<link[^>]+rel=["']canonical["'][^>]*>/i)?.[0] || "";
        canonical =
          /href=["']([^"']+)["']/i.exec(mCanon)?.[1] ||
          null;
        if (canonical && !isHttpUrl(canonical)) {
          canonical = new URL(canonical, origin).toString();
        }
      }
    } catch {
      /* ignore */
    }

    const payload: CacheValue = {
      author_name: author,
      author_url: origin,
      site_title: siteTitle,
      canonical_url: canonical || origin,
      confidence: Number(best.confidence.toFixed(2)),
      source: "web",
      _diag,
    };

    setCached(cacheKey, payload);
    res.setHeader("x-cache", "MISS");
    return res.status(200).json(payload);
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
