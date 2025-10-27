// api/book-author-website.ts
/// <reference types="node" />

/**
 * Author Website Resolver
 * - Keeps original logic and thresholds
 * - Adds domain priors + early-accept for personal sites (with confirmations)
 * - Capped probes with short timeouts + retry-on-abort
 * - Deterministic tie-breaks
 * - Versioned LRU caches for Google CSE queries, title expansion, and site validations
 * - Borderline telemetry in _diag for observability
 * - Hardened name-boundary detection and tail cleanup (prevents “Malcolm Gladwell Due”)
 */

/* =============================
   Vercel runtime
   ============================= */
export const config = { runtime: "nodejs" } as const;

/* =============================
   Build/logic versioning (for caches)
   ============================= */
const RESOLVER_VERSION = process.env.RESOLVER_VERSION ?? "2025-10-26-b";

/* =============================
   Flags / Tunables (ENV)
   ============================= */
const SKIP_AUTH = (process.env.SKIP_AUTH || "").toLowerCase() === "true";

// Website acceptance threshold
const WEBSITE_MIN_SCORE = Number(process.env.WEBSITE_MIN_SCORE ?? 0.6);

// Feature flags
const FLAG_EARLY_ACCEPT_PERSONAL_DOMAIN =
  (process.env.EARLY_ACCEPT_PERSONAL_DOMAIN ?? "true").toLowerCase() === "true";
const FLAG_CAP_PROBES =
  (process.env.CAP_PROBES ?? "true").toLowerCase() === "true";
const FLAG_TOKEN_NAME_SCORE =
  (process.env.TOKEN_NAME_SCORE ?? "true").toLowerCase() === "true";

// Probe paths / timeouts
const PROBE_PATHS_BASE = ["/", "/about"] as const;
const PROBE_PATHS_FALLBACK = ["/books"] as const;
const FETCH_TIMEOUT_GENERAL_MS = Number(process.env.FETCH_TIMEOUT_GENERAL_MS ?? 5500);
const FETCH_TIMEOUT_SAMPLE_MS = Number(process.env.FETCH_TIMEOUT_SAMPLE_MS ?? 1400);
const FETCH_RETRY_ON_ABORT = Number(process.env.FETCH_RETRY_ON_ABORT ?? 1);

// Caching
const CACHE_MAX = Number(process.env.CACHE_MAX ?? 256);
const CACHE_TTL_POS_MS = Number(process.env.CACHE_TTL_POS_MS ?? 1000 * 60 * 60 * 12); // 12h
const CACHE_TTL_NEG_MS = Number(process.env.CACHE_TTL_NEG_MS ?? 1000 * 60 * 60 * 2);  // 2h

// Borderline bands for extra telemetry
const AUTHOR_SCORE_BAND = Number(process.env.AUTHOR_SCORE_BAND ?? 0.03);
const SITE_SCORE_BAND = Number(process.env.SITE_SCORE_BAND ?? 0.05);

// Name acceptance hysteresis (keeps legacy cutoff while logging near misses)
const NAME_ACCEPT_CUTOFF = 0.65;
const NAME_HYSTERESIS = 0.02;

// Domain priors: personal > platform (applied only to ties/near-ties)
const DOMAIN_PRIOR_DELTA = Number(process.env.DOMAIN_PRIOR_DELTA ?? 0.06);

// Domain classification / platform list
const PLATFORM_HOSTS = [
  "substack.com","medium.com","wordpress.com","blogspot.","tumblr.com",
  "linktr.ee","beacons.ai","instagram.com","facebook.com","x.com","twitter.com",
  "youtube.com","tiktok.com","soundcloud.com"
];

/* =============================
   Auth
   ============================= */
const AUTH_SECRETS: string[] = (process.env.AUTHOR_UPDATES_SECRET || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function headerCI(req: any, name: string): string | undefined {
  if (!req?.headers) return undefined;
  const ent = Object.entries(req.headers).find(([k]) => k.toLowerCase() === name.toLowerCase());
  if (!ent) return undefined;
  const val = ent[1] as any;
  if (Array.isArray(val)) return val[0] as string;
  return typeof val === "string" ? val : undefined;
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
   Google CSE
   ============================= */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_ID = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "";
const USE_SEARCH = !!(CSE_KEY && CSE_ID);

/* =============================
   LRU cache (module-scoped, best-effort in serverless warm starts)
   ============================= */
type CacheVal<T> = { value: T; expires: number; neg?: boolean };
class LRU<K, V> {
  private map = new Map<K, CacheVal<V>>();
  constructor(private max = 256) {}
  get(key: K): CacheVal<V> | undefined {
    const v = this.map.get(key);
    if (!v) return undefined;
    this.map.delete(key);
    this.map.set(key, v);
    return v;
  }
  set(key: K, val: CacheVal<V>): void {
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, val);
    if (this.map.size > this.max) {
      const it = this.map.keys().next();
      if (!it.done) this.map.delete(it.value as K);
    }
  }
}

const lruCSE = new LRU<string, any[]>(CACHE_MAX);
const lruExpandTitle = new LRU<string, { full: string; usedShort: boolean; debug: any }>(CACHE_MAX);
const lruFetchText = new LRU<string, string>(CACHE_MAX);
const lruSiteValidation = new LRU<string, { origin: string; urlScore: number; contentScore: number; finalScore: number; samples: Array<{ path: string; contentScore: number }>; fromQuery: string }[]>(CACHE_MAX);

/* =============================
   Fetch with timeout + retry (on AbortError)
   ============================= */
async function fetchWithTimeout(url: string, init?: RequestInit, ms = FETCH_TIMEOUT_GENERAL_MS) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { "User-Agent": "BookAuthorWebsite/1.1", ...(init?.headers || {}) },
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function fetchWithRetryAbort(url: string, init: RequestInit | undefined, ms: number, retries: number) {
  let attempt = 0;
  while (true) {
    try {
      return await fetchWithTimeout(url, init, ms);
    } catch (err: any) {
      const isAbort = (err?.name === "AbortError") || /aborted/i.test(String(err?.message || ""));
      if (isAbort && attempt < retries) { attempt++; continue; }
      throw err;
    }
  }
}

function cacheKey(parts: Array<string | number | boolean | undefined | null>): string {
  return parts.map(p => String(p ?? "")).join("::");
}

async function googleCSE(query: string, num = 10): Promise<any[]> {
  if (!USE_SEARCH) return [];
  const key = cacheKey(["cse", RESOLVER_VERSION, query, num]);
  const cached = lruCSE.get(key);
  const now = Date.now();
  if (cached && cached.expires > now) return cached.value;

  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(num));

  const resp = await fetchWithRetryAbort(url.toString(), undefined, FETCH_TIMEOUT_GENERAL_MS, FETCH_RETRY_ON_ABORT);
  if (!resp?.ok) {
    const text = await resp?.text?.();
    lruCSE.set(key, { value: [], expires: now + CACHE_TTL_NEG_MS, neg: true });
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  const items = Array.isArray(data?.items) ? data.items : [];
  lruCSE.set(key, { value: items, expires: now + CACHE_TTL_POS_MS });
  return items;
}

/* =============================
   Utils (precompiled regex & shared tokenization)
   ============================= */
const RE_NON_ALNUM_SPACE_APOS_HYPHEN = /[^\p{L}\p{N}\s'-]/gu;
const RE_NON_ALNUM_SPACE_APOS_COLON_HYPHEN = /[^\p{L}\p{N}\s':-]/gu;
const RE_WS = /\s+/g;
const RE_QUOTE_SMART = /[“”]/g;
const RE_BLOCK_META = /<meta\b[^>]*?(?:name|property)=["'](?:og:title|og:description|description|twitter:title|twitter:description)["'][^>]*?>/gi;
const RE_META_CONTENT = /\bcontent=["']([^"']+)["']/i;
const RE_TITLE_TAG = /<title[^>]*>([\s\S]*?)<\/title>/i;
const RE_JSONLD_BLOCK = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
const RE_SCRIPT = /<script[\s\S]*?<\/script>/gi;
const RE_STYLE = /<style[\s\S]*?<\/style>/gi;
const RE_HTML_COMMENT = /<!--[\s\S]*?-->/g;
const RE_TAG = /<[^>]+>/g;

function tokens(s: string): string[] {
  return s
    .toLowerCase()
    .normalize("NFKC")
    .replace(RE_NON_ALNUM_SPACE_APOS_HYPHEN, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function hostOf(link: string): string {
  try { return new URL(link).host.toLowerCase(); } catch { return ""; }
}

function apexOfHost(host: string): string {
  const parts = host.split(".");
  if (parts.length >= 3 && (host.endsWith(".co.uk") || host.endsWith(".com.au") || host.endsWith(".co.jp"))) {
    return parts.slice(-3).join(".");
  }
  return parts.slice(-2).join(".");
}

function cleanTitle(s: string): string {
  return (s || "")
    .replace(RE_WS, " ")
    .replace(RE_QUOTE_SMART, '"')
    .trim();
}

function tokenSet(s: string): Set<string> {
  return new Set(
    s.toLowerCase()
     .normalize("NFKC")
     .replace(RE_NON_ALNUM_SPACE_APOS_COLON_HYPHEN, " ")
     .split(/\s+/).filter(Boolean)
  );
}

function normalizeCompareTitle(s: string): string {
  return s.toLowerCase().replace(/[\s“”"'-]+/g, " ").trim();
}

/* Helpers for subtitle tail hygiene (unchanged logic) */
function stripSiteSuffix(s: string): string {
  return (s || "")
    .replace(
      /\s*[-–—•|]\s*(?:Wikipedia|Amazon|Goodreads|Barnes\s*&\s*Noble|Bookshop(?:\.org)?|Penguin\s*Random\s*House|Simon\s*&\s*Schuster|HarperCollins|Hachette(?:\s*Book\s*Group)?|Macmillan|Official\s*Site|Home|Books?).*$/i,
      ""
    )
    .trim();
}
function cleanFirstSubtitleSegment(s: string): string {
  const cut = s.split(/[|•–—-]{1,2}|\b\|\b/)[0];
  return (cut || "").replace(RE_WS, " ").trim();
}
function looksLikeISBNy(s: string): boolean {
  const d = (s.match(/\d/g) || []).length;
  return d >= 6 || /\b(?:ISBN|978|979)\b/i.test(s);
}
function looksLikeCategoryTail(s: string): boolean {
  return /\b(?:Books?|Book|Home|Official Site|Edit(?:ion)?|Paperback|Hardcover|Audiobook)\b/i.test(s);
}
function looksLikeAuthorListTail(s: string): boolean {
  return /(?:,|\band\b|\bwith\b)/i.test(s) && /\b[A-Z][\p{L}'-]+/u.test(s);
}

/* =============================
   EXTRA: banned generic subtitles
   ============================= */
const BANNED_SUBTITLES = new Set([
  "a novel","novel","a memoir","memoir","a biography","biography","an autobiography","autobiography"
]);
function isBannedSubtitle(s: string): boolean {
  return BANNED_SUBTITLES.has((s || "").trim().toLowerCase());
}

/* =============================
   Name-likeness scoring (unchanged thresholds, small perf tweaks)
   ============================= */
const MIDDLE_PARTICLES = new Set([
  "de","del","de la","di","da","dos","das","do","van","von","bin","ibn","al","el","le","la","du","st","saint","mc","mac","ap"
]);

const GENERATIONAL_SUFFIX = new Set(["jr","jr.","sr","sr.","ii","iii","iv","v"]);
const BAD_TAIL = new Set([
  "frankly","review","reviews","opinion","analysis","explainer","interview","podcast",
  "video","transcript","essay","profile","biography","news","guide","column","commentary",
  "editorial","update","thread","blog","price"
]);

function isCasedWordToken(tok: string): boolean {
  return /^[A-Z][\p{L}’'-]*[A-Za-z]$/u.test(tok);
}
function isParticle(tok: string): boolean { return MIDDLE_PARTICLES.has(tok.toLowerCase()); }
function isSuffix(tok: string): boolean { return GENERATIONAL_SUFFIX.has(tok.toLowerCase()); }
function looksLikeAdverb(tok: string): boolean { return /^[A-Za-z]{3,}ly$/u.test(tok); }

/** NEW: Keepable & droppable suffix logic */
const KEEPABLE_SUFFIX = new Set([
  "jr","jr.","sr","sr.","ii","iii","iv","v","phd","ph.d.","md","m.d.","obe","cbe"
]);

const DROP_IF_TRAILING = new Set([
  "due","because","however","meanwhile","thus","therefore","but","review","interview","analysis","explained","explainer","says","writes"
]);

function endsWithKeepableSuffix(name: string): boolean {
  const toks = name.trim().split(/\s+/);
  const last = (toks[toks.length - 1] || "").toLowerCase();
  return KEEPABLE_SUFFIX.has(last);
}

function stripDiscourseTail(name: string): string {
  let toks = name.trim().split(/\s+/);
  while (toks.length > 1) {
    const last = toks[toks.length - 1];
    const low = last.toLowerCase();
    if (KEEPABLE_SUFFIX.has(low)) break;
    if (DROP_IF_TRAILING.has(low)) { toks.pop(); continue; }
    if (/^\d{1,4}$/.test(last)) { toks.pop(); continue; } // drop year/artifact
    if (/^[a-z]+$/.test(last)) { toks.pop(); continue; }  // trailing lowercase token
    break;
  }
  return toks.join(" ");
}

function nameLikeness(raw: string): number {
  const s = raw.trim().replace(/\s+/g, " ");
  if (!s) return 0;

  const partsOrig = s.split(" ");
  const parts: string[] = [];
  for (let i = 0; i < partsOrig.length; i++) {
    const cur = partsOrig[i];
    const next = partsOrig[i + 1]?.toLowerCase();
    if (i + 1 < partsOrig.length && (cur.toLowerCase() === "de" && next === "la")) { parts.push("de la"); i++; continue; }
    parts.push(cur);
  }

  if (parts.length < 2) return 0;
  if (parts.length > 5) return 0.25;

  let score = 1.0;

  if (!isCasedWordToken(parts[0])) score -= 0.6;

  const last = parts[parts.length - 1];
  const lastIsSuffix = isSuffix(last);
  const lastName = lastIsSuffix ? parts[parts.length - 2] : last;
  if (!isCasedWordToken(lastName)) score -= 0.6;

  for (let i = 1; i < parts.length - (lastIsSuffix ? 2 : 1); i++) {
    const p = parts[i];
    if (!isCasedWordToken(p)) score += isParticle(p) ? -0.05 : -0.35;
  }

  if (BAD_TAIL.has(last.toLowerCase())) score -= 0.7;
  if (!lastIsSuffix && looksLikeAdverb(last)) score -= 0.6;
  if (DROP_IF_TRAILING.has(last.toLowerCase())) score -= 0.6; // NEW: penalize discourse tails
  if (/\d/.test(s)) score -= 0.8;
  if (/[;:!?]$/.test(s)) score -= 0.2;

  if (parts.length === 2) score += 0.10;
  if (parts.length === 3) score += 0.12;
  if (parts.length === 4) score += 0.05;

  return Math.max(0, Math.min(1, score));
}

/** Obvious non-name words that often appear capitalized in subtitles/titles */
const COMMON_TITLE_WORDS = new Set([
  "Change","Transitions","Transition","Moving","Forward","Next","Day","The",
  "Future","Guide","How","Policy","Center","Office","Press","News","Support","Poet"
]);

function looksLikeCommonPair(name: string): boolean {
  const toks = name.split(/\s+/);
  if (toks.length !== 2) return false;
  return toks.every(t => COMMON_TITLE_WORDS.has(t));
}

function maybeOrgish(name: string): boolean {
  const ABSTRACT_TOKENS = new Set([
    "Change","Transitions","News","Press","Policy","Support","Guide","Team",
    "Center","Office","School","Library","Community","Media","Communications","Poet"
  ]);
  const toks = name.split(/\s+/);
  return toks.length === 2 && toks.every(t => ABSTRACT_TOKENS.has(t));
}

/* =============================
   Explicit org/imprint detection + tail stripping
   ============================= */
const NAME_TAIL_GARBAGE = new Set([
  "book","books","press","publisher","publishers","publishing","imprint",
  "media","group","house","co","co.","company","inc","inc.","llc","ltd","ltd.","gmbh",
  "records","studios","partners","associates"
]);

const ROLE_TAIL_TOKENS = new Set([
  "illustrated","illustrator","illustrations",
  "edited","editor","editors",
  "foreword","afterword","introduction","intro",
  "translated","translator","translators"
]);

function stripTrailingGarbage(s: string): string {
  let parts = s.split(/\s+/);
  let changed = false;
  while (parts.length > 1) {
    const last = parts[parts.length - 1];
    const low = last.toLowerCase();
    if (
      NAME_TAIL_GARBAGE.has(low) ||
      BAD_TAIL.has(low) ||
      looksLikeAdverb(last) ||
      ROLE_TAIL_TOKENS.has(low) ||
      DROP_IF_TRAILING.has(low) ||      // NEW: discourse tails
      /^[a-z]+$/.test(last) ||          // NEW: trailing lowercase token
      /^\d{1,4}$/.test(last)            // NEW: trailing year/number artifact
    ) {
      parts.pop();
      changed = true;
      continue;
    }
    break;
  }
  return changed ? parts.join(" ") : s;
}

function cleanRoleArtifacts(name: string): string {
  let s = name.replace(/\b(Illustrated|Illustrator|Edited|Editor|Foreword|Afterword|Introduction|Intro|Translated|Translator)s?\b$/i, "").trim();
  s = s.replace(/[,\s]+$/g, "").trim();
  return s;
}

const ORG_TOKENS = new Set([
  "books","press","publisher","publishers","publishing","imprint",
  "group","house","media","studios","pictures","records",
  "llc","inc","ltd","gmbh","company","co.","university","dept","department"
]);

function isOrgishName(name: string): boolean {
  const toks = name.toLowerCase().split(/\s+/);
  if (toks.some(t => ORG_TOKENS.has(t))) return true;
  if (/[&]/.test(name)) return true; // “Simon & Schuster”
  if (/\b(?:and|partners|associates)\b/i.test(name)) return true;
  return false;
}

/* =============================
   Clean + validate a candidate (HARDENED)
   ============================= */
function normalizeNameCandidate(raw: string, title: string): string | null {
  let s = raw.trim()
    .replace(/[)\]]+$/g, "")
    .replace(/[.,;:–—-]+$/g, "")
    .replace(RE_WS, " ");

  s = s.split(/\s+/).map(tok => tok.replace(/[.,;:]+$/g, "")).join(" ");

  s = cleanRoleArtifacts(s);
  s = stripDiscourseTail(s);   // NEW: drop “Due”, “Because”, etc.
  s = stripTrailingGarbage(s); // existing + NEW tails

  if (isOrgishName(s)) return null;

  const partsEarly = s.split(/\s+/);
  if (partsEarly.length === 2 && looksLikeCommonPair(s)) return null;

  const titleToks = tokens(title);
  if (titleToks.includes(s.toLowerCase())) return null;

  if (partsEarly.length === 2) {
    const overlap = partsEarly.filter(p => titleToks.includes(p.toLowerCase())).length;
    if (overlap === 2) return null;
  }

  // One more pass in case tail rules exposed new garbage
  s = stripDiscourseTail(s);
  s = stripTrailingGarbage(s);
  if (isOrgishName(s)) return null;

  const likeness = nameLikeness(s);
  return likeness >= NAME_ACCEPT_CUTOFF ? s : null;
}

/* =============================
   Publisher/retailer domain hints
   ============================= */
const PUBLISHERS = [
  "us.macmillan.com","macmillan.com","penguinrandomhouse.com","prh.com",
  "harpercollins.com","simonandschuster.com","simonandschuster.net",
  "macmillanlearning.com","hachettebookgroup.com","fsgbooks.com"
];

const PUBLISHER_OR_RETAIL = [
  ...PUBLISHERS,
  "amazon.","goodreads.com","bookshop.org","barnesandnoble.com"
];

function preferPublisherOrRetail(host: string): boolean {
  return PUBLISHER_OR_RETAIL.some(d => host.includes(d));
}

/* =============================
   Phase 0: Title expansion (subtitle recovery) with caching
   ============================= */
function startsWithShortThenSubtitle(title: string, short: string): string | null {
  const esc = short.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`^${esc}\\s*[:—-]\\s*(.+)$`, "i");
  const m = title.match(re);
  if (!m) return null;
  const subtitleRaw = m[1];
  let subtitle = cleanFirstSubtitleSegment(stripSiteSuffix(subtitleRaw));
  if (
    !subtitle ||
    /\bWikipedia\b/i.test(subtitle) ||
    looksLikeISBNy(subtitle) ||
    looksLikeCategoryTail(subtitle) ||
    looksLikeAuthorListTail(subtitle) ||
    isBannedSubtitle(subtitle)
  ) {
    return null;
  }
  return subtitle || null;
}

async function expandBookTitle(shortTitle: string): Promise<{ full: string; usedShort: boolean; debug: any }> {
  const key = cacheKey(["expand", RESOLVER_VERSION, shortTitle]);
  const cached = lruExpandTitle.get(key);
  const now = Date.now();
  if (cached && cached.expires > now) return cached.value;

  const q = `"${shortTitle}" book`;
  const items = await googleCSE(q, 8);

  let bestFull: string | null = null;
  let bestHost: string | null = null;
  const evidence: any[] = [];

  for (const it of items) {
    const host = hostOf(it.link);
    if (host.includes("wikipedia.org")) continue;

    const mtList = Array.isArray(it.pagemap?.metatags) ? it.pagemap.metatags : [];
    const candidates: string[] = [];

    if (typeof it.title === "string") candidates.push(cleanTitle(it.title));
    for (const mt of mtList) {
      const t = (mt?.["og:title"] as string) || (mt?.title as string);
      if (typeof t === "string") candidates.push(cleanTitle(t));
    }

    for (const candRaw of candidates) {
      const cand = stripSiteSuffix(candRaw);
      const subtitle = startsWithShortThenSubtitle(cand, shortTitle);
      if (subtitle) {
        const full = `${shortTitle}: ${subtitle}`;
        if (/\bWikipedia\b/i.test(subtitle)) continue;
        evidence.push({ host, cand, full, reason: "subtitle_from_prefix" });
        if (!bestFull || (preferPublisherOrRetail(host) && !(bestHost && preferPublisherOrRetail(bestHost!)))) {
          bestFull = full;
          bestHost = host;
        }
      } else {
        const shortToks = tokenSet(shortTitle);
        const candToks = tokenSet(cand);
        const contained = [...shortToks].every(t => candToks.has(t));
        if (contained && cand.length >= shortTitle.length + 4) {
          const colonIdx = cand.indexOf(":");
          if (colonIdx > 0) {
            const head = cand.slice(0, colonIdx).trim();
            let tail = cleanFirstSubtitleSegment(stripSiteSuffix(cand.slice(colonIdx + 1))).trim();

            if (!tail ||
                /\bWikipedia\b/i.test(tail) ||
                looksLikeISBNy(tail) ||
                looksLikeCategoryTail(tail) ||
                looksLikeAuthorListTail(tail) ||
                isBannedSubtitle(tail)) {
              continue;
            }

            if (head.toLowerCase().startsWith(shortTitle.toLowerCase()) && tail) {
              const full = `${shortTitle}: ${tail}`;
              evidence.push({ host, cand, full, reason: "subtitle_from_colon" });
              if (!bestFull || (preferPublisherOrRetail(host) && !(bestHost && preferPublisherOrRetail(bestHost!)))) {
                bestFull = full;
                bestHost = host;
              }
            }
          }
        }
      }
    }
  }

  if (bestFull && /:\s*Wikipedia\s*$/i.test(bestFull)) bestFull = null;

  const result = {
    full: bestFull || shortTitle,
    usedShort: !bestFull,
    debug: { q, bestFull, evidence }
  };
  lruExpandTitle.set(key, { value: result, expires: now + CACHE_TTL_POS_MS });
  return result;
}

/* =============================
   Phase 1: Determine Author(s)
   ============================= */

type AuthorSignal =
  | { kind: "author_label"; name: string }
  | { kind: "written_by"; name: string }
  | { kind: "byline_ordered"; name: string; position: number }
  | { kind: "with"; name: string }
  | { kind: "editor"; name: string }
  | { kind: "foreword"; name: string }
  | { kind: "illustrator"; name: string }
  | { kind: "plain_last_first"; name: string };

const POSITION_WEIGHTS = [1.0, 0.9, 0.85, 0.8, 0.75, 0.7];

function splitNamesList(s: string): string[] {
  return s
    .split(/\s*,\s*|\s+and\s+|\s*&\s+/i)
    .map((x) => x.trim())
    .filter(Boolean);
}

const RE_AUTHOR = /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=[),.?:;!–—-]|\s|$)/gu;
const RE_WRITTEN = /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=[),.?:;!–—-]|\s|$)/gu;
const RE_BYLINE_BLOCK = /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*(?:\s*(?:,?\s*and\s+|&\s*)[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})?)(?:\s+with\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*)\b)?/gu;
const RE_EDITED_BY = /\bedited by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*(?:\s*(?:,?\s*and\s+|&\s*)[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})?)/gu;
const RE_FOREWORD_BY = /\bforeword by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/gu;
const RE_ILLUSTRATED_BY = /\billustrated by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*(?:\s*(?:,?\s*and\s+|&\s*)[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})?)/gu;
const RE_LAST_FIRST = /\b([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)\b(?!\s*(?:and|or|&|,))/gu;

function extractAuthorSignals(text: string, opts?: { booky?: boolean; title?: string }): AuthorSignal[] {
  const out: AuthorSignal[] = [];
  const booky = !!opts?.booky;
  const title = opts?.title || "";

  let m: RegExpExecArray | null;

  while ((m = RE_AUTHOR.exec(text))) {
    out.push({ kind: "author_label", name: cleanRoleArtifacts(m[1]) });
  }
  while ((m = RE_WRITTEN.exec(text))) {
    out.push({ kind: "written_by", name: cleanRoleArtifacts(m[1]) });
  }
  while ((m = RE_BYLINE_BLOCK.exec(text))) {
    const list = splitNamesList(m[1]).map(cleanRoleArtifacts).map(stripTrailingGarbage).map(stripDiscourseTail).filter(Boolean);
    list.forEach((n, i) => out.push({ kind: "byline_ordered", name: n, position: i }));
    if (m[2]) {
      const withList = splitNamesList(m[2]).map(cleanRoleArtifacts).map(stripTrailingGarbage).map(stripDiscourseTail).filter(Boolean);
      withList.forEach((n) => out.push({ kind: "with", name: n }));
    }
  }
  while ((m = RE_EDITED_BY.exec(text))) {
    splitNamesList(m[1]).map(cleanRoleArtifacts).map(stripTrailingGarbage).map(stripDiscourseTail).filter(Boolean)
      .forEach((n) => out.push({ kind: "editor", name: n }));
  }
  while ((m = RE_FOREWORD_BY.exec(text))) {
    out.push({ kind: "foreword", name: cleanRoleArtifacts(stripDiscourseTail(m[1])) });
  }
  while ((m = RE_ILLUSTRATED_BY.exec(text))) {
    splitNamesList(m[1]).map(cleanRoleArtifacts).map(stripTrailingGarbage).map(stripDiscourseTail).filter(Boolean)
      .forEach((n) => out.push({ kind: "illustrator", name: n }));
  }

  if (booky) {
    while ((m = RE_LAST_FIRST.exec(text))) {
      const name = `${m[2]} ${m[1]}`;
      out.push({ kind: "plain_last_first", name: cleanRoleArtifacts(stripDiscourseTail(name)) });
    }
  }

  // Filter out obvious false positives that echo the title content
  const titleToks = tokens(title);
  return out.filter(sig => {
    const normed = tokens(sig.name);
    const overlap = normed.filter(t => titleToks.includes(t)).length;
    return overlap < Math.min(2, normed.length);
  });
}

type AuthorAgg = {
  score: number;
  roles: Record<string, number>;
  positions: number[];
  highSignals: number;
};

function weightForSignal(sig: AuthorSignal, host: string, expandedMatch: boolean): number {
  let w =
    sig.kind === "author_label" ? 1.0 :
    sig.kind === "written_by" ? 1.0 :
    sig.kind === "byline_ordered" ? 0.8 * (POSITION_WEIGHTS[sig.position] || POSITION_WEIGHTS[POSITION_WEIGHTS.length - 1]) :
    sig.kind === "with" ? 0.35 :
    sig.kind === "editor" ? 0.2 :
    sig.kind === "foreword" ? 0.1 :
    sig.kind === "illustrator" ? 0.1 :
    sig.kind === "plain_last_first" ? 0.6 : 0.0;

  if (host && PUBLISHERS.some(d => host.endsWith(d))) w *= 1.4;
  if (expandedMatch) w *= 1.15;

  return w;
}

async function resolveAuthor(bookTitle: string): Promise<{
  primary: string | null;
  coAuthors: string[];
  confidence: number;
  ranked: Array<{ name: string; score: number }>;
  debug: any;
}> {
  const expanded = await expandBookTitle(bookTitle);
  const titleForSearch = expanded.full;

  let query = `"${titleForSearch}" book written by -film -movie -screenplay -soundtrack -director`;
  let items = await googleCSE(query, 10);

  const looksSuspiciousExpansion = /:\s*Wikipedia\s*$/i.test(titleForSearch);
  const allPdfOrWiki = (items || []).length > 0 && (items || []).every(it => {
    const h = hostOf(it.link);
    const mime = (it as any).mime || "";
    return h.includes("wikipedia.org") || /pdf/i.test(mime) || /\.pdf(?:$|\?)/i.test(it.link);
  });

  if ((!items || items.length === 0) || looksSuspiciousExpansion || allPdfOrWiki) {
    query = `"${bookTitle}" book written by -film -movie -screenplay -soundtrack -director`;
    items = await googleCSE(query, 10);
  }

  const aggs = new Map<string, AuthorAgg>();

  for (const it of items) {
    const fields: string[] = [];
    if (typeof it.title === "string") fields.push(it.title);
    if (typeof it.snippet === "string") fields.push(it.snippet);

    const mtList = Array.isArray(it.pagemap?.metatags) ? it.pagemap.metatags : [];
    let mtIsBook = false;
    for (const mt of mtList) {
      if (mt && typeof mt === "object") {
        const t1 = (mt as any).title;
        const t2 = (mt as any)["og:title"];
        const t3 = (mt as any)["book:author"];
        const ogType = (mt as any)["og:type"];
        if (typeof t1 === "string") fields.push(t1);
        if (typeof t2 === "string") fields.push(t2);
        if (typeof t3 === "string") fields.push(`Author: ${t3}`);
        if (ogType && String(ogType).toLowerCase().includes("book")) mtIsBook = true;
      }
    }

    let host = "";
    try { host = new URL(it.link).host.toLowerCase(); } catch {}

    const haystack = fields.join(" • ");
    const candidateTitles: string[] = [];
    if (typeof it.title === "string") candidateTitles.push(it.title);
    for (const mt of mtList) {
      const t = (mt?.["og:title"] as string) || (mt?.title as string);
      if (typeof t === "string") candidateTitles.push(t);
    }
    const expandedMatch = candidateTitles.some(t => normalizeCompareTitle(t) === normalizeCompareTitle(titleForSearch));

    const sigs = extractAuthorSignals(haystack, { booky: mtIsBook, title: titleForSearch });

    for (const sig of sigs) {
      const normed = normalizeNameCandidate(sig.name, bookTitle);
      if (!normed) continue;

      if ((sig.kind === "plain_last_first") && (maybeOrgish(normed) || looksLikeCommonPair(normed))) continue;

      const w = weightForSignal(sig, host, expandedMatch);
      if (w <= 0) continue;

      const cur = aggs.get(normed) || { score: 0, roles: {}, positions: [], highSignals: 0 };
      cur.score += w;
      cur.roles[sig.kind] = (cur.roles[sig.kind] || 0) + w;
      if (sig.kind === "byline_ordered") cur.positions.push(sig.position);
      if (sig.kind === "author_label" || sig.kind === "written_by") cur.highSignals += 1;
      aggs.set(normed, cur);
    }
  }

  if (aggs.size === 0) {
    return {
      primary: null,
      coAuthors: [],
      confidence: 0,
      ranked: [],
      debug: { expanded, query, items, aggs: {} }
    };
  }

  // Merge short/long variants by first two tokens, now preferring SHORTEST canonical
  const clusters = new Map<string, Array<{ name: string; score: number; high: number }>>();
  for (const [name, a] of aggs) {
    const toks = name.split(/\s+/);
    if (toks.length < 2) continue;
    const key = toks.slice(0, 2).join(" ").toLowerCase();
    const arr = clusters.get(key) || [];
    arr.push({ name, score: a.score, high: a.highSignals });
    clusters.set(key, arr);
  }

  const merged: Array<{ name: string; score: number; high: number }> = [];
  for (const arr of clusters.values()) {
    // Choose canonical: SHORTEST unless a longer ends with a keepable suffix (Jr., III, Ph.D., etc.)
    const sorted = [...arr].sort((a, b) => {
      const al = a.name.split(/\s+/).length, bl = b.name.split(/\s+/).length;
      if (al !== bl) return al - bl; // shortest first
      if (a.score !== b.score) return b.score - a.score;
      return a.name.localeCompare(b.name);
    });

    // Find shortest; if it lacks keepable suffix but a longer same-root has it, pick that longer
    let canonical = sorted[0].name;
    const withSuffix = arr.find(e => endsWithKeepableSuffix(e.name));
    if (withSuffix) {
      const base = canonical.toLowerCase();
      const sufBase = withSuffix.name.toLowerCase().replace(/\s+(?:jr\.?|sr\.?|ii|iii|iv|v|ph\.?d\.?|m\.?d\.?|obe|cbe)$/i, "");
      if (base === sufBase) canonical = withSuffix.name;
    }

    // Sum scores across near-duplicates (prefix / suffix variants)
    const canonLow = canonical.toLowerCase();
    let score = 0;
    let high = 0;
    for (const e of arr) {
      const nLow = e.name.toLowerCase();
      const sameRoot = nLow === canonLow || nLow.startsWith(canonLow + " ") || canonLow.startsWith(nLow + " ");
      if (sameRoot) { score += e.score; high += e.high; }
    }
    merged.push({ name: canonical, score, high });
  }

  merged.sort((a, b) => b.score - a.score);

  // pick primary; prefer one with high signals unless another has massively more score
  let primary = merged[0];
  const withHigh = merged.filter(m => m.high > 0);
  if (withHigh.length) {
    const bestHigh = withHigh[0];
    if (primary.high === 0 && primary.score < 1.5 * bestHigh.score) {
      primary = bestHigh;
    }
  }

  const second = merged[1];
  let confidence = primary.score >= 1.6 ? 0.95 : 0.8;
  if (second) {
    const gap = (primary.score - second.score) / Math.max(1, primary.score);
    if (gap < 0.1 && (primary.high > 0 && second.high > 0)) {
      confidence = Math.max(0.85, confidence - 0.05);
    }
  }

  const coAuthors = merged.slice(1).map(m => m.name);

  return {
    primary: primary?.name || null,
    coAuthors,
    confidence,
    ranked: merged.map(m => ({ name: m.name, score: Number(m.score.toFixed(3)) })),
    debug: {
      expanded,
      query,
      items,
      aggs: Object.fromEntries([...aggs].map(([k, v]) => [k, { ...v, score: Number(v.score.toFixed(3)) }]))
    }
  };
}

/* =============================
   Page fetching + content validation helpers
   ============================= */

const MAX_BYTES = 400_000; // ~400KB cap

async function fetchPageText(url: string, ms = FETCH_TIMEOUT_SAMPLE_MS): Promise<string> {
  const key = cacheKey(["fetchText", RESOLVER_VERSION, url, ms]);
  const cached = lruFetchText.get(key);
  const now = Date.now();
  if (cached && cached.expires > now) return cached.value;

  try {
    const resp = await fetchWithRetryAbort(url, { headers: { Accept: "text/html,*/*" } }, ms, FETCH_RETRY_ON_ABORT);
    if (!resp?.ok) {
      lruFetchText.set(key, { value: "", expires: now + CACHE_TTL_NEG_MS, neg: true });
      return "";
    }
    const reader = (resp as any).body?.getReader?.();
    if (!reader) {
      const raw = await resp.text();
      const text = extractVisibleText(raw);
      lruFetchText.set(key, { value: text, expires: now + CACHE_TTL_POS_MS });
      return text;
    }
    let received = 0;
    const chunks: Uint8Array[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) {
        received += value.byteLength;
        chunks.push(value);
        if (received >= MAX_BYTES) break;
      }
    }
    const buf = new TextDecoder("utf-8", { fatal: false }).decode(concatUint8(chunks));
    const text = extractVisibleText(buf);
    lruFetchText.set(key, { value: text, expires: now + CACHE_TTL_POS_MS });
    return text;
  } catch {
    lruFetchText.set(key, { value: "", expires: now + CACHE_TTL_NEG_MS, neg: true });
    return "";
  }
}

function concatUint8(parts: Uint8Array[]): Uint8Array {
  const size = parts.reduce((n, p) => n + p.byteLength, 0);
  const out = new Uint8Array(size);
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.byteLength; }
  return out;
}

function extractVisibleText(html: string): string {
  const metaBits: string[] = [];

  html.replace(RE_BLOCK_META, (m) => {
    const c = m.match(RE_META_CONTENT)?.[1];
    if (c) metaBits.push(c);
    return m;
  });
  const title = html.match(RE_TITLE_TAG)?.[1] ?? "";

  const jsonLdMatches = html.match(RE_JSONLD_BLOCK) || [];
  for (const block of jsonLdMatches) {
    const inner = block.match(/<script[^>]*>([\s\S]*?)<\/script>/i)?.[1];
    if (inner) metaBits.push(inner);
  }

  let txt = html
    .replace(RE_SCRIPT, " ")
    .replace(RE_STYLE, " ")
    .replace(RE_HTML_COMMENT, " ")
    .replace(RE_TAG, " ")
    .replace(RE_WS, " ")
    .trim();

  const metaJoined = [title, ...metaBits].filter(Boolean).join(" • ");
  return [metaJoined, txt].filter(Boolean).join(" • ");
}

function norm(s: string): string {
  return (s || "").toLowerCase().normalize("NFKC");
}

function tokenBag(s: string): string[] {
  return norm(s).replace(RE_NON_ALNUM_SPACE_APOS_HYPHEN, " ").split(/\s+/).filter(Boolean);
}

function overlapRatio(a: string[], b: string[]): number {
  const A = new Set(a.filter(t => t.length > 2));
  const B = new Set(b.filter(t => t.length > 2));
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter++;
  return inter / Math.min(A.size, B.size);
}

function hasJsonLdPersonOrBook(htmlOrText: string, author: string, bookTitle: string): { person: boolean; book: boolean } {
  const h = htmlOrText;
  const a = norm(author).replace(/"/g, '\\"');
  const b = norm(bookTitle).replace(/"/g, '\\"');

  const hasPerson = /"@type"\s*:\s*"(?:Person|Author)"/i.test(h) && new RegExp(`"name"\\s*:\\s*"?${a.replace(/\s+/g, "\\s+")}"?`, "i").test(h);
  const hasBook = /"@type"\s*:\s*"(?:Book|CreativeWork)"/i.test(h) && new RegExp(`"name"\\s*:\\s*".{0,20}${b.slice(0, 12).replace(/\s+/g, "\\s+")}.{0,20}"`, "i").test(h);
  return { person: !!hasPerson, book: !!hasBook };
}

function contentSignalsForSite(text: string, author: string, bookTitle: string): number {
  const a = norm(author);
  const bag = tokenBag(text);
  const bookOverlap = overlapRatio(bag, tokenBag(bookTitle)); // 0..1
  const aExact = new RegExp(`\\b${a.replace(/\s+/g, "\\s+")}\\b`, "i").test(text) ? 1 : 0;

  const hasBooksPage = /\b(books|publications|titles|works)\b/i.test(text) ? 1 : 0
