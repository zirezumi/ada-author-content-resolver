// api/book-author-website.ts
/// <reference types="node" />

/* =============================
   Vercel runtime
   ============================= */
export const config = { runtime: "nodejs" } as const;

/* =============================
   Tunables (perf + behavior)
   ============================= */
// Hard caps to avoid slowdowns from crawling
const WEBSITE_FETCH_TIMEOUT_MS = Number(process.env.WEBSITE_FETCH_TIMEOUT_MS ?? 2000); // per-page timeout
const WEBSITE_TOTAL_BUDGET_MS  = Number(process.env.WEBSITE_TOTAL_BUDGET_MS ?? 5000); // total budget per request for resolveWebsite
const WEBSITE_MAX_CANDIDATES   = Number(process.env.WEBSITE_MAX_CANDIDATES ?? 3);    // only validate top-N domains
const WEBSITE_MAX_PATHS_PER    = Number(process.env.WEBSITE_MAX_PATHS_PER ?? 2);     // max paths per domain (including "/")
const WEBSITE_EARLY_SATURATE   = Number(process.env.WEBSITE_EARLY_SATURATE ?? 0.85); // early exit threshold for content score
const WEBSITE_SITEQUERY_LIMIT  = Number(process.env.WEBSITE_SITEQUERY_LIMIT ?? 1);    // hits to try when doing site: query fallback
const ENABLE_IN_MEMORY_CACHE   = (process.env.WEBSITE_ENABLE_CACHE ?? "true").toLowerCase() === "true";
const PAGE_CACHE_TTL_MS        = Number(process.env.PAGE_CACHE_TTL_MS ?? 10 * 60 * 1000); // 10m
const SEARCH_CACHE_TTL_MS      = Number(process.env.SEARCH_CACHE_TTL_MS ?? 10 * 60 * 1000); // 10m
const MAX_BYTES = 400_000; // keep as-is (~400KB)

/* =============================
   Simple in-memory caches (best-effort)
   ============================= */
type CacheEntry<T> = { ts: number; value: T };
const pageTextCache = new Map<string, CacheEntry<string>>();
const searchCache   = new Map<string, CacheEntry<any[]>>();

function getCached<T>(map: Map<string, CacheEntry<T>>, key: string, ttl: number): T | undefined {
  if (!ENABLE_IN_MEMORY_CACHE) return undefined;
  const hit = map.get(key);
  if (!hit) return undefined;
  if (Date.now() - hit.ts > ttl) { map.delete(key); return undefined; }
  return hit.value;
}
function setCached<T>(map: Map<string, CacheEntry<T>>, key: string, val: T) {
  if (!ENABLE_IN_MEMORY_CACHE) return;
  map.set(key, { ts: Date.now(), value: val });
}

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
   Google CSE
   ============================= */
const CSE_KEY = process.env.GOOGLE_CSE_KEY || "";
const CSE_ID = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "";
const USE_SEARCH = !!(CSE_KEY && CSE_ID);

async function fetchWithTimeout(url: string, init?: RequestInit, ms = 5500) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { "User-Agent": "BookAuthorWebsite/1.0", ...(init?.headers || {}) },
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function googleCSE(query: string, num = 10): Promise<any[]> {
  if (!USE_SEARCH) return [];
  const cacheKey = `q:${query}|n:${num}`;
  const cached = getCached(searchCache, cacheKey, SEARCH_CACHE_TTL_MS);
  if (cached) return cached;

  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(num));

  const resp = await fetchWithTimeout(url.toString(), undefined, Math.min(WEBSITE_FETCH_TIMEOUT_MS, 5000));
  if (!resp?.ok) {
    const text = await resp?.text?.();
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  const items = Array.isArray(data?.items) ? data.items : [];
  setCached(searchCache, cacheKey, items);
  return items;
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

function hostOf(link: string): string {
  try { return new URL(link).host.toLowerCase(); } catch { return ""; }
}

function cleanTitle(s: string): string {
  return (s || "")
    .replace(/\s+/g, " ")
    .replace(/[“”]/g, '"')
    .trim();
}

function tokenSet(s: string): Set<string> {
  return new Set(
    s.toLowerCase()
     .normalize("NFKC")
     .replace(/[^\p{L}\p{N}\s':-]/gu, " ")
     .split(/\s+/).filter(Boolean)
  );
}

function normalizeCompareTitle(s: string): string {
  return s.toLowerCase().replace(/[\s“”"'-]+/g, " ").trim();
}

/* Helpers for subtitle tail hygiene */
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
  return (cut || "").replace(/\s+/g, " ").trim();
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
   Name-likeness scoring
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

// NEW: trailing function words/stopwords that should never end a person name
const TAIL_STOPWORDS = new Set([
  "due","to","of","for","and","or","but","with","from","at","by","in","on","as","than","because","since",
  "that","which","who","whom","whose","into","onto","over","under","after","before","between","among",
  "across","without","within","about","around","against","toward","towards","upon","per","via"
]);

function isCasedWordToken(tok: string): boolean {
  return /^[A-Z][\p{L}’'-]*[A-Za-z]$/u.test(tok);
}
function isParticle(tok: string): boolean { return MIDDLE_PARTICLES.has(tok.toLowerCase()); }
function isSuffix(tok: string): boolean { return GENERATIONAL_SUFFIX.has(tok.toLowerCase()); }
function looksLikeAdverb(tok: string): boolean { return /^[A-Za-z]{3,}ly$/u.test(tok); }

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
   EXTRA: explicit org/imprint detection + tail stripping
   ============================= */
const NAME_TAIL_GARBAGE = new Set([
  "book","books","press","publisher","publishers","publishing","imprint",
  "media","group","house","co","co.","company","inc","inc.","llc","ltd","ltd.","gmbh",
  "records","studios","partners","associates"
]);

function stripTrailingGarbage(s: string): string {
  let parts = s.split(/\s+/);
  let changed = false;
  while (parts.length > 1) {
    const last = parts[parts.length - 1].toLowerCase();
    if (NAME_TAIL_GARBAGE.has(last) || BAD_TAIL.has(last) || TAIL_STOPWORDS.has(last) || looksLikeAdverb(last)) {
      parts.pop();
      changed = true;
      continue;
    }
    break;
  }
  return changed ? parts.join(" ") : s;
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
   Clean + validate a candidate
   ============================= */
function normalizeNameCandidate(raw: string, title: string): string | null {
  let s = raw.trim()
    .replace(/[)\]]+$/g, "")
    .replace(/[.,;:–—-]+$/g, "")
    .replace(/\s+/g, " ");

  s = s.split(/\s+/).map(tok => tok.replace(/[.,;:]+$/g, "")).join(" ");

  // First pass: strip publisher-ish tails early
  s = stripTrailingGarbage(s);

  // Hard reject organizations / imprints
  if (isOrgishName(s)) return null;

  // Early reject for common-word pairs (subtitle artifacts)
  const partsEarly = s.split(/\s+/);
  if (partsEarly.length === 2 && looksLikeCommonPair(s)) return null;

  const titleToks = tokens(title);
  if (titleToks.includes(s.toLowerCase())) return null;

  // If it's a 2-word candidate and both words appear in the title, reject
  if (partsEarly.length === 2) {
    const overlap = partsEarly.filter(p => titleToks.includes(p.toLowerCase())).length;
    if (overlap === 2) return null;
  }

  // Drop one trailing garbage token if present
  const parts = s.split(" ");
  const last = parts[parts.length - 1];
  if (BAD_TAIL.has(last.toLowerCase()) || TAIL_STOPWORDS.has(last.toLowerCase()) || looksLikeAdverb(last)) {
    if (parts.length >= 3) { parts.pop(); s = parts.join(" "); }
  }

  // Second pass strip + org re-check
  s = stripTrailingGarbage(s);
  if (isOrgishName(s)) return null;

  return nameLikeness(s) >= 0.65 ? s : null;
}

/* =============================
   Extract names with strict rules (+publisher-context skip)
   ============================= */
type Signal = "author_label" | "wrote_book" | "byline" | "plain_last_first";

function extractNamesFromText(
  text: string,
  opts?: { booky?: boolean }
): Array<{ name: string; signal: Signal }> {
  const booky = !!opts?.booky;
  const out: Array<{ name: string; signal: Signal }> = [];

  const patterns: Array<{ re: RegExp; signal: Signal; swap?: boolean }> = [
    { re: /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "author_label" },
    { re: /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "author_label" },
    { re: /\b([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s+(?:wrote|writes)\s+the\s+book\b/gu, signal: "wrote_book" },
    { re: /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "byline" },
    { re: /\bby\s+([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)(?=\s*(?:[),.?:;!–—-]\s|$))/gu, signal: "byline", swap: true },
  ];

  if (booky) {
    patterns.push({
      re: /\b([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)\b(?!\s*(?:and|or|&|,))/gu,
      signal: "plain_last_first",
      swap: true
    });
  }

  for (const { re, signal, swap } of patterns) {
    let m: RegExpExecArray | null;
    while ((m = re.exec(text))) {
      // Skip publisher contexts like “Published by Forge Books”
      const left = text.slice(Math.max(0, m.index - 30), m.index);
      if (/\b(Published|Imprint|Edition|Edited)\s+$/i.test(left)) continue;

      const name = swap ? `${m[2]} ${m[1]}` : m[1];
      out.push({ name: name.trim(), signal });
    }
  }
  return out;
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
   Phase 0: Title expansion (subtitle recovery)
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
  const q = `"${shortTitle}" book`;
  const items = await googleCSE(q, 8);

  let bestFull: string | null = null;
  let bestHost: string | null = null;  // track host
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
        if (!bestFull || (preferPublisherOrRetail(host) && !(bestHost && preferPublisherOrRetail(bestHost)))) {
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
              if (!bestFull || (preferPublisherOrRetail(host) && !(bestHost && preferPublisherOrRetail(bestHost)))) {
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

  return {
    full: bestFull || shortTitle,
    usedShort: !bestFull,
    debug: { q, bestFull, evidence }
  };
}

/* =============================
   Phase 1: Determine Author (with title expansion + robust fallback)
   ============================= */
async function resolveAuthor(bookTitle: string): Promise<{ name: string|null, confidence: number, debug: any }> {
  // Phase 0: expand title
  const expanded = await expandBookTitle(bookTitle);
  const titleForSearch = expanded.full;

  let query = `"${titleForSearch}" book written by -film -movie -screenplay -soundtrack -director`;
  let items = await googleCSE(query, 10);

  // Fallback if expansion was suspicious or results are junky (all PDFs/wiki)
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

  const candidates: Record<string, number> = {};
  const highSignalSeen: Record<string, number> = {};

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
        if (typeof t3 === "string") fields.push(t3);
        if (ogType && String(ogType).toLowerCase().includes("book")) mtIsBook = true;
      }
    }

    let host = "";
    try { host = new URL(it.link).host.toLowerCase(); } catch {}

    const haystack = fields.join(" • ");
    const matches = extractNamesFromText(haystack, { booky: mtIsBook });

    const candidateTitles: string[] = [];
    if (typeof it.title === "string") candidateTitles.push(it.title);
    for (const mt of mtList) {
      const t = (mt?.["og:title"] as string) || (mt?.title as string);
      if (typeof t === "string") candidateTitles.push(t);
    }
    const expandedMatch = candidateTitles.some(t => normalizeCompareTitle(t) === normalizeCompareTitle(titleForSearch));

    for (const { name, signal } of matches) {
      const norm = normalizeNameCandidate(name, bookTitle);
      if (!norm) continue;

      let w: number;
      switch (signal) {
        case "author_label":     w = 3.0; break;
        case "wrote_book":       w = 2.0; break;
        case "byline":           w = 1.0; break;
        case "plain_last_first": w = 0.6; break;
      }

      // Reject org-ish or common-word pairs for weak signals
      if ((signal === "byline" || signal === "plain_last_first") &&
          (maybeOrgish(norm) || looksLikeCommonPair(norm))) {
        continue;
      }

      // Publisher boost
      if (host && PUBLISHERS.some(d => host.endsWith(d))) w *= 1.4;

      // Expanded title exact-match nudge
      if (expandedMatch) w *= 1.15;

      candidates[norm] = (candidates[norm] || 0) + w;

      if (signal === "author_label" || signal === "wrote_book") {
        highSignalSeen[norm] = (highSignalSeen[norm] || 0) + 1;
      }
    }
  }

  const entries = Object.entries(candidates);
  if (entries.length === 0) {
    return { name: null, confidence: 0, debug: { expanded, query, items, candidates } };
  }

  // Helper to check tail cleanliness
  function hasCleanTail(name: string): boolean {
    const toks = name.trim().split(/\s+/);
    if (toks.length === 0) return false;
    const last = toks[toks.length - 1].toLowerCase();
    return !(BAD_TAIL.has(last) || NAME_TAIL_GARBAGE.has(last) || TAIL_STOPWORDS.has(last) || looksLikeAdverb(toks[toks.length - 1]));
  }

  // Cluster variants (merge prefixes/supersets), then pick CLEAN tail first
  const clusters = new Map<string, Array<{ name: string; count: number }>>();
  for (const [name, count] of entries) {
    const toks = name.split(/\s+/);
    if (toks.length < 2) continue;
    const key = toks.slice(0, 2).join(" ").toLowerCase();
    const arr = clusters.get(key) || [];
    arr.push({ name, count });
    clusters.set(key, arr);
  }

  const merged: Array<{ name: string; score: number }> = [];
  for (const arr of clusters.values()) {
    // Prefer clean-tail candidates by highest count; tie-break by length then lexicographically
    const clean = arr.filter((x) => hasCleanTail(x.name));
    const pool = clean.length ? clean : arr.slice();

    pool.sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      const al = a.name.split(/\s+/).length, bl = b.name.split(/\s+/).length;
      if (bl !== al) return bl - al;
      return a.name.localeCompare(b.name);
    });

    const representative = pool[0].name;
    const repLower = representative.toLowerCase();

    // Sum scores from variants that are prefix/superset compatible
    let score = 0;
    for (const { name, count } of arr) {
      const nLower = name.toLowerCase();
      if (repLower.startsWith(nLower) || nLower.startsWith(repLower)) score += count;
    }
    merged.push({ name: representative, score });
  }

  if (merged.length) {
    merged.sort((a, b) => b.score - a.score);

    const withHigh = merged.filter(m => (highSignalSeen[m.name] || 0) > 0);

    let pick = merged[0];
    if (withHigh.length) {
      const bestHigh = withHigh[0];
      const bestOverall = merged[0];

      if ((highSignalSeen[bestOverall.name] || 0) === 0) {
        if (bestOverall.score < 1.5 * bestHigh.score) {
          pick = bestHigh;
        } else {
          pick = bestOverall;
        }
      } else {
        pick = bestOverall;
      }
    }

    const confidence = pick.score >= 2 ? 0.95 : 0.8;
    return { name: pick.name, confidence, debug: { expanded, query, items, candidates, merged, highSignalSeen, picked: pick.name } };
  }

  // Fallback
  const sorted = entries.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return b[0].split(/\s+/).length - a[0].split(/\s+/).length;
  });
  const [best, count] = sorted[0];
  const confidence = count >= 2 ? 0.95 : 0.8;
  return { name: best, confidence, debug: { expanded, query, items, candidates, picked: best } };
}

/* =============================
   Page fetching + content validation helpers
   ============================= */

async function fetchPageText(url: string, ms = WEBSITE_FETCH_TIMEOUT_MS): Promise<string> {
  const cached = getCached(pageTextCache, url, PAGE_CACHE_TTL_MS);
  if (cached !== undefined) return cached;

  try {
    const resp = await fetchWithTimeout(url, { headers: { Accept: "text/html,*/*" } }, ms);
    if (!resp?.ok) { setCached(pageTextCache, url, ""); return ""; }
    // stream and cap
    const reader = resp.body?.getReader();
    if (!reader) {
      const t = await resp.text();
      const vis = extractVisibleText(t);
      setCached(pageTextCache, url, vis);
      return vis;
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
    const vis = extractVisibleText(buf);
    setCached(pageTextCache, url, vis);
    return vis;
  } catch {
    setCached(pageTextCache, url, "");
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
  // keep meta/title content; also capture JSON-LD before stripping scripts
  const metaBits: string[] = [];

  // META/TITLE
  html.replace(
    /<meta\b[^>]*?(?:name|property)=["'](?:og:title|og:description|description|twitter:title|twitter:description)["'][^>]*?>/gi,
    (m) => {
      const c = m.match(/\bcontent=["']([^"']+)["']/i)?.[1];
      if (c) metaBits.push(c);
      return m;
    }
  );
  const title = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] ?? "";

  // JSON-LD (Person/Book signals)
  const jsonLdMatches = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const block of jsonLdMatches) {
    const inner = block.match(/<script[^>]*>([\s\S]*?)<\/script>/i)?.[1];
    if (inner) metaBits.push(inner);
  }

  // Strip scripts/styles/comments/tags for visible text
  let txt = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const metaJoined = [title, ...metaBits].filter(Boolean).join(" • ");
  return [metaJoined, txt].filter(Boolean).join(" • ");
}

function norm(s: string): string {
  return (s || "").toLowerCase().normalize("NFKC");
}

function tokenBag(s: string): string[] {
  return norm(s).replace(/[^\p{L}\p{N}\s'-]/gu, " ").split(/\s+/).filter(Boolean);
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
  // quick-and-dirty detection without a full HTML parser
  const h = htmlOrText;
  const a = norm(author).replace(/"/g, '\\"');
  const b = norm(bookTitle).replace(/"/g, '\\"');

  const hasPerson = /"@type"\s*:\s*"(?:Person|Author)"/i.test(h) && new RegExp(`"name"\\s*:\\s*"?${a.replace(/\s+/g, "\\s+")}"?`, "i").test(h);
  const hasBook = /"@type"\s*:\s*"(?:Book|CreativeWork)"/i.test(h) && new RegExp(`"name"\\s*:\\s*".{0,20}${b.slice(0, 12).replace(/\s+/g, "\\s+")}.{0,20}"`, "i").test(h);
  return { person: !!hasPerson, book: !!hasBook };
}

function contentSignalsForSite(text: string, author: string, bookTitle: string): number {
  // weighted content-score 0..1
  const a = norm(author);

  const bag = tokenBag(text);
  const bookOverlap = overlapRatio(bag, tokenBag(bookTitle)); // 0..1
  const aExact = new RegExp(`\\b${a.replace(/\s+/g, "\\s+")}\\b`, "i").test(text) ? 1 : 0;
  const aboutName = new RegExp(`\\babout\\s+${a.replace(/\s+/g, "\\s+")}\\b`, "i").test(text) ? 1 : 0;
  const byName = new RegExp(`\\bby\\s+${a.replace(/\s+/g, "\\s+")}\\b`, "i").test(text) ? 1 : 0;

  const { person, book } = hasJsonLdPersonOrBook(text, author, bookTitle);

  // penalties for obvious wrong-person cues when NO book signals are present
  const sporty = /\b(olympian|olympics|ski|nfl|nba|cyclist|triathlete)\b/i.test(text) ? 1 : 0;
  const altCareerPenalty = (sporty && bookOverlap < 0.2) ? 0.25 : 0;

  // combine
  let score =
    0.45 * Math.min(1, bookOverlap * 1.5) +
    0.2 * aExact +
    0.1 * (aboutName || byName ? 1 : 0) +
    0.15 * (person ? 1 : 0) +
    0.1 * (book ? 1 : 0);

  score = Math.max(0, score - altCareerPenalty);
  return Math.min(1, score);
}

/* =============================
   Phase 2: Candidate URL scoring (helpers + threshold)
   ============================= */
const BLOCKED_DOMAINS = [
  "wikipedia.org","goodreads.com","google.com","books.google.com","reddit.com",
  "amazon.","barnesandnoble.com","bookshop.org","penguinrandomhouse.com",
  "harpercollins.com","simonandschuster.com","macmillan.com","hachettebookgroup.com",
  "spotify.com","apple.com"
];

function isBlockedHost(host: string): boolean {
  return BLOCKED_DOMAINS.some((d) => host.includes(d));
}

function scoreCandidateUrl(u: URL, author: string): number {
  let score = 0;
  const pathDepth = u.pathname.split("/").filter(Boolean).length;
  if (pathDepth === 0) score += 0.5;
  if (u.protocol === "https:") score += 0.1;

  const toks = tokens(author).filter(t => t.length > 2);
  const host = u.host.toLowerCase();
  for (const t of toks) if (host.includes(t)) score += 0.4;

  return Math.min(1, score);
}

// Minimum normalized score to accept a site (default 0.6)
const WEBSITE_MIN_SCORE = Number(process.env.WEBSITE_MIN_SCORE ?? 0.6);

/* =============================
   Small async pool (limit per-domain path fetch concurrency)
   ============================= */
async function mapPool<T, R>(items: T[], limit: number, fn: (x: T, idx: number) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let i = 0;
  const workers: Promise<void>[] = [];
  async function run() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      out[idx] = await fn(items[idx], idx);
    }
  }
  for (let k = 0; k < Math.max(1, Math.min(limit, items.length)); k++) {
    workers.push(run());
  }
  await Promise.all(workers);
  return out;
}

/* =============================
   Optimized resolveWebsite with early exits, caching, and budgets
   ============================= */
async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
  const start = Date.now();
  function remainingBudget(): number {
    return Math.max(0, WEBSITE_TOTAL_BUDGET_MS - (Date.now() - start));
  }
  function timeboxed<T>(p: Promise<T>, ms: number): Promise<T> {
    const t = new Promise<never>((_, rej) => setTimeout(() => rej(new Error("timebox")), ms));
    return Promise.race([p, t]) as Promise<T>;
  }

  const queries = [
    `"${author}" official site`,
    `"${author}" author website`,
    `"${bookTitle}" "${author}" author website`,
  ];

  // First pass: get a few distinct domains (CSE results are cached)
  const candidates: Array<{ urlObj: URL; urlScore: number; fromQuery: string }> = [];
  for (const q of queries) {
    if (remainingBudget() <= 0) break;
    const items = await timeboxed(googleCSE(q, 10), Math.min(remainingBudget(), 2500));
    for (const it of items) {
      let u: URL | null = null;
      try { u = new URL(it.link); } catch { continue; }
      const host = u.host.toLowerCase();
      if (isBlockedHost(host)) continue;
      const urlScore = scoreCandidateUrl(u, author);
      // keep only one per domain (prefer higher urlScore)
      const existIdx = candidates.findIndex(c => c.urlObj.host === u!.host);
      if (existIdx === -1) {
        candidates.push({ urlObj: u, urlScore, fromQuery: q });
      } else if (candidates[existIdx].urlScore < urlScore) {
        candidates[existIdx] = { urlObj: u, urlScore, fromQuery: q };
      }
    }
  }

  if (!candidates.length) {
    return { url: null, debug: { tried: queries, threshold: WEBSITE_MIN_SCORE, reason: "no_candidates" } };
  }

  // Sort and trim to top domains to validate
  const topFew = candidates
    .sort((a, b) => b.urlScore - a.urlScore)
    .slice(0, WEBSITE_MAX_CANDIDATES);

  // Validate content on each domain with:
  //  - fast homepage check
  //  - at most WEBSITE_MAX_PATHS_PER extra paths
  //  - early exit when contentScore >= WEBSITE_EARLY_SATURATE
  //  - optional single site: query if still weak
  const COMMON_PATHS = ["", "/about", "/books", "/book", "/works", "/publications", "/titles", "/bio"];

  type Validation = {
    origin: string;
    urlScore: number;
    contentScore: number;
    finalScore: number;
    samples: Array<{ path: string; contentScore: number }>;
    fromQuery: string;
    elapsedMs: number;
  };

  const validations: Validation[] = [];

  for (const c of topFew) {
    if (remainingBudget() <= 0) break;

    const vStart = Date.now();
    const origin = c.urlObj.origin;
    const samples: Array<{ path: string; contentScore: number }> = [];

    // 1) Homepage fast check (timeboxed)
    const homeText = await timeboxed(fetchPageText(origin, Math.min(WEBSITE_FETCH_TIMEOUT_MS, remainingBudget())), Math.min(remainingBudget(), WEBSITE_FETCH_TIMEOUT_MS));
    let bestContent = contentSignalsForSite(homeText, author, bookTitle);
    samples.push({ path: "/", contentScore: bestContent });

    // If URL is name-like and homepage is decent, accept with lighter threshold (skip deeper crawl)
    const authorToks = tokens(author).filter(t => t.length > 2);
    const host = new URL(origin).host.toLowerCase();
    const hostHasAuthor = authorToks.some(t => host.includes(t));
    if (hostHasAuthor && bestContent >= 0.5) {
      const finalScore = 0.45 * c.urlScore + 0.55 * bestContent;
      validations.push({
        origin, urlScore: c.urlScore, contentScore: bestContent, finalScore, samples, fromQuery: c.fromQuery,
        elapsedMs: Date.now() - vStart
      });
      continue; // early accept consideration among candidates
    }

    // 2) Probe a few common paths (bounded number & parallel limited)
    if (bestContent < WEBSITE_EARLY_SATURATE && remainingBudget() > 0) {
      const extraPaths = COMMON_PATHS.slice(1).slice(0, WEBSITE_MAX_PATHS_PER - 1); // already did "/"
      const toFetch = extraPaths.map(p => ({ url: origin + p, path: p }));
      const pathResults = await mapPool(toFetch, 2, async ({ url, path }) => {
        if (remainingBudget() <= 0) return { path, score: 0 };
        try {
          const text = await timeboxed(fetchPageText(url, Math.min(WEBSITE_FETCH_TIMEOUT_MS, remainingBudget())), Math.min(remainingBudget(), WEBSITE_FETCH_TIMEOUT_MS));
          const s = contentSignalsForSite(text, author, bookTitle);
          return { path, score: s };
        } catch { return { path, score: 0 }; }
      });

      for (const r of pathResults) {
        samples.push({ path: r.path || "", contentScore: r.score });
        bestContent = Math.max(bestContent, r.score);
        if (bestContent >= WEBSITE_EARLY_SATURATE) break; // early saturation
      }
    }

    // 3) If still weak, try a single targeted site: query for the BOOK TITLE on this domain
    if (bestContent < 0.5 && remainingBudget() > 0 && WEBSITE_SITEQUERY_LIMIT > 0) {
      try {
        const siteQuery = `"${bookTitle}" site:${c.urlObj.host}`;
        const siteHits = await timeboxed(googleCSE(siteQuery, WEBSITE_SITEQUERY_LIMIT), Math.min(remainingBudget(), 2000));
        const firstHit = siteHits?.find(h => {
          try { return new URL(h.link).host === c.urlObj.host; } catch { return false; }
        });
        if (firstHit) {
          const t = await timeboxed(fetchPageText(firstHit.link, Math.min(WEBSITE_FETCH_TIMEOUT_MS, remainingBudget())), Math.min(remainingBudget(), WEBSITE_FETCH_TIMEOUT_MS));
          const s = contentSignalsForSite(t, author, bookTitle);
          bestContent = Math.max(bestContent, s);
          samples.push({ path: new URL(firstHit.link).pathname, contentScore: s });
        }
      } catch { /* ignore */ }
    }

    // Combine URL & content scores
    const finalScore = 0.45 * c.urlScore + 0.55 * bestContent;
    validations.push({
      origin,
      urlScore: c.urlScore,
      contentScore: bestContent,
      finalScore,
      samples,
      fromQuery: c.fromQuery,
      elapsedMs: Date.now() - vStart
    });

    // If we have a very strong candidate already and budget is low, break
    if (finalScore >= 0.9 && remainingBudget() < 1000) break;
  }

  if (!validations.length) {
    return { url: null, debug: { threshold: WEBSITE_MIN_SCORE, reason: "no_validations", budgetUsedMs: Date.now() - start } };
  }

  // pick best validated site
  validations.sort((a, b) => b.finalScore - a.finalScore);
  const top = validations[0];

  // slightly higher bar if the domain isn’t name-like (no last name in host)
  const authorToks = tokens(author).filter(t => t.length > 2);
  const host = new URL(top.origin).host.toLowerCase();
  const hostHasAuthor = authorToks.some(t => host.includes(t));
  const threshold = Math.max(WEBSITE_MIN_SCORE, hostHasAuthor ? 0.60 : 0.70);

  if (top.finalScore >= threshold) {
    return {
      url: top.origin,
      debug: {
        threshold,
        picked: top,
        candidates: validations.slice(0, 5),
        budgetUsedMs: Date.now() - start,
        knobs: {
          WEBSITE_FETCH_TIMEOUT_MS, WEBSITE_TOTAL_BUDGET_MS, WEBSITE_MAX_CANDIDATES,
          WEBSITE_MAX_PATHS_PER, WEBSITE_EARLY_SATURATE, WEBSITE_SITEQUERY_LIMIT,
          CACHE: ENABLE_IN_MEMORY_CACHE
        }
      }
    };
  }

  // No confident author site — return null so callers can treat as “no personal site”
  return {
    url: null,
    debug: {
      threshold,
      candidates: validations.slice(0, 5),
      budgetUsedMs: Date.now() - start,
      knobs: {
        WEBSITE_FETCH_TIMEOUT_MS, WEBSITE_TOTAL_BUDGET_MS, WEBSITE_MAX_CANDIDATES,
        WEBSITE_MAX_PATHS_PER, WEBSITE_EARLY_SATURATE, WEBSITE_SITEQUERY_LIMIT,
        CACHE: ENABLE_IN_MEMORY_CACHE
      }
    }
  };
}

/* =============================
   Handler
   ============================= */
export default async function handler(req: any, res: any) {
  try {
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "POST required" });
    if (!requireAuth(req, res)) return;

    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;
    const bookTitle: string = String(body.book_title || "").trim();
    if (!bookTitle) return res.status(400).json({ error: "book_title required" });

    const authorRes = await resolveAuthor(bookTitle);
    if (!authorRes.name) {
      return res.status(200).json({
        book_title: bookTitle,
        inferred_author: null,
        confidence: 0,
        error: "no_author_found",
        _diag: { flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID, WEBSITE_MIN_SCORE }, author: authorRes.debug }
      });
    }

    const siteRes = await resolveWebsite(authorRes.name, bookTitle);

    return res.status(200).json({
      book_title: bookTitle,
      inferred_author: authorRes.name,
      author_confidence: authorRes.confidence,
      author_url: siteRes.url,
      confidence: siteRes.url ? 0.9 : 0,
      _diag: {
        flags: {
          USE_SEARCH,
          CSE_KEY: !!CSE_KEY,
          CSE_ID: !!CSE_ID,
          WEBSITE_MIN_SCORE,
          PERF: {
            WEBSITE_FETCH_TIMEOUT_MS,
            WEBSITE_TOTAL_BUDGET_MS,
            WEBSITE_MAX_CANDIDATES,
            WEBSITE_MAX_PATHS_PER,
            WEBSITE_EARLY_SATURATE,
            WEBSITE_SITEQUERY_LIMIT,
            ENABLE_IN_MEMORY_CACHE: ENABLE_IN_MEMORY_CACHE
          }
        },
        author: authorRes.debug,
        site: siteRes.debug
      }
    });
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
