// api/book-author-website.ts
/// <reference types="node" />

/**
 * Deterministic, two-phase resolver:
 *  Phase 1:  "<BOOK_TITLE>" book author  → parse author from Wikipedia / Google Books / Goodreads (consensus + person-shape)
 *  Phase 2:  "<AUTHOR>" "<BOOK_TITLE>" author website → deterministic CSE filtering:
 *            - reject reference/retail/social/publisher (unless publisher fallback allowed)
 *            - prefer vanity domains containing author name (or clean root == author)
 *            - accept titles/snippets that say "Official Website"
 *
 * Input JSON:
 * {
 *   "book_title": "A Promised Land",
 *   "include_search": true,               // default true (needs GOOGLE_CSE_KEY + GOOGLE_CSE_ID)
 *   "allow_estate_sites": false,          // default false
 *   "exclude_publisher_sites": true,      // default true (block publisher domains in Phase 2)
 *   "debug": true                         // optional
 * }
 *
 * Output: { book_title, inferred_author, pub_year, life_dates, author_viable, viability_reason,
 *           author_url, site_title, canonical_url, author_confidence, confidence, _diag? }
 */

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
   Search config
   ============================= */
const CSE_KEY = (process.env.GOOGLE_CSE_KEY || "").trim();
const CSE_ID = (process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_CX || "").trim();
const USE_SEARCH_BASE = !!(CSE_KEY && CSE_ID);

/* =============================
   Tunables / limits
   ============================= */
const FETCH_TIMEOUT_MS = 7000;
const USER_AGENT = "BookAuthorWebsite/3.1 (+https://example.com)";
const CONCURRENCY = 4;

const limit = pLimit(CONCURRENCY);

/* =============================
   Utils
   ============================= */
function tokens(s: string): string[] {
  return String(s || "")
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
function normalizeBook(s: string) {
  return s.normalize("NFKC").replace(/\s+/g, " ").trim();
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
function eTLD1(host: string): string {
  const h = (host || "").replace(/^www\./, "");
  const parts = h.split(".");
  if (parts.length <= 2) return h;
  return parts.slice(-2).join(".");
}
function dedupeByETLD1(urls: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const u of urls) {
    try {
      const k = eTLD1(hostOf(u));
      if (!seen.has(k)) {
        seen.add(k);
        out.push(u);
      }
    } catch { /* ignore */ }
  }
  return out;
}

/* =============================
   Google CSE (deterministic usage)
   ============================= */
type CSEItem = { title: string; link: string; snippet?: string };
async function googleCSE(query: string, num = 10): Promise<CSEItem[]> {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(Math.max(1, Math.min(10, num))));
  const resp = await fetchWithTimeout(url.toString());
  const text = await resp.text();
  if (!resp?.ok) throw new Error(`cse_http_${resp?.status}: ${text?.slice(0, 500)}`);
  const data: any = JSON.parse(text);
  const items: any[] = Array.isArray(data?.items) ? data.items : [];
  return items.map(it => ({ title: String(it.title || ""), link: String(it.link || ""), snippet: String(it.snippet || it.htmlSnippet || "") }));
}

/* =============================
   Person-shape & name sanitation
   ============================= */
function sanitizeName(raw: string): string {
  let s = raw.replace(/\([^)]*\)/g, " "); // remove parentheticals
  s = s.replace(/\b(american|british|canadian|australian|irish|nigerian|indian|israeli|french|german|italian|spanish|mexican|scottish|welsh)\b\s*/gi, " ");
  s = s.replace(/\b(author|writer|novelist|historian|journalist|professor|phd|dr\.?)\b/gi, " ");
  s = s.replace(/[,;:]+/g, " ");
  return s.replace(/\s+/g, " ").trim();
}
function looksLikePersonName(name: string, bookTitle: string): boolean {
  const stop = new Set(["series","edition","volume","press","books","publishing","nation","inc","llc","ltd","homo","sapiens","deus","graphic","history","novel","memoir"]);
  const toks = name.trim().split(/\s+/);
  if (toks.length < 2 || toks.length > 4) return false;
  const bt = new Set(tokens(bookTitle));
  for (const t of toks) {
    const w = t.toLowerCase();
    const connector = /^(de|da|van|von|di|du|la|le|del|dos)$/i.test(w);
    const titlecase = /^[A-Z][\p{L}'-]+$/u.test(t);
    if (!(titlecase || connector)) return false;
    if (bt.has(w) || stop.has(w)) return false;
  }
  return true;
}

/* =============================
   Phase 1: deterministic author extraction helpers
   ============================= */

// Wikipedia (English) book page → infobox Author
async function wikipediaBookAuthorsFromCSE(title: string): Promise<string[]> {
  const q = `"${title}" site:en.wikipedia.org intitle:"${title}" -film -movie`;
  const items = await googleCSE(q);
  const page = items.find((it) => /en\.wikipedia\.org\/wiki\//i.test(it.link));
  if (!page) return [];
  const r = await fetchWithTimeout(page.link);
  if (!r?.ok) return [];
  const html = await r.text();
  const cell = html.match(/>Author<\/th>[^]*?<td[^>]*>([^]*?)<\/td>/i)?.[1] || "";
  const links = Array.from(cell.matchAll(/<a[^>]+>([^<]+)<\/a>/gi)).map((m) => m[1].trim()).filter(Boolean);
  const texts = links.length ? links : [cell.replace(/<[^>]+>/g, " ").trim()];
  const cleaned = texts.map((t) => sanitizeName(t)).filter(Boolean);
  return cleaned;
}

// Google Books → author + first publication year
async function googleBooksAuthorAndYearFromCSE(title: string): Promise<{ authors: string[]; year: number | null }> {
  const q = `"${title}" site:books.google.com intitle:"${title}"`;
  const items = await googleCSE(q);
  const page = items.find((it) => /books\.google\.com\/books/i.test(it.link));
  if (!page) return { authors: [], year: null };
  const r = await fetchWithTimeout(page.link);
  if (!r?.ok) return { authors: [], year: null };
  const html = await r.text();

  const metaAuthor = html.match(/<meta[^>]+name=["']author["'][^>]+content=["']([^"']+)["']/i)?.[1] || "";
  let authorTxt = metaAuthor;
  if (!authorTxt) {
    authorTxt = html.match(/\bBy ([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\b/u)?.[1] || "";
  }
  const authors = authorTxt
    ? authorTxt.split(/,|&| and /i).map((s) => sanitizeName(s.trim())).filter(Boolean)
    : [];

  let year: number | null = null;
  const y1 = html.match(/\bPublished\s*(?:on|in)?\s*(\d{4})\b/i)?.[1];
  const y2 = html.match(/\b(\d{4})\b[^<>{}]{0,40}pages/i)?.[1];
  if (y1 && /^\d{4}$/.test(y1)) year = Number(y1);
  else if (y2 && /^\d{4}$/.test(y2)) year = Number(y2);

  return { authors, year: year ?? null };
}

// Goodreads book page → byline author
async function goodreadsAuthorFromCSE(title: string): Promise<string[]> {
  const q = `"${title}" site:goodreads.com/book/show`;
  const items = await googleCSE(q);
  const page = items.find((it) => /goodreads\.com\/book\/show/i.test(it.link));
  if (!page) return [];
  const r = await fetchWithTimeout(page.link);
  if (!r?.ok) return [];
  const html = await r.text();
  const area = html.slice(0, 200_000);
  const m = area.match(/\bby\s+<a[^>]*>([^<]{2,120})<\/a>/i)?.[1] || "";
  const txt = m || area.match(/\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\b/u)?.[1] || "";
  const names = txt ? [sanitizeName(txt)] : [];
  return names.filter(Boolean);
}

function pickAuthorDeterministic(title: string, wiki: string[], gbooks: string[], goodreads: string[]) {
  const candidates = new Map<string, { sources: Set<string> }>();
  const push = (name: string, src: string) => {
    const clean = sanitizeName(name);
    if (!clean || !looksLikePersonName(clean, title)) return;
    if (!candidates.has(clean)) candidates.set(clean, { sources: new Set() });
    candidates.get(clean)!.sources.add(src);
  };
  wiki.forEach((n) => push(n, "wikipedia"));
  gbooks.forEach((n) => push(n, "googlebooks"));
  goodreads.forEach((n) => push(n, "goodreads"));

  let best: { name: string; confidence: number; sources: string[] } | null = null;
  for (const [name, { sources }] of candidates.entries()) {
    const srcs = Array.from(sources);
    let confidence = 0;
    if (srcs.includes("wikipedia") && srcs.includes("googlebooks")) confidence = 0.95;
    else if (srcs.includes("wikipedia") && srcs.includes("goodreads")) confidence = 0.9;
    else if (srcs.includes("googlebooks") && srcs.includes("goodreads")) confidence = 0.85;
    else if (srcs.length === 1) confidence = 0.8;
    if (!best || confidence > best.confidence) best = { name, confidence, sources: srcs };
  }
  return best;
}

/* =============================
   Life dates (for viability) from Author Wikipedia
   ============================= */
async function authorLifeDatesFromWikipedia(authorName: string): Promise<{ birthYear: number | null; deathYear: number | null; wikiUrl?: string | null }> {
  const q = `"${authorName}" site:en.wikipedia.org`;
  const items = await googleCSE(q);
  const page = items.find((it) => /en\.wikipedia\.org\/wiki\//i.test(it.link));
  if (!page) return { birthYear: null, deathYear: null, wikiUrl: null };
  const r = await fetchWithTimeout(page.link);
  if (!r?.ok) return { birthYear: null, deathYear: null, wikiUrl: page.link };
  const html = await r.text();

  const takeYear = (s?: string | null) => {
    const m = s?.match(/(\d{4})/);
    return m ? Number(m[1]) : null;
  };

  const birthISO = html.match(/class=["']bday["'][^>]*>(\d{4})-(\d{2})-(\d{2})/i)?.[1] || null;
  const bornRow = html.match(/>Born<[^]*?<td[^>]*>([^<]+)</i)?.[1] ?? null;
  const deathISO = html.match(/class=["']dday deathdate["'][^>]*>(\d{4})-(\d{2})-(\d{2})/i)?.[1] || null;
  const diedRow = html.match(/>Died<[^]*?<td[^>]*>([^<]+)</i)?.[1] ?? null;

  const birthYear = takeYear(birthISO) ?? takeYear(bornRow);
  const deathYear = takeYear(deathISO) ?? takeYear(diedRow);

  return { birthYear, deathYear, wikiUrl: page.link };
}

/* =============================
   Viability rules (deterministic)
   ============================= */
function evaluateAuthorViability(
  birthYear: number | null,
  deathYear: number | null,
  pubYear: number | null,
  allowEstateSites: boolean
): { viable: boolean; reason: string } {
  const nowYear = new Date().getFullYear();

  if (typeof deathYear === "number") {
    if (deathYear < 1995) return { viable: false, reason: "deceased_pre_web_era" };
    if (deathYear < 2005 && !allowEstateSites) return { viable: false, reason: "deceased_early_web_era_estate_not_allowed" };
    return allowEstateSites ? { viable: true, reason: "deceased_estate_allowed" } : { viable: false, reason: "deceased_estate_sites_not_allowed" };
  }
  if (typeof birthYear === "number") {
    const age = nowYear - birthYear;
    if (age >= 0 && age < 120) return { viable: true, reason: "likely_living_author" };
  }
  if (typeof pubYear === "number") {
    if (pubYear < 1900) return { viable: false, reason: "pubyear_lt_1900_not_viable" };
    if (pubYear < 1950) return allowEstateSites ? { viable: true, reason: "pubyear_1900_1950_estate_allowed" } : { viable: false, reason: "pubyear_1900_1950_estate_not_allowed" };
    if (pubYear < 1995) return { viable: true, reason: "pubyear_1950_1994_cautious" };
    return { viable: true, reason: "pubyear_ge_1995_likely_living" };
  }
  return { viable: false, reason: "unknown_life_and_pubyear_not_confident" };
}

/* =============================
   Phase 2 (refactor): Deterministic CSE-only website resolution
   ============================= */

// Hard-filter sets (reference/retail/social). Publishers can be excluded or allowed as fallback.
const REFERENCE_HOSTS = [
  "wikipedia.org", "wikidata.org", "wikimedia.org", "britannica.com",
  "goodreads.com", "books.google.com", "openlibrary.org"
];
const RETAIL_SOCIAL_HOSTS = [
  "amazon.", "audible.", "bookshop.org", "barnesandnoble.com", "itunes.apple.com",
  "kobo.com", "target.com", "waterstones.com",
  "twitter.com", "x.com", "facebook.com", "instagram.com", "tiktok.com", "linkedin.com", "youtube.com", "imdb.com"
];
const PUBLISHER_HOSTS = [
  "penguinrandomhouse.com","harpercollins.com","simonandschuster.com","macmillan.com","hachettebookgroup.com",
  "us.macmillan.com","bloomsbury.com","fsgbooks.com","knopfdb.com","littlebrown.com","doubleday.com","vintagebooks.com",
  "randomhouse.com","prh.com","prh.co.uk","panmacmillan.com","scholastic.com","tor.com","orbitbooks.net"
];

function hostInList(host: string, list: string[]): boolean {
  const h = host.toLowerCase();
  return list.some((d) => h === d || h.endsWith(`.${d}`) || h.includes(d)); // includes() catches amazon.* etc.
}

function compactAuthorKey(authorName: string): string {
  return authorName.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function looksVanityHost(host: string, authorName: string): boolean {
  const base = host.replace(/^www\./, "").toLowerCase();
  const key = compactAuthorKey(authorName);
  // accept if host contains compact author key (e.g., yuvalnoahharari.com, barackobamabooks.com, ynharari.com)
  if (base.replace(/[^a-z0-9]/g, "").includes(key)) return true;
  // hyphenated variants (yuval-noah-harari.com) — strip hyphens then compare
  if (base.replace(/-/g, "").includes(key)) return true;
  return false;
}

function titleOrSnippetHintsOfficial(title: string, snippet?: string): boolean {
  const hay = `${title} ${snippet || ""}`.toLowerCase();
  return /\bofficial (site|website)\b/.test(hay) || /\bauthor website\b/.test(hay);
}

function titleOrSnippetMentionsBooks(title: string, snippet?: string): boolean {
  const hay = `${title} ${snippet || ""}`.toLowerCase();
  return /\bbooks?\b|\bworks?\b|\bbibliography\b/.test(hay);
}

function buildPhase2Queries(authorName: string, bookTitle: string): string[] {
  const qA = `"${authorName}"`;
  const qB = `"${bookTitle}"`;
  const negatives = "-film -movie -director -trailer -whitehouse -campaign -donate";
  return [
    `${qB} ${qA} author website ${negatives}`,
    `${qA} ${qB} author website ${negatives}`,
    `${qA} "official website" ${negatives}`,
    `${qA} "official site" ${negatives}`,
    `${qA} author website ${negatives}`,
  ];
}

type SitePick = { url: string; siteTitle: string | null; reason: string };

function normalizeToOrigin(u: string): string {
  return originOf(u) ?? u;
}

function pickBestAuthorWebsiteFromCSE(
  items: CSEItem[],
  authorName: string,
  bookTitle: string,
  excludePublisherSites: boolean
): SitePick | null {
  // 1) Filter out reference + retail/social
  const filtered = items.filter((it) => {
    if (!isHttpUrl(it.link)) return false;
    const host = hostOf(it.link);
    if (hostInList(host, REFERENCE_HOSTS)) return false;
    if (hostInList(host, RETAIL_SOCIAL_HOSTS)) return false;
    if (excludePublisherSites && hostInList(host, PUBLISHER_HOSTS)) return false;
    return true;
  });

  // Dedup by eTLD+1
  const urls = dedupeByETLD1(filtered.map((f) => f.link));

  // Build a small map to retrieve titles/snippets by host
  const byHost = new Map<string, CSEItem>();
  filtered.forEach((f) => byHost.set(eTLD1(hostOf(f.link)), f));

  // 2) Acceptance order (deterministic “greedy”):
  // 2a) Vanity hosts that look like author domains AND mention "official" or "author website" in title/snippet.
  for (const u of urls) {
    const host = hostOf(u);
    const item = byHost.get(eTLD1(host))!;
    if (!looksVanityHost(host, authorName)) continue;
    if (!titleOrSnippetHintsOfficial(item.title, item.snippet) && !titleOrSnippetMentionsBooks(item.title, item.snippet)) continue;
    return { url: normalizeToOrigin(u), siteTitle: item.title || null, reason: "vanity_official_or_books_hint" };
  }

  // 2b) Vanity hosts that look like author domains, regardless of hint.
  for (const u of urls) {
    const host = hostOf(u);
    const item = byHost.get(eTLD1(host))!;
    if (!looksVanityHost(host, authorName)) continue;
    return { url: normalizeToOrigin(u), siteTitle: item.title || null, reason: "vanity_host" };
  }

  // 2c) Non-vanity domains whose title/snippet explicitly say "Official Website" or "author website".
  for (const u of urls) {
    const host = hostOf(u);
    const item = byHost.get(eTLD1(host))!;
    if (titleOrSnippetHintsOfficial(item.title, item.snippet)) {
      return { url: normalizeToOrigin(u), siteTitle: item.title || null, reason: "explicit_official_marker" };
    }
  }

  // 2d) Optional publisher author-profile fallback (if allowed)
  if (!excludePublisherSites) {
    for (const u of urls) {
      const host = hostOf(u);
      const item = byHost.get(eTLD1(host))!;
      if (hostInList(host, PUBLISHER_HOSTS)) {
        // heuristic: accept if title/snippet include author name AND book mentions/books hints
        const nameOk =
          jaccard(tokens(`${item.title} ${item.snippet || ""}`), tokens(authorName)) >= 0.3 ||
          containsAll(tokens(`${item.title} ${item.snippet || ""}`), tokens(authorName));
        const bookOk = jaccard(tokens(`${item.title} ${item.snippet || ""}`), tokens(bookTitle)) >= 0.2;
        if (nameOk && (bookOk || titleOrSnippetMentionsBooks(item.title, item.snippet))) {
          return { url: normalizeToOrigin(u), siteTitle: item.title || null, reason: "publisher_author_profile" };
        }
      }
    }
  }

  return null;
}

async function resolveWebsiteDeterministicCSE(
  authorName: string,
  bookTitle: string,
  excludePublisherSites: boolean
): Promise<SitePick | null> {
  const queries = buildPhase2Queries(authorName, bookTitle);
  const batches = await Promise.all(queries.map(q => limit(() => googleCSE(q))));
  const items = batches.flat();
  if (!items.length) return null;
  return pickBestAuthorWebsiteFromCSE(items, authorName, bookTitle, excludePublisherSites);
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

    const bookTitleInput = String(body.book_title || "");
    const bookTitle = normalizeBook(bookTitleInput);
    if (!bookTitle) return res.status(400).json({ error: "book_title required" });

    const includeSearch: boolean = body.include_search !== false; // default true
    const allowEstateSites: boolean = body.allow_estate_sites === true;
    const excludePublisherSites: boolean = body.exclude_publisher_sites !== false; // default true
    const debug: boolean = body.debug === true;

    const USE_SEARCH = includeSearch && USE_SEARCH_BASE;
    res.setHeader("x-use-search", String(USE_SEARCH));
    res.setHeader("x-cse-id-present", String(!!CSE_ID));
    res.setHeader("x-cse-key-present", String(!!CSE_KEY));

    if (!USE_SEARCH) {
      return res.status(200).json({
        book_title: bookTitle,
        inferred_author: undefined,
        pub_year: null,
        life_dates: null,
        author_viable: false,
        viability_reason: "search_disabled_or_not_configured",
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: 0,
        source: "web",
        _diag: debug ? { reason: "search_disabled_or_not_configured" } : undefined,
      });
    }

    /* ========= Phase 1: book → author (deterministic) ========= */
    const diagAuthor: any = { queries: [], wiki: null, googlebooks: null, goodreads: null, picked: null };

    const [wikiAuthors, gbInfo, grAuthors] = await Promise.all([
      (async () => {
        const q = `"${bookTitle}" site:en.wikipedia.org intitle:"${bookTitle}" -film -movie`;
        diagAuthor.queries.push(q);
        try { return await wikipediaBookAuthorsFromCSE(bookTitle); } catch { return []; }
      })(),
      (async () => {
        const q = `"${bookTitle}" site:books.google.com intitle:"${bookTitle}"`;
        diagAuthor.queries.push(q);
        try { return await googleBooksAuthorAndYearFromCSE(bookTitle); } catch { return { authors: [], year: null }; }
      })(),
      (async () => {
        const q = `"${bookTitle}" site:goodreads.com/book/show`;
        diagAuthor.queries.push(q);
        try { return await goodreadsAuthorFromCSE(bookTitle); } catch { return []; }
      })(),
    ]);

    diagAuthor.wiki = wikiAuthors;
    diagAuthor.googlebooks = gbInfo;
    diagAuthor.goodreads = grAuthors;

    const chosen = pickAuthorDeterministic(bookTitle, wikiAuthors, gbInfo.authors, grAuthors);
    if (!chosen) {
      return res.status(200).json({
        book_title: bookTitle,
        inferred_author: undefined,
        pub_year: gbInfo.year ?? null,
        life_dates: null,
        author_viable: false,
        viability_reason: "no_confident_author_match",
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: 0,
        source: "web",
        _diag: debug ? { author: diagAuthor } : undefined,
      });
    }

    const authorName = chosen.name;
    const authorConfidence = chosen.confidence;
    diagAuthor.picked = { name: authorName, confidence: authorConfidence, sources: chosen.sources };

    const pubYear = gbInfo.year ?? null;

    // Life dates via author bio page on English Wikipedia (single fetch)
    const life = await authorLifeDatesFromWikipedia(authorName);
    const viability = evaluateAuthorViability(life.birthYear, life.deathYear, pubYear, allowEstateSites);

    if (!viability.viable) {
      return res.status(200).json({
        book_title: bookTitle,
        inferred_author: authorName,
        pub_year: pubYear,
        life_dates: { birthYear: life.birthYear, deathYear: life.deathYear },
        author_viable: false,
        viability_reason: viability.reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: Number(authorConfidence.toFixed(2)),
        source: "web",
        _diag: debug ? { author: diagAuthor, life } : undefined,
      });
    }

    /* ========= Phase 2: author+book → author website (deterministic CSE-only) ========= */
    const queries = buildPhase2Queries(authorName, bookTitle);
    const batches = await Promise.all(queries.map(q => limit(() => googleCSE(q))));
    const items = batches.flat();

    const sitePick = pickBestAuthorWebsiteFromCSE(items, authorName, bookTitle, excludePublisherSites);

    const diagSite: any = debug ? { queries, considered: items.map(it => ({ title: it.title, link: it.link })), picked: sitePick } : undefined;

    if (!sitePick) {
      return res.status(200).json({
        book_title: bookTitle,
        inferred_author: authorName,
        pub_year: pubYear,
        life_dates: { birthYear: life.birthYear, deathYear: life.deathYear },
        author_viable: true,
        viability_reason: viability.reason,
        author_url: undefined,
        site_title: undefined,
        canonical_url: undefined,
        confidence: 0,
        author_confidence: Number(authorConfidence.toFixed(2)),
        source: "web",
        _diag: debug ? { author: diagAuthor, life, site: diagSite } : undefined,
      });
    }

    // We keep canonical_url = origin (no HTML fetch in this deterministic refactor)
    const origin = sitePick.url;
    const siteTitle = sitePick.siteTitle;

    // Confidence by path
    const conf =
      sitePick.reason === "vanity_official_or_books_hint" ? 0.9 :
      sitePick.reason === "vanity_host" ? 0.88 :
      sitePick.reason === "explicit_official_marker" ? 0.85 :
      sitePick.reason === "publisher_author_profile" ? 0.75 : 0.8;

    return res.status(200).json({
      book_title: bookTitle,
      inferred_author: authorName,
      pub_year: pubYear,
      life_dates: { birthYear: life.birthYear, deathYear: life.deathYear },
      author_viable: true,
      viability_reason: viability.reason,
      author_url: origin,
      site_title: siteTitle,
      canonical_url: origin,
      confidence: Number(conf.toFixed(2)),
      author_confidence: Number(authorConfidence.toFixed(2)),
      source: "web",
      _diag: debug ? { author: diagAuthor, life, site: diagSite } : undefined,
    });
  } catch (err: unknown) {
    const message = (err as Error)?.message || "internal_error";
    console.error("handler_error", message);
    return res.status(500).json({ error: message });
  }
}
