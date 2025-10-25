// api/book-author-website.ts
/// <reference types="node" />

/**
 * Deterministic, two-phase resolver:
 *  Phase 1:  "<BOOK_TITLE>" book author  → parse author from Wikipedia / Google Books / Goodreads (consensus + person-shape)
 *  Phase 2:  "<AUTHOR>" "<BOOK_TITLE>" author website → greedy accept: books-domain → vanity official → (optional) publisher author-profile
 *
 * Input JSON:
 * {
 *   "book_title": "A Promised Land",
 *   "include_search": true,               // default true (needs GOOGLE_CSE_KEY + GOOGLE_CSE_ID)
 *   "allow_estate_sites": false,          // default false
 *   "exclude_publisher_sites": true,      // default true
 *   "debug": true                         // optional
 * }
 *
 * Output: { book_title, inferred_author, pub_year, life_dates, author_viable, viability_reason, author_url, site_title, canonical_url, author_confidence, confidence, _diag? }
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
const USER_AGENT = "BookAuthorWebsite/3.0 (+https://example.com)";
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
async function googleCSE(query: string, num = 10): Promise<any[]> {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(Math.max(1, Math.min(10, num))));

  const resp = await fetchWithTimeout(url.toString());
  const text = await resp.text();
  if (!resp?.ok) throw new Error(`cse_http_${resp?.status}: ${text?.slice(0, 500)}`);
  const data: any = JSON.parse(text);
  return Array.isArray(data?.items) ? data.items : [];
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
  const page = items.find((it: any) => /en\.wikipedia\.org\/wiki\//i.test(it?.link || ""));
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
  const page = items.find((it: any) => /books\.google\.com\/books/i.test(it?.link || ""));
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
  const page = items.find((it: any) => /goodreads\.com\/book\/show/i.test(it?.link || ""));
  if (!page) return [];
  const r = await fetchWithTimeout(page.link);
  if (!r?.ok) return [];
  const html = await r.text();
  const area = html.slice(0, 200_000);

  // Try "by <a>Author Name</a>"
  const m = area.match(/\bby\s+<a[^>]*>([^<]{2,120})<\/a>/i)?.[1] || "";
  const txt = m || area.match(/\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\b/u)?.[1] || "";
  const names = txt ? [sanitizeName(txt)] : [];
  return names.filter(Boolean);
}

function pickAuthorDeterministic(title: string, wiki: string[], gbooks: string[], goodreads: string[]) {
  // Normalize, filter by person-shape
  const candidates = new Map<string, { sources: Set<string> }>();

  const push = (name: string, src: string) => {
    const clean = sanitizeName(name);
    if (!clean || !looksLikePersonName(clean, title)) return;
    const key = clean;
    if (!candidates.has(key)) candidates.set(key, { sources: new Set() });
    candidates.get(key)!.sources.add(src);
  };

  wiki.forEach((n) => push(n, "wikipedia"));
  gbooks.forEach((n) => push(n, "googlebooks"));
  goodreads.forEach((n) => push(n, "goodreads"));

  // Consensus rule
  let best: { name: string; confidence: number; sources: string[] } | null = null;

  for (const [name, { sources }] of candidates.entries()) {
    const srcs = Array.from(sources);
    let confidence = 0;
    if (srcs.includes("wikipedia") && srcs.includes("googlebooks")) confidence = 0.95;
    else if (srcs.includes("wikipedia") && srcs.includes("goodreads")) confidence = 0.9;
    else if (srcs.includes("googlebooks") && srcs.includes("goodreads")) confidence = 0.85;
    else if (srcs.length === 1) confidence = 0.8;

    if (!best || confidence > best.confidence) {
      best = { name, confidence, sources: srcs };
    }
  }
  return best;
}

/* =============================
   Life dates (for viability) from Author Wikipedia
   ============================= */
async function authorLifeDatesFromWikipedia(authorName: string): Promise<{ birthYear: number | null; deathYear: number | null; wikiUrl?: string | null }> {
  const q = `"${authorName}" site:en.wikipedia.org`;
  const items = await googleCSE(q);
  const page = items.find((it: any) => /en\.wikipedia\.org\/wiki\//i.test(it?.link || ""));
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
   Phase 2: deterministic website resolution
   ============================= */

const RETAIL_OR_SOCIAL = [
  "amazon.", "bookshop.org", "barnesandnoble.com", "audible.", "itunes.apple.com",
  "goodreads.com", "x.com", "twitter.com", "facebook.com", "instagram.com", "tiktok.com", "linkedin.com", "imdb.com"
];

function isRetailOrSocialHost(host: string): boolean {
  const h = host.toLowerCase();
  return RETAIL_OR_SOCIAL.some((bad) => h.includes(bad));
}

function looksLikeAuthorBooksDomain(url: string, authorName: string): boolean {
  try {
    const u = new URL(url);
    const host = u.host.replace(/^www\./, "").toLowerCase();
    const key = authorName.toLowerCase().replace(/\s+/g, "");
    if (host.includes(key) && host.includes("book")) return true;     // e.g., barackobamabooks.com
    if (host.startsWith("books.") && host.includes(key)) return true; // books.authorname.com
    return false;
  } catch { return false; }
}

function looksLikeBooksSection(url: string, authorName: string): boolean {
  try {
    const u = new URL(url);
    const path = u.pathname.toLowerCase();
    return /\/(books|works|bibliography)(\/|$)/.test(path);
  } catch { return false; }
}

async function validateBooksPage(url: string, authorName: string, bookTitle: string) {
  try {
    const origin = originOf(url) ?? url;
    const r = await fetchWithTimeout(origin);
    if (!r?.ok) return null;
    const html = await r.text();
    const lower = html.toLowerCase();
    const title = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1] || "";
    const h1 = /<h1[^>]*>([^<]{2,200})<\/h1>/i.exec(html)?.[1] || "";
    const titleH1 = tokens(`${title} ${h1}`);
    const authorTok = tokens(authorName);
    const bookTok = tokens(bookTitle);

    const authorNamed = containsAll(titleH1, authorTok) || jaccard(titleH1, authorTok) >= 0.35;

    const bookMentioned =
      containsAll(tokens(html), bookTok) || jaccard(tokens(html), bookTok) >= 0.20;

    const hasBooksNav = /\b(books|works|bibliography)\b/i.test(lower);
    const showsMultipleWorks =
      (lower.match(/\bbook(s)?\b/g) || []).length >= 3 ||
      (html.match(/<img[^>]+(cover|book|jacket)[^>]*>/gi) || []).length >= 2 ||
      (html.match(/<a[^>]+>([^<]{2,80})<\/a>/gi) || []).filter(a => /book|novel|memoir|essays/i.test(a)).length >= 2;

    // simple civic/campaign deny
    const civicNoise = /\b(whitehouse|house\.gov|senate|campaign|donate)\b/i.test(lower);
    if (civicNoise) return null;

    return { origin, siteTitle: title.trim() || null, authorNamed, bookMentioned, hasBooksNav, showsMultipleWorks, html };
  } catch {
    return null;
  }
}

function isBareHomepage(url: string): boolean {
  try {
    const u = new URL(url);
    const p = u.pathname.replace(/\/+$/, "");
    return p === "" || p === "/";
  } catch { return false; }
}

function pickCanonicalOrOrigin(html: string, url: string, authorName: string): string {
  const origin = originOf(url) || url;
  const m = html.match(/<link[^>]+rel=["']canonical["'][^>]*>/i)?.[0] || "";
  const href = /href=["']([^"']+)["']/i.exec(m)?.[1] || null;
  if (!href) return origin;
  const canon = isHttpUrl(href) ? href : new URL(href, origin).toString();

  const hostCanon = hostOf(canon).replace(/^www\./, "");
  const hostOrig  = hostOf(origin).replace(/^www\./, "");
  const authorKey = authorName.toLowerCase().replace(/\s+/g, "");
  const canonVanity = hostCanon.includes(authorKey);
  const origVanity  = hostOrig.includes(authorKey);

  if (canonVanity && !origVanity) return canon;
  if (canonVanity && origVanity) return canon;
  if (isBareHomepage(canon)) return origin;
  return canon;
}

function buildPhase2Queries(authorName: string, bookTitle: string): string[] {
  const qA = `"${authorName}"`;
  const qB = `"${bookTitle}"`;
  const negatives = "-film -movie -director -trailer -whitehouse -campaign -donate";
  return [
    `${qA} ${qB} ("author website" OR "official site" OR "official website") ${negatives}`,
    `${qA} ("author website" OR "official site" OR "official website") ${negatives}`
  ];
}

async function resolveWebsiteDeterministic(
  authorName: string,
  bookTitle: string,
  excludePublisherSites: boolean
): Promise<{ url: string; siteTitle: string | null; reason: string; html?: string } | null> {
  const queries = buildPhase2Queries(authorName, bookTitle);
  const allItems = (await Promise.all(queries.map(q => limit(() => googleCSE(q))))).flat();
  const urls = allItems.map((it: any) => String(it.link || "")).filter(isHttpUrl);
  const deduped = dedupeByETLD1(urls);

  // Greedy pass 1: author-books domain
  for (const u of deduped) {
    if (!looksLikeAuthorBooksDomain(u, authorName)) continue;
    if (isRetailOrSocialHost(hostOf(u))) continue;
    const ok = await validateBooksPage(u, authorName, bookTitle);
    if (ok && ok.authorNamed && (ok.bookMentioned || ok.hasBooksNav || ok.showsMultipleWorks)) {
      return { url: ok.origin, siteTitle: ok.siteTitle, reason: "author_books_domain", html: ok.html };
    }
  }

  // Greedy pass 2: vanity + /books|/works|/bibliography path
  for (const u of deduped) {
    if (!looksLikeBooksSection(u, authorName)) continue;
    if (isRetailOrSocialHost(hostOf(u))) continue;
    const ok = await validateBooksPage(u, authorName, bookTitle);
    if (ok && ok.authorNamed && (ok.bookMentioned || ok.showsMultipleWorks || ok.hasBooksNav)) {
      return { url: ok.origin, siteTitle: ok.siteTitle, reason: "vanity_books_section", html: ok.html };
    }
  }

  // Greedy pass 3: clean vanity personal site that mentions the book OR has books nav
  for (const u of deduped) {
    const host = hostOf(u);
    if (isRetailOrSocialHost(host)) continue;

    const compact = authorName.toLowerCase().replace(/\s+/g, "");
    const vanityish = host.replace(/^www\./, "").includes(compact);
    if (!vanityish) continue;

    const ok = await validateBooksPage(u, authorName, bookTitle);
    if (ok && ok.authorNamed && (ok.bookMentioned || ok.hasBooksNav)) {
      return { url: ok.origin, siteTitle: ok.siteTitle, reason: "vanity_official", html: ok.html };
    }
  }

  // Optional fallback: publisher author-profile (never homepage)
  if (!excludePublisherSites) {
    for (const u of deduped) {
      const host = hostOf(u);
      if (/penguinrandomhouse\.com|harpercollins\.com|simonandschuster\.com|macmillan\.com|hachettebookgroup\.com/i.test(host)) {
        if (isRetailOrSocialHost(host)) continue;
        if (isBareHomepage(u)) continue;
        const ok = await validateBooksPage(u, authorName, bookTitle);
        if (ok && ok.authorNamed && ok.bookMentioned) {
          return { url: ok.origin, siteTitle: ok.siteTitle, reason: "publisher_author_profile", html: ok.html };
        }
      }
    }
  }

  return null;
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

    // Run all three sources (deterministic consensus)
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

    // pub_year (prefer Google Books; else try Wikipedia book page year already implicit in gbInfo)
    const pubYear = gbInfo.year ?? null;

    // life dates for viability
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

    /* ========= Phase 2: author+book → writing website (deterministic) ========= */
    const diagSite: any = { queries: buildPhase2Queries(authorName, bookTitle), picked: null };

    const site = await resolveWebsiteDeterministic(authorName, bookTitle, excludePublisherSites);

    if (!site) {
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

    // canonical/origin preference
    let canonical = site.url;
    let siteTitle = site.siteTitle;
    try {
      if (site.html) {
        canonical = pickCanonicalOrOrigin(site.html, site.url, authorName);
        if (!siteTitle) siteTitle = /<title[^>]*>([^<]*)<\/title>/i.exec(site.html)?.[1]?.trim() || null;
      } else {
        const r = await fetchWithTimeout(site.url);
        if (r?.ok) {
          const html = await r.text();
          canonical = pickCanonicalOrOrigin(html, site.url, authorName);
          siteTitle = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1]?.trim() || null;
        }
      }
    } catch { /* ignore */ }

    diagSite.picked = { url: site.url, canonical, reason: site.reason };

    // Confidence is deterministic by path that accepted it
    const confidence = site.reason === "author_books_domain" ? 0.9
                     : site.reason === "vanity_books_section" ? 0.88
                     : site.reason === "vanity_official" ? 0.85
                     : site.reason === "publisher_author_profile" ? 0.75
                     : 0.8;

    return res.status(200).json({
      book_title: bookTitle,
      inferred_author: authorName,
      pub_year: pubYear,
      life_dates: { birthYear: life.birthYear, deathYear: life.deathYear },
      author_viable: true,
      viability_reason: viability.reason,
      author_url: site.url,
      site_title: siteTitle,
      canonical_url: canonical,
      confidence: Number(confidence.toFixed(2)),
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
