// api/book-author-website.ts
/// <reference types="node" />

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
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", query);
  url.searchParams.set("num", String(num));

  const resp = await fetchWithTimeout(url.toString());
  if (!resp?.ok) {
    const text = await resp?.text?.();
    throw new Error(`cse_http_${resp?.status}: ${text || ""}`.slice(0, 240));
  }
  const data: any = await resp.json();
  return Array.isArray(data?.items) ? data.items : [];
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

/** NEW: role-tail words that should never be part of a person’s name */
const ROLE_TAIL_TOKENS = new Set([
  "illustrated","illustrator","illustrations",
  "edited","editor","editors",
  "foreword","afterword","introduction","intro",
  "translated","translator","translators"
]);

/** NEW: honorifics and post-nominals commonly glued to names */
const HONORIFICS = new Set([
  "mr","mrs","ms","miss","mx","dr","prof","prof.","sir","dame","lord","lady","rev","rabbi","imam"
]);
const DEGREE_OR_POSTNOMINAL = new Set([
  "phd","ph.d.","md","m.d.","mba","jd","j.d.","mph","mphil","m.phil.",
  "obe","cbe","dbe","kbe","frs","fba","frsl","frhists","dlitt","dphil","d.phil."
]);

/** NEW: prepositional/connector tails that usually start a clause after a name */
const NAME_FOLLOW_TAIL = new Set([
  "on","at","for","from","about","regarding","with","via","in","of","by","speaks","interview","talks","discusses"
]);

function stripTrailingGarbage(s: string): string {
  let parts = s.split(/\s+/);
  let changed = false;
  while (parts.length > 1) {
    const last = parts[parts.length - 1].toLowerCase();
    if (
      NAME_TAIL_GARBAGE.has(last) ||
      BAD_TAIL.has(last) ||
      looksLikeAdverb(last) ||
      ROLE_TAIL_TOKENS.has(last)
    ) {
      parts.pop();
      changed = true;
      continue;
    }
    break;
  }
  return changed ? parts.join(" ") : s;
}

/** Additional cleanup for role artifacts inside extracted names */
function cleanRoleArtifacts(name: string): string {
  let s = name.replace(/\b(Illustrated|Illustrator|Edited|Editor|Foreword|Afterword|Introduction|Intro|Translated|Translator)s?\b$/i, "").trim();
  // Also trim accidental mid-name connectors like trailing commas or dangles
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
   NEW: stronger clause/paren/honorific trimming for names
   ============================= */
function stripTrailingParenBlock(s: string): string {
  return s.replace(/\s*[\(\[\{][^()\[\]{}]{0,80}[\)\]\}]\s*$/u, "").trim();
}

function stripParentheticalSuffixes(s: string): string {
  let prev: string;
  let cur = s.trim();
  do {
    prev = cur;
    cur = stripTrailingParenBlock(cur);
  } while (cur !== prev);
  return cur;
}

function stripClauseAfterPunct(name: string): string {
  let s = name.replace(/\s+[–—-]\s+(\S.*)$/u, (m, tail) => {
    const first = String(tail).split(/\s+/)[0] || "";
    if (NAME_FOLLOW_TAIL.has(first.toLowerCase())) return "";
    if (/^[a-z]/.test(first)) return "";
    return m;
  }).trim();

  s = s.replace(/\s*,\s+(\S.*)$/u, (m, tail) => {
    const first = String(tail).split(/\s+/)[0] || "";
    if (NAME_FOLLOW_TAIL.has(first.toLowerCase())) return "";
    if (/^(author|writer|editor|journalist|historian|novelist)\b/i.test(tail)) return "";
    return m;
  }).trim();

  return s;
}

function stripHonorificsAndPostnominals(s: string): string {
  let parts = s.split(/\s+/).filter(Boolean);
  while (parts.length > 0 && HONORIFICS.has(parts[0].toLowerCase().replace(/\.$/, ""))) {
    parts.shift();
  }
  while (parts.length > 1) {
    const last = parts[parts.length - 1];
    const low = last.toLowerCase();
    if (isSuffix(last)) break;
    if (DEGREE_OR_POSTNOMINAL.has(low.replace(/\.$/, ""))) { parts.pop(); continue; }
    if (/^(?:19|20)\d{2}$/.test(low) || /^@\w{2,}$/.test(last)) { parts.pop(); continue; }
    break;
  }
  return parts.join(" ");
}

function stripTrailingGarbageStrong(s: string): string {
  let out = s.trim();
  out = stripParentheticalSuffixes(out);
  out = stripClauseAfterPunct(out);
  out = stripHonorificsAndPostnominals(out);
  out = stripTrailingGarbage(out);    // existing trimming of garbage/adverbs/roles
  out = stripTrailingParenBlock(out); // one more safety pass
  return out.trim();
}

function trimToBestNamePrefix(s: string): string {
  const parts = s.split(/\s+/).filter(Boolean);
  const maxLen = Math.min(6, parts.length);
  let best = s;
  let bestScore = nameLikeness(s);

  for (let len = 2; len <= maxLen; len++) {
    const cand = parts.slice(0, len).join(" ");
    const sc = nameLikeness(cand);
    if (sc > bestScore + 0.04 || (Math.abs(sc - bestScore) <= 0.04 && cand.split(" ").length > best.split(" ").length)) {
      best = cand;
      bestScore = sc;
    }
  }
  return best;
}

/* =============================
   Clean + validate a candidate
   ============================= */
function normalizeNameCandidate(raw: string, title: string): string | null {
  let s = raw.trim()
    .replace(/[)\]]+$/g, "")
    .replace(/[.,;:–—-]+$/g, "")
    .replace(/\s+/g, " ");

  // scrub token punctuation
  s = s.split(/\s+/).map(tok => tok.replace(/[.,;:]+$/g, "")).join(" ");

  // NEW stronger cleanup sequence
  s = cleanRoleArtifacts(s);
  s = stripTrailingGarbageStrong(s);
  s = trimToBestNamePrefix(s);

  // Hard reject organizations / imprints
  if (isOrgishName(s)) return null;

  // Early reject for common-word pairs (subtitle artifacts) and title echoes
  const partsEarly = s.split(/\s+/);
  const titleToks = tokens(title);
  if (partsEarly.length === 2 && looksLikeCommonPair(s)) return null;
  if (titleToks.includes(s.toLowerCase())) return null;
  if (partsEarly.length === 2) {
    const overlap = partsEarly.filter(p => titleToks.includes(p.toLowerCase())).length;
    if (overlap === 2) return null;
  }

  // Second pass strong strip + org re-check
  s = stripTrailingGarbageStrong(s);
  if (isOrgishName(s)) return null;

  return nameLikeness(s) >= 0.65 ? s : null;
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
  let bestHost: string | null = null;
  const evidence: any[] = []

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

  return {
    full: bestFull || shortTitle,
    usedShort: !bestFull,
    debug: { q, bestFull, evidence }
  };
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

function extractAuthorSignals(text: string, opts?: { booky?: boolean; title?: string }): AuthorSignal[] {
  const out: AuthorSignal[] = [];
  const booky = !!opts?.booky;
  const title = opts?.title || "";

  // Author / written by
  const reAuthor = /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=[),.?:;!–—-]|\s|$)/gu;
  const reWritten = /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})(?=[),.?:;!–—-]|\s|$)/gu;

  // by A, B and C [with D]
  const reBylineBlock = /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*(?:\s*(?:,?\s*and\s+|&\s*)[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})?)(?:\s+with\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*)\b)?/gu;

  // role-specific
  const reEditedBy = /\bedited by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*(?:\s*(?:,?\s*and\s+|&\s*)[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})?)/gu;
  const reForewordBy = /\bforeword by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/gu;
  const reIllustratedBy = /\billustrated by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}(?:\s*,\s*[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})*(?:\s*(?:,?\s*and\s+|&\s*)[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})?)/gu;

  // optional: plain "Last, First" if page is booky
  const reLastFirst = /\b([A-Z][\p{L}'-]+),\s+([A-Z][\p{L}'-]+)\b(?!\s*(?:and|or|&|,))/gu;

  let m: RegExpExecArray | null;

  while ((m = reAuthor.exec(text))) {
    out.push({ kind: "author_label", name: cleanRoleArtifacts(m[1]) });
  }
  while ((m = reWritten.exec(text))) {
    out.push({ kind: "written_by", name: cleanRoleArtifacts(m[1]) });
  }
  while ((m = reBylineBlock.exec(text))) {
    const list = splitNamesList(m[1]).map(cleanRoleArtifacts).map(stripTrailingGarbage).filter(Boolean);
    list.forEach((n, i) => out.push({ kind: "byline_ordered", name: n, position: i }));
    if (m[2]) {
      const withList = splitNamesList(m[2]).map(cleanRoleArtifacts).map(stripTrailingGarbage).filter(Boolean);
      withList.forEach((n) => out.push({ kind: "with", name: n }));
    }
  }
  while ((m = reEditedBy.exec(text))) {
    splitNamesList(m[1]).map(cleanRoleArtifacts).map(stripTrailingGarbage).filter(Boolean)
      .forEach((n) => out.push({ kind: "editor", name: n }));
  }
  while ((m = reForewordBy.exec(text))) {
    out.push({ kind: "foreword", name: cleanRoleArtifacts(m[1]) });
  }
  while ((m = reIllustratedBy.exec(text))) {
    splitNamesList(m[1]).map(cleanRoleArtifacts).map(stripTrailingGarbage).filter(Boolean)
      .forEach((n) => out.push({ kind: "illustrator", name: n }));
  }

  if (booky) {
    while ((m = reLastFirst.exec(text))) {
      const name = `${m[2]} ${m[1]}`;
      out.push({ kind: "plain_last_first", name: cleanRoleArtifacts(name) });
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

  // Merge short/long variants by first two tokens, prefer longest
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
    arr.sort((a, b) => {
      const al = a.name.split(/\s+/).length, bl = b.name.split(/\s+/).length;
      if (al !== bl) return bl - al;
      if (a.score !== b.score) return b.score - a.score;
      return a.name.localeCompare(b.name);
    });
    const longest = arr[0].name;
    const longLower = longest.toLowerCase();
    let score = 0;
    let high = 0;
    for (const e of arr) {
      const nLower = e.name.toLowerCase();
      if (longLower.startsWith(nLower) || nLower.startsWith(longLower)) { score += e.score; high += e.high; }
    }
    merged.push({ name: longest, score, high });
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

const MAX_BYTES = 400_000; // ~400KB safety cap to avoid huge pages

async function fetchPageText(url: string, ms = 5500): Promise<string> {
  try {
    const resp = await fetchWithTimeout(url, { headers: { Accept: "text/html,*/*" } }, ms);
    if (!resp?.ok) return "";
    // stream and cap
    const reader = (resp as any).body?.getReader?.();
    if (!reader) return extractVisibleText(await resp.text());
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
    return extractVisibleText(buf);
  } catch {
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

  // JSON-LD blobs
  const jsonLdMatches = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const block of jsonLdMatches) {
    const inner = block.match(/<script[^>]*>([\s\S]*?)<\/script>/i)?.[1];
    if (inner) metaBits.push(inner);
  }

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

/** Authorship-focused boosts */
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

  const hasBooksPage = /\b(books|publications|titles|works)\b/i.test(text) ? 1 : 0;
  const mentionsAuthorRole = /\b(author|writer|written by|byline)\b/i.test(text) ? 1 : 0;

  const { person, book } = hasJsonLdPersonOrBook(text, author, bookTitle);

  let score =
    0.40 * Math.min(1, bookOverlap * 1.5) +
    0.18 * aExact +
    0.12 * hasBooksPage +
    0.10 * mentionsAuthorRole +
    0.12 * (person ? 1 : 0) +
    0.08 * (book ? 1 : 0);

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

async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
  const queries = [
    `"${author}" official site`,
    `"${author}" author website`,
    `"${bookTitle}" "${author}" author website`,
  ];

  const candidates: Array<{ urlObj: URL; urlScore: number; fromQuery: string }> = [];
  for (const q of queries) {
    const items = await googleCSE(q, 10);
    for (const it of items) {
      let u: URL | null = null;
      try { u = new URL(it.link); } catch { continue; }
      const host = u.host.toLowerCase();
      if (isBlockedHost(host)) continue;
      const urlScore = scoreCandidateUrl(u, author);
      if (!candidates.some(c => c.urlObj.host === u!.host)) {
        candidates.push({ urlObj: u, urlScore, fromQuery: q });
      }
    }
  }

  if (!candidates.length) {
    return { url: null, debug: { tried: "broad_author_query", threshold: WEBSITE_MIN_SCORE } };
  }

  const topFew = candidates
    .sort((a, b) => b.urlScore - a.urlScore)
    .slice(0, 5);

  const validations: Array<{
    origin: string;
    urlScore: number;
    contentScore: number;
    finalScore: number;
    samples: Array<{ path: string; contentScore: number }>;
    fromQuery: string;
  }> = [];

  for (const c of topFew) {
    const origin = c.urlObj.origin;
    const paths = ["", "/books", "/book", "/works", "/publications", "/titles", "/about", "/bio"];
    const samples: Array<{ path: string; contentScore: number }> = [];

    const homeText = await fetchPageText(origin);
    let bestContent = contentSignalsForSite(homeText, author, bookTitle);
    samples.push({ path: "/", contentScore: bestContent });

    for (const p of paths.slice(1)) {
      if (bestContent >= 0.85) break;
      const text = await fetchPageText(origin + p);
      const s = contentSignalsForSite(text, author, bookTitle);
      bestContent = Math.max(bestContent, s);
      samples.push({ path: p, contentScore: s });
    }

    if (bestContent < 0.5) {
      const siteQuery = `"${bookTitle}" site:${c.urlObj.host}`;
      const siteHits = await googleCSE(siteQuery, 3);
      const firstHit = siteHits?.find(h => {
        try { return new URL(h.link).host === c.urlObj.host; } catch { return false; }
      });
      if (firstHit) {
        const t = await fetchPageText(firstHit.link);
        const s = contentSignalsForSite(t, author, bookTitle);
        bestContent = Math.max(bestContent, s);
        samples.push({ path: new URL(firstHit.link).pathname, contentScore: s });
      }
    }

    const finalScore = 0.45 * c.urlScore + 0.55 * bestContent;
    validations.push({
      origin,
      urlScore: c.urlScore,
      contentScore: bestContent,
      finalScore,
      samples,
      fromQuery: c.fromQuery,
    });
  }

  validations.sort((a, b) => b.finalScore - a.finalScore);
  const top = validations[0];

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
      }
    };
  }

  return {
    url: null,
    debug: { threshold, candidates: validations.slice(0, 5) }
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
    if (!authorRes.primary) {
      return res.status(200).json({
        book_title: bookTitle,
        primary_author: null,
        co_authors: [],
        inferred_author: null,
        author_confidence: 0,
        author_url: null,
        confidence: 0,
        error: "no_author_found",
        _diag: { flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID, WEBSITE_MIN_SCORE }, author: authorRes.debug }
      });
    }

    const siteRes = await resolveWebsite(authorRes.primary, bookTitle);

    return res.status(200).json({
      book_title: bookTitle,
      primary_author: authorRes.primary,
      co_authors: authorRes.coAuthors,
      authors_ranked: authorRes.ranked,
      inferred_author: authorRes.primary,          // backward-compat
      author_confidence: authorRes.confidence,     // backward-compat
      author_url: siteRes.url,
      confidence: siteRes.url ? 0.9 : 0,
      _diag: {
        flags: { USE_SEARCH, CSE_KEY: !!CSE_KEY, CSE_ID: !!CSE_ID, WEBSITE_MIN_SCORE },
        author: authorRes.debug,
        site: siteRes.debug
      }
    });
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
