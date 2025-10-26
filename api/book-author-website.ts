// api/book-author-website.ts
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

function jaccard(a: string[], b: string[]) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}

/* =============================
   Name heuristics
   ============================= */
const MIDDLE_PARTICLES = new Set(["de","del","de la","di","da","dos","das","do","van","von","bin","ibn","al","el","le","la","du","st","saint","mc","mac","ap"]);
const GENERATIONAL_SUFFIX = new Set(["jr","jr.","sr","sr.","ii","iii","iv","v"]);
const BAD_TAIL = new Set(["frankly","review","reviews","opinion","analysis","explainer","interview","podcast","video","transcript","essay","profile","biography","news","guide","column","commentary","editorial","update","thread","blog"]);

function isCasedWordToken(tok: string): boolean {
  return /^[A-Z][\p{L}’'-]*[A-Za-z]$/u.test(tok);
}
function isSuffix(tok: string): boolean {
  return GENERATIONAL_SUFFIX.has(tok.toLowerCase());
}
function looksLikeAdverb(tok: string): boolean {
  return /^[A-Za-z]{3,}ly$/u.test(tok);
}

function nameLikeness(raw: string): number {
  const s = raw.trim().replace(/\s+/g, " ");
  if (!s) return 0;
  const partsOrig = s.split(" ");
  const parts: string[] = [];
  for (let i = 0; i < partsOrig.length; i++) {
    const cur = partsOrig[i];
    const next = partsOrig[i + 1]?.toLowerCase();
    if (i + 1 < partsOrig.length && cur.toLowerCase() === "de" && next === "la") {
      parts.push("de la"); i++; continue;
    }
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
  if (BAD_TAIL.has(last.toLowerCase())) score -= 0.7;
  if (!lastIsSuffix && looksLikeAdverb(last)) score -= 0.6;
  if (/\d/.test(s)) score -= 0.8;

  if (parts.length === 2) score += 0.10;
  if (parts.length === 3) score += 0.12;
  if (parts.length === 4) score += 0.05;

  return Math.max(0, Math.min(1, score));
}

function normalizeNameCandidate(raw: string, title: string): string | null {
  let s = raw.trim().replace(/[)\]]+$/g, "").replace(/[.,;:–—-]+$/g, "").replace(/\s+/g, " ");
  if (tokens(title).includes(s.toLowerCase())) return null;
  const parts = s.split(" ");
  const last = parts[parts.length - 1];
  if (BAD_TAIL.has(last.toLowerCase()) || looksLikeAdverb(last)) {
    if (parts.length >= 3) {
      parts.pop(); s = parts.join(" ");
    }
  }
  if (parts.length === 2 && looksLikeCommonPair(s)) return null;
  return nameLikeness(s) >= 0.65 ? s : null;
}

/* =============================
   Common pair guard
   ============================= */
const COMMON_TITLE_WORDS = new Set([
  "Change","Transitions","Transition","Moving","Forward","Next","Day","The",
  "Future","Guide","How","Policy","Center","Office","Press","News","Support"
]);
function looksLikeCommonPair(name: string): boolean {
  const toks = name.split(/\s+/);
  if (toks.length !== 2) return false;
  return toks.every(t => COMMON_TITLE_WORDS.has(t));
}

/* =============================
   Extract names
   ============================= */
type Signal = "author_label" | "wrote_book" | "byline" | "plain_last_first";
function extractNamesFromText(text: string, opts?: { booky?: boolean }): Array<{ name: string; signal: Signal }> {
  const booky = !!opts?.booky;
  const out: Array<{ name: string; signal: Signal }> = [];
  const patterns: Array<{ re: RegExp; signal: Signal; swap?: boolean }> = [
    { re: /\bAuthor:\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/gu, signal: "author_label" },
    { re: /\bwritten by\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/gu, signal: "author_label" },
    { re: /\b([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s+(?:wrote|writes)\s+the\s+book\b/gu, signal: "wrote_book" },
    { re: /\bby\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/gu, signal: "byline" },
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
      const name = swap ? `${m[2]} ${m[1]}` : m[1];
      out.push({ name: name.trim(), signal });
    }
  }
  return out;
}

/* =============================
   Resolve author
   ============================= */
async function resolveAuthor(bookTitle: string): Promise<{ name: string|null, confidence: number, debug: any }> {
  const query = `"${bookTitle}" book written by -film -movie -screenplay -soundtrack -director`;
  const items = await googleCSE(query, 10);

  const candidates: Record<string, number> = {};
  const highSignalSeen: Record<string, number> = {};

  for (const it of items) {
    const fields: string[] = [];
    if (it.title) fields.push(it.title);
    if (it.snippet) fields.push(it.snippet);
    const mtList = Array.isArray(it.pagemap?.metatags) ? it.pagemap.metatags : [];
    let mtIsBook = false;
    for (const mt of mtList) {
      if (typeof mt === "object") {
        if ((mt as any)["og:type"]?.toLowerCase().includes("book")) mtIsBook = true;
      }
    }
    let host = "";
    try { host = new URL(it.link).host.toLowerCase(); } catch {}

    const haystack = fields.join(" • ");
    const matches = extractNamesFromText(haystack, { booky: mtIsBook });

    for (const { name, signal } of matches) {
      const norm = normalizeNameCandidate(name, bookTitle);
      if (!norm) continue;
      let w: number;
      switch (signal) {
        case "author_label": w = 3.0; break;
        case "wrote_book": w = 2.0; break;
        case "byline": w = 1.0; break;
        case "plain_last_first": w = 0.6; break;
        default: w = 1.0;
      }
      if ((signal === "byline" || signal === "plain_last_first") &&
          (looksLikeCommonPair(norm))) continue;
      candidates[norm] = (candidates[norm] || 0) + w;
      if (signal === "author_label" || signal === "wrote_book") {
        highSignalSeen[norm] = (highSignalSeen[norm] || 0) + 1;
      }
    }
  }

  const entries = Object.entries(candidates);
  if (!entries.length) return { name: null, confidence: 0, debug: { query, items, candidates } };

  // Pick winner preferring high-signal
  const merged = entries.map(([name, score]) => ({ name, score }));
  merged.sort((a,b) => b.score - a.score);
  let pick = merged[0];
  const withHigh = merged.filter(m => (highSignalSeen[m.name]||0) > 0);
  if (withHigh.length) {
    const bestHigh = withHigh[0];
    if ((highSignalSeen[pick.name]||0)===0 && pick.score < 1.5*bestHigh.score) {
      pick = bestHigh;
    }
  }
  const confidence = pick.score >= 2 ? 0.95 : 0.8;
  return { name: pick.name, confidence, debug: { query, items, candidates, merged, highSignalSeen, picked: pick.name } };
}

/* =============================
   Website resolution (unchanged except prioritization)
   ============================= */
const BLOCKED_DOMAINS = ["wikipedia.org","goodreads.com","google.com","amazon.","barnesandnoble.com","bookshop.org"];
function isBlockedHost(host: string): boolean {
  return BLOCKED_DOMAINS.some((d) => host.includes(d));
}
async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
  const queries = [
    `"${author}" author website`,
    `"${author}" official site`,
    `"${bookTitle}" "${author}" author website`
  ];
  for (const q of queries) {
    const items = await googleCSE(q, 10);
    const filtered = items.filter(it => {
      try { return !isBlockedHost(new URL(it.link).host.toLowerCase()); }
      catch { return false; }
    });
    if (filtered.length) {
      const top = filtered[0];
      return { url: new URL(top.link).origin, debug: { query: q, picked: top, items: filtered.map(i=>i.link) } };
    }
  }
  return { url: null, debug: { tried: queries } };
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
      return res.status(200).json({ book_title: bookTitle, inferred_author: null, confidence: 0, error: "no_author_found", _diag: authorRes.debug });
    }
    const siteRes = await resolveWebsite(authorRes.name, bookTitle);
    return res.status(200).json({
      book_title: bookTitle,
      inferred_author: authorRes.name,
      author_confidence: authorRes.confidence,
      author_url: siteRes.url,
      confidence: siteRes.url ? 0.9 : 0,
      _diag: { author: authorRes.debug, site: siteRes.debug }
    });
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
