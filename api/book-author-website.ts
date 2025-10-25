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

function looksLikePersonName(name: string, title: string): boolean {
  if (!name) return false;
  const parts = name.split(/\s+/);
  if (parts.length < 1 || parts.length > 4) return false;
  const bad = ["press","publishing","amazon","goodreads","google","book","books"];
  if (bad.some((b) => name.toLowerCase().includes(b))) return false;
  if (tokens(title).includes(name.toLowerCase())) return false;
  return /^[A-Z]/.test(parts[0]);
}

function extractNamesFromText(text: string): string[] {
  const patterns = [
    /\bby ([A-Z][\p{L}'-]+(?: [A-Z][\p{L}'-]+){0,3})/gu,
    /\bAuthor: ([A-Z][\p{L}'-]+(?: [A-Z][\p{L}'-]+){0,3})/gu,
    /\bwritten by ([A-Z][\p{L}'-]+(?: [A-Z][\p{L}'-]+){0,3})/gu,
  ];
  const names: string[] = [];
  for (const pat of patterns) {
    let m;
    while ((m = pat.exec(text))) {
      names.push(m[1].trim());
    }
  }
  return names;
}

/* =============================
   Phase 1: Determine Author
   ============================= */
async function resolveAuthor(bookTitle: string): Promise<{ name: string|null, confidence: number, debug: any }> {
  const query = `"${bookTitle}" book author -film -movie -screenplay -soundtrack -director`;
  const items = await googleCSE(query, 10);

  const candidates: Record<string, number> = {};
  for (const it of items) {
    const text = `${it.title} ${it.snippet || ""}`;
    for (const n of extractNamesFromText(text)) {
      if (looksLikePersonName(n, bookTitle)) {
        candidates[n] = (candidates[n] || 0) + 1;
      }
    }
  }

  const sorted = Object.entries(candidates).sort((a,b) => b[1]-a[1]);
  if (sorted.length === 0) {
    return { name: null, confidence: 0, debug: { query, items, candidates } };
  }
  const [best, count] = sorted[0];
  const confidence = count >= 2 ? 0.95 : 0.8;
  return { name: best, confidence, debug: { query, items, candidates, picked: best } };
}

/* =============================
   Phase 2: Resolve Website (unchanged from your deterministic refactor)
   ============================= */
const BLOCKED_DOMAINS = [
  "wikipedia.org","goodreads.com","google.com","books.google.com",
  "amazon.","barnesandnoble.com","bookshop.org","penguinrandomhouse.com",
  "harpercollins.com","simonandschuster.com","macmillan.com","hachettebookgroup.com"
];

function isBlockedHost(host: string): boolean {
  return BLOCKED_DOMAINS.some((d) => host.includes(d));
}

async function resolveWebsite(author: string, bookTitle: string): Promise<{ url: string|null, debug: any }> {
  const queries = [
    `"${bookTitle}" "${author}" author website`,
    `"${author}" official site`,
    `"${author}" author website`
  ];

  for (const q of queries) {
    const items = await googleCSE(q, 10);
    const filtered = items.filter(it => {
      const host = new URL(it.link).host.toLowerCase();
      return !isBlockedHost(host);
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

    const payload = {
      book_title: bookTitle,
      inferred_author: authorRes.name,
      author_confidence: authorRes.confidence,
      author_url: siteRes.url,
      confidence: siteRes.url ? 0.9 : 0,
      _diag: { author: authorRes.debug, site: siteRes.debug }
    };

    return res.status(200).json(payload);
  } catch (err: any) {
    console.error("handler_error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
