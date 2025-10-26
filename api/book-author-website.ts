// api/book-author-website.ts
/* eslint-disable @typescript-eslint/no-explicit-any */

import type { NextApiRequest, NextApiResponse } from "next";

/**
 * ENV
 *  - CSE_KEY: Google Custom Search JSON API key
 *  - CSE_ID:  Google Custom Search Engine ID
 *  - WEBSITE_MIN_SCORE: number (0..1)
 *  - AUTH_TOKEN: shared secret for X-Auth header (optional; if present, enforced)
 */

type CSEItem = {
  link?: string;
  displayLink?: string;
  title?: string;
  htmlTitle?: string;
  snippet?: string;
  htmlSnippet?: string;
  pagemap?: any;
  mime?: string;
  fileFormat?: string;
};

type ResolveResponse = {
  book_title: string;
  inferred_author?: string | null;
  author_confidence?: number;
  author_url?: string | null;
  confidence: number;
  error?: string;
  _diag?: any;
};

const REQUIRED_AUTH = process.env.AUTH_TOKEN?.trim();

/* ----------------------------- Utilities ----------------------------- */

const SITE_SUFFIX =
  /\s*[-–:]\s*(Wikipedia|Goodreads|Amazon(\.com)?|Barnes\s*&\s*Noble|Penguin\s*Random\s*House|Macmillan|HarperCollins|Simon\s*&\s*Schuster|PRH)\s*$/i;

function cleanTitleLike(s: string): string {
  return (s || "")
    .replace(SITE_SUFFIX, "") // drop common " - Wikipedia"/retailer suffixes
    .replace(/\s*\|.*$/, "") // drop " | Site"
    .replace(/[“”"’‘]+/g, "") // quotes
    .replace(/\s{2,}/g, " ")
    .trim();
}

function looksAllCaps(s: string): boolean {
  return /[A-Z]/.test(s) && !/[a-z]/.test(s);
}

/** Title expansion: prefer evidence that starts with the user’s title; otherwise keep short. */
function expandBookTitleForSearch(
  userTitle: string,
  evidenceTitles: string[]
): { full: string; usedShort: boolean; debug: any } {
  const base = cleanTitleLike(userTitle);
  let best = "";

  const considered: Array<{ cand: string; full: string; reason: string }> = [];

  for (const raw of evidenceTitles || []) {
    const t = cleanTitleLike(raw);
    if (!t) continue;

    considered.push({ cand: raw, full: t, reason: "from_evidence" });

    if (t.toLowerCase().startsWith(base.toLowerCase())) {
      if (t.length > best.length) best = t;
    }
  }

  // Pollution checks
  const polluted =
    !best ||
    /(wikipedia|goodreads|amazon|barnes|noble|penguin|random|house|harpercollins|macmillan|simon\s*&\s*schuster)/i.test(
      best
    ) ||
    best.split(/\s+/).length > 12;

  return {
    full: polluted ? base : best,
    usedShort: polluted,
    debug: {
      q: `"${cleanTitleLike(userTitle)}" book`,
      bestFull: polluted ? null : best,
      evidence: considered,
    },
  };
}

/** Build a safe author query from expanded title. */
function buildAuthorQuery(expandedTitle: string): string {
  const t0 = cleanTitleLike(expandedTitle);
  const t = looksAllCaps(t0) ? t0.toLowerCase() : t0;

  const toks = t.split(/\s+/);
  const head = toks.slice(0, Math.min(6, toks.length)).join(" ");
  const tail = toks.slice(6).join(" ");

  const parts = [`"${head}"`, tail, "book", "written by"].filter(Boolean);
  const core = parts.join(" ");
  return `${core} -film -movie -screenplay -soundtrack -director`;
}

/** Normalize name candidates: strip roles, prices, and trailing garbage; title-case properly. */
function normalizeNameCandidate(raw: string): string | null {
  if (!raw) return null;

  // 1) Pull the part that looks like a person name out of noisy strings (e.g., "by XYZ, PhD – $14.99")
  let s = raw
    .replace(/^[^A-Za-z]+/, "")
    .replace(/\bby\s+/i, "")
    .replace(/\s*\([^)]*\)\s*$/, "") // drop trailing parentheticals
    .replace(/\s*[,;•|–-]\s*(?:author|writer|editor|illustrator|narrator|price|hardcover|paperback|ebook|edition)\b.*$/i, "")
    .replace(/\s+\$?\d+(?:\.\d{2})?\b.*$/i, "") // drop price and after
    .replace(/\s{2,}/g, " ")
    .trim();

  // 2) If the string is a comma-separated role title (e.g., "Poet Lovelace", "Change Transitions"),
  //    reject words that are common role/descriptor nouns when they appear in the leading slot.
  const roleish = new Set([
    "poet",
    "saint",
    "doctor",
    "professor",
    "coach",
    "pastor",
    "bishop",
    "minister",
    "president",
    "prime",
    "queen",
    "king",
    "change",
    "transitions",
    "author",
    "writer",
  ]);
  const tokens = s.split(/\s+/);
  if (tokens.length === 2) {
    const [w1, w2] = tokens.map((t) => t.toLowerCase());
    if (roleish.has(w1) && /^[a-z]+$/.test(w2)) {
      // likely "Poet Lovelace" → drop
      return null;
    }
  }

  // 3) Remove stray "price" if it stuck to the end (e.g., "Yuval Noah Harari Price")
  s = s.replace(/\bprice\b$/i, "").trim();

  // 4) Guard against all-uppercase names from shouted titles
  if (looksAllCaps(s)) {
    s = s.toLowerCase();
  }

  // 5) Title-case words that look like names; preserve "van", "de", etc.
  const keepLower = new Set(["van", "de", "da", "del", "dos", "di", "von", "der", "la", "le", "of"]);
  s = s
    .split(/\s+/)
    .map((w, i) => {
      const lw = w.toLowerCase();
      if (i > 0 && keepLower.has(lw)) return lw;
      return w.charAt(0).toUpperCase() + w.slice(1);
    })
    .join(" ")
    .replace(/\s{2,}/g, " ")
    .trim();

  // Basic sanity: require at least one space (firstname lastname) unless it's a known single-name author
  if (!/\s/.test(s)) {
    const singletons = new Set(["Plato", "Homer", "Voltaire", "Molière", "Aeschylus", "Euripides", "Sophocles"]);
    if (!singletons.has(s)) return null;
  }

  // Ban generic phrases
  if (/^(poet|author|writer|change|transitions)$/i.test(s)) return null;

  return s || null;
}

function scoreAuthorNameQuality(name: string): number {
  // lightweight heuristic: 0..1
  if (!name) return 0;
  let score = 0.5;
  if (/\s/.test(name)) score += 0.2;
  if (/^[A-Z][a-z]+(?:\s+[A-Z][a-z'-]+)+$/.test(name)) score += 0.2;
  if (!/(author|writer|editor|illustrator|price)/i.test(name)) score += 0.1;
  return Math.max(0, Math.min(1, score));
}

function isLikelyPdf(item: CSEItem): boolean {
  return item.mime === "application/pdf" || /pdf/i.test(item.fileFormat || "");
}

function unique<T>(arr: T[]): T[] {
  return Array.from(new Set(arr));
}

/* ----------------------------- CSE Helpers ----------------------------- */

async function cseSearch(q: string) {
  const key = process.env.CSE_KEY;
  const cx = process.env.CSE_ID;
  if (!key || !cx) throw new Error("Missing CSE_KEY or CSE_ID");

  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", key);
  url.searchParams.set("cx", cx);
  url.searchParams.set("q", q);
  url.searchParams.set("num", "10");

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`CSE error: ${res.status}`);
  return res.json();
}

/* --------------------------- Extraction logic -------------------------- */

function harvestEvidenceTitles(items: CSEItem[]): string[] {
  const out: string[] = [];
  for (const it of items) {
    if (it?.title) out.push(it.title);
    if (it?.htmlTitle) out.push(it.htmlTitle);
  }
  return out;
}

function harvestAuthorCandidates(items: CSEItem[]): string[] {
  const cands: string[] = [];

  for (const it of items) {
    if (isLikelyPdf(it)) continue; // PDFs often quote Wikipedia, rarely clean author lines

    const src: string[] = [];
    if (it.title) src.push(it.title);
    if (it.snippet) src.push(it.snippet);
    if (it.htmlSnippet) src.push(it.htmlSnippet);

    // Common “by <Author>” pattern
    for (const s of src) {
      const m = s.match(/\bby\s+([A-Z][A-Za-z'’.\-]+\s+[A-Z][A-Za-z'’.\-]+(?:\s+[A-Z][A-Za-z'’.\-]+)*)/i);
      if (m) cands.push(m[1]);
    }

    // Goodreads/Amazon title pattern: “<Book> by <Author>”
    for (const s of src) {
      const m = s.match(/\bby\s+([A-Z][^\]|:]+?)(?:\s*[\]|:–-]|$)/i);
      if (m) cands.push(m[1]);
    }

    // Fallback: pagemap metadata (sometimes includes author)
    const meta = it.pagemap?.metatags?.[0];
    const ogTitle: string | undefined = meta?.["og:title"] || meta?.["twitter:title"];
    if (ogTitle) {
      const m = ogTitle.match(/\bby\s+([A-Z][A-Za-z'’.\-]+\s+[A-Z][A-Za-z'’.\-]+.*)$/i);
      if (m) cands.push(m[1]);
    }
  }

  return cands;
}

function pickBestAuthor(rawCandidates: string[]) {
  const normalized: string[] = [];
  for (const c of rawCandidates) {
    const n = normalizeNameCandidate(c);
    if (n) normalized.push(n);
  }
  const counts = new Map<string, number>();
  for (const n of normalized) counts.set(n, (counts.get(n) || 0) + 1);

  let best = "";
  let bestScore = 0;

  for (const [name, cnt] of counts.entries()) {
    const quality = scoreAuthorNameQuality(name);
    const score = cnt * 0.6 + quality * 0.4;
    if (score > bestScore) {
      bestScore = score;
      best = name;
    }
  }

  return { name: best || null, confidence: Number(best ? Math.min(0.95, bestScore / 3).toFixed(2) : 0) };
}

function harvestWebsiteCandidates(items: CSEItem[], author: string): Array<{ url: string; score: number }> {
  const out: Array<{ url: string; score: number }> = [];
  const authorLow = author.toLowerCase();

  for (const it of items) {
    const url = it.link || "";
    const host = (it.displayLink || "").toLowerCase();
    const title = (it.title || "").toLowerCase();

    if (!url) continue;

    let score = 0;

    // Prefer obvious official sites
    if (/official site|official website|home page/i.test(it.title || "")) score += 0.5;

    // Domain hints
    if (host.includes(authorLow.replace(/\s+/g, ""))) score += 0.3;
    if (/\.org|\.com|\.net$/i.test(host)) score += 0.05;

    // Author name in title
    if (title.includes(authorLow)) score += 0.15;

    // Deprioritize socials, retailers, and aggregators
    if (/(twitter|x\.com|facebook|instagram|goodreads|amazon|barnes|noble|wikipedia)/i.test(host)) score -= 0.4;

    if (score > 0) out.push({ url, score });
  }

  // unique by url
  const seen = new Set<string>();
  const uniq = out.filter((o) => (seen.has(o.url) ? false : (seen.add(o.url), true)));

  return uniq.sort((a, b) => b.score - a.score);
}

/* ------------------------------ API route ------------------------------ */

export default async function handler(req: NextApiRequest, res: NextApiResponse<ResolveResponse>) {
  try {
    if (REQUIRED_AUTH) {
      const token = (req.headers["x-auth"] || req.headers["X-Auth"]) as string | undefined;
      if (!token || token !== REQUIRED_AUTH) {
        return res.status(401).json({
          book_title: (req.body?.book_title as string) || "",
          confidence: 0,
          error: "unauthorized",
        });
      }
    }

    if (req.method !== "POST") {
      return res.status(405).json({ book_title: "", confidence: 0, error: "method_not_allowed" });
    }

    const rawTitle: string = (req.body?.book_title || "").toString().trim();
    if (!rawTitle) {
      return res.status(400).json({ book_title: "", confidence: 0, error: "missing_book_title" });
    }

    // Phase 1: initial search to gather evidence titles
    const initialQ = `"${cleanTitleLike(rawTitle)}" book`;
    const initial = await cseSearch(initialQ);
    const firstItems: CSEItem[] = initial?.items || [];
    const evidenceTitles = harvestEvidenceTitles(firstItems);

    // Phase 2: safer title expansion (fix #1 & #2)
    const expanded = expandBookTitleForSearch(rawTitle, evidenceTitles);

    // Phase 3: build robust author query (fix #3)
    const authorQuery = buildAuthorQuery(expanded.full);
    const authorSearch = await cseSearch(authorQuery);
    const authorItems: CSEItem[] = authorSearch?.items || [];

    // Collect author candidates from both waves (dedup to keep things tidy)
    const candidates = unique([
      ...harvestAuthorCandidates(firstItems),
      ...harvestAuthorCandidates(authorItems),
    ]);
    const picked = pickBestAuthor(candidates);

    const diag: any = {
      flags: {
        USE_SEARCH: true,
        CSE_KEY: !!process.env.CSE_KEY,
        CSE_ID: !!process.env.CSE_ID,
        WEBSITE_MIN_SCORE: Number(process.env.WEBSITE_MIN_SCORE ?? 0.6),
      },
      author: {
        expanded,
        query: authorQuery,
        items: authorItems,
        candidates: candidates.slice(0, 24),
        picked: picked.name,
      },
    };

    if (!picked.name) {
      return res.status(200).json({
        book_title: rawTitle,
        inferred_author: null,
        confidence: 0,
        error: "no_author_found",
        _diag: diag,
      });
    }

    // Phase 4: website resolution (very lightweight heuristic)
    const siteQuery = `"${picked.name}" official site`;
    const siteSearch = await cseSearch(siteQuery);
    const siteItems: CSEItem[] = siteSearch?.items || [];
    const siteCands = harvestWebsiteCandidates(siteItems, picked.name);
    const minScore = Math.max(0, Math.min(1, Number(process.env.WEBSITE_MIN_SCORE ?? 0.6)));
    const topSite = siteCands.find((c) => c.score >= minScore);

    diag.site = {
      tried: [siteQuery],
      threshold: minScore,
      top: siteCands[0] || null,
    };

    return res.status(200).json({
      book_title: rawTitle,
      inferred_author: picked.name,
      author_confidence: picked.confidence,
      author_url: topSite?.url ?? null,
      confidence: topSite ? Number(Math.min(1, picked.confidence * topSite.score).toFixed(2)) : 0,
      _diag: diag,
    });
  } catch (err: any) {
    return res.status(200).json({
      book_title: (req.body?.book_title as string) || "",
      inferred_author: null,
      confidence: 0,
      error: err?.message || "unknown_error",
    });
  }
}
