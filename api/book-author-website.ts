// api/book-author-website.ts
//
// Refactored: tighter title expansion, safer author-query building, and
// stronger name normalization to avoid false authors like “Poet Lovelace”
// or “Yuval Noah Harari Price”. Includes small heuristic scorer and a
// publisher/retailer bias toward clean author strings.

import type { NextApiRequest, NextApiResponse } from "next";

// ---- Config -----------------------------------------------------------------

const CSE_KEY = process.env.CSE_KEY!;
const CSE_ID = process.env.CSE_ID!;

// optional auth header for the endpoint (examples in your curl snippets)
const AUTH_TOKEN = process.env.AUTH_TOKEN;

// website score threshold (0–1)
const WEBSITE_MIN_SCORE =
  process.env.WEBSITE_MIN_SCORE ? Number(process.env.WEBSITE_MIN_SCORE) : 0.6;

// timeouts
const FETCH_TIMEOUT_MS = 12_000;

// hosts we generally trust for clean author strings
const HOST_WEIGHTS: Record<string, number> = {
  "amazon.com": 0.9,
  "www.amazon.com": 0.9,
  "amazon.co.uk": 0.85,
  "www.amazon.co.uk": 0.85,
  "goodreads.com": 0.85,
  "www.goodreads.com": 0.85,
  "penguinrandomhouse.com": 0.8,
  "www.penguinrandomhouse.com": 0.8,
  "harpercollins.com": 0.8,
  "www.harpercollins.com": 0.8,
  "simonandschuster.com": 0.8,
  "www.simonandschuster.com": 0.8,
  "macmillan.com": 0.8,
  "us.macmillan.com": 0.8,
  "barnesandnoble.com": 0.75,
  "www.barnesandnoble.com": 0.75,
  "en.wikipedia.org": 0.3, // good for subtitle/title, not for author surface text
};

const WEBSITE_BAD_HOSTS = new Set([
  "amazon.com",
  "www.amazon.com",
  "amazon.co.uk",
  "www.amazon.co.uk",
  "goodreads.com",
  "www.goodreads.com",
  "en.wikipedia.org",
  "www.wikipedia.org",
  "facebook.com",
  "www.facebook.com",
  "twitter.com",
  "x.com",
  "instagram.com",
  "www.instagram.com",
  "linkedin.com",
  "www.linkedin.com",
  "youtube.com",
  "www.youtube.com",
]);

// ---- Utilities ---------------------------------------------------------------

// Strip site suffixes and presentation junk from titles we harvest
const SITE_SUFFIX =
  /\s*[-–:]\s*(Wikipedia|Goodreads|Amazon(?:\.com)?|Barnes\s*&\s*Noble|Penguin\s*Random\s*House|Macmillan|HarperCollins|Simon\s*&\s*Schuster|PRH)\s*$/i;

function cleanTitleLike(s: string): string {
  return s
    .replace(SITE_SUFFIX, "") // “… - Wikipedia”
    .replace(/\s*\|.*$/, "") // “… | Site”
    .replace(/[“”"']+/g, "") // quotes
    .replace(/\s{2,}/g, " ")
    .trim();
}

// Abortable fetch with timeout
async function fetchJSON<T>(url: string): Promise<T> {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: ac.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return (await res.json()) as T;
  } finally {
    clearTimeout(t);
  }
}

type CSEItem = {
  kind: string;
  title: string;
  htmlTitle?: string;
  link: string;
  displayLink: string;
  snippet?: string;
  htmlSnippet?: string;
  pagemap?: any;
  mime?: string;
  fileFormat?: string;
};

type CSEResult = {
  items?: CSEItem[];
};

// Build a CSE request URL
function cseUrl(q: string, num = 10): string {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", q);
  url.searchParams.set("num", String(num));
  return url.toString();
}

// Safer expansion: try to find a longer title that *starts with* the user’s
// string; ignore “ - Wikipedia” etc. If polluted, keep the user’s short title.
function expandBookTitleForSearch(
  userTitle: string,
  evidenceTitles: string[]
): { full: string; usedShort: boolean } {
  const base = cleanTitleLike(userTitle);
  let best = "";

  for (const raw of evidenceTitles) {
    const t = cleanTitleLike(raw || "");
    if (!t) continue;
    if (t.toLowerCase().startsWith(base.toLowerCase())) {
      if (t.length > best.length) best = t;
    }
  }

  const polluted =
    !best ||
    /(?:wikipedia|goodreads|amazon|barnes|noble|penguin|random|house)/i.test(
      best
    ) ||
    best.split(/\s+/).length > 12;

  const full = polluted ? base : best;
  return { full, usedShort: polluted };
}

// Build author query without over-quoting; handle ALL CAPS inputs.
function buildAuthorQuery(expandedTitle: string): string {
  const t = cleanTitleLike(expandedTitle);
  const looksAllCaps = /[A-Z]/.test(t) && !/[a-z]/.test(t);
  const norm = looksAllCaps ? t.toLowerCase() : t;

  const toks = norm.split(/\s+/);
  const head = toks.slice(0, Math.min(6, toks.length)).join(" ");
  const tail = toks.slice(6).join(" ");

  const parts = [`"${head}"`, tail, "book", "written by"]
    .filter(Boolean)
    .join(" ");

  return `${parts} -film -movie -screenplay -soundtrack -director`;
}

// Normalize “name-like” candidates and prune junk
function normalizeNameCandidate(raw: string): string | null {
  if (!raw) return null;
  let s = raw;

  // remove HTML entities and brackets content
  s = s.replace(/&amp;|&quot;|&apos;|&lt;|&gt;/g, " ");
  s = s.replace(/\[[^\]]+\]|\([^)]+\)|\{[^}]+\}/g, " ");

  // commas followed by roles/credits
  s = s.replace(
    /\s*,\s*(?:Ph\.?D\.?|MD|M\.?D\.?|Ed\.?D\.?|MBA|J\.?D\.?|Prof\.?|Professor|Editor|Ed\.|Illustrator|Illustrated by|Translator|Foreword by|Preface by|Introduction by)\b.*$/i,
    ""
  );

  // Drop obvious non-name tokens and “price” artefacts
  s = s
    .replace(/\b(price|prices|pricing|sale|discount|deal)\b/gi, " ")
    .replace(
      /[\$£€¥]\s*\d+(?:[\.,]\d{2})?|\b\d+(?:\.\d{2})?\s*(?:USD|EUR|GBP|JPY)\b/gi,
      " "
    );

  // Strip leading role markers like "Author: ", "By "
  s = s.replace(/^\s*(?:author|by|written by|writer|creator)\s*[:\-]\s*/i, "");

  // collapse whitespace & trim punctuation
  s = s.replace(/[–—-]/g, " ").replace(/\s{2,}/g, " ").trim();
  s = s.replace(/^[^\p{L}]+|[^\p{L}]+$/gu, "");

  if (!s) return null;

  // Token inspection
  const toks = s.split(/\s+/);

  // filter if token includes URL-like or domain-y string
  if (toks.some((t) => /\w+\.\w{2,}/.test(t))) return null;

  // Permit hyphenated names, accents; reject if too many words (not a name)
  if (toks.length > 5) return null;

  // Special-case “Poet Lovelace” style: drop leading role nouns (Poet, Sir, Dr.)
  if (/^(poet|sir|dame|lord|lady|dr|doctor|prof|professor)$/i.test(toks[0])) {
    s = toks.slice(1).join(" ");
  }

  // Remove trailing conjunctions like “and” or “with …”
  s = s.replace(/\s+\b(and|with)\b.*$/i, "").trim();

  // Basic “looks like a person” heuristic: at least 2 alphabetic tokens
  const alphaTokens = s.split(/\s+/).filter((t) => /[A-Za-z\u00C0-\u017F]/.test(t));
  if (alphaTokens.length < 2) return null;

  // Title-case the result for consistency
  s = s
    .split(/\s+/)
    .map((t) =>
      t.length <= 3 && t === t.toLowerCase()
        ? t // keep known lower-cased (e.g., "de", "van") as-is
        : t.charAt(0).toUpperCase() + t.slice(1)
    )
    .join(" ");

  return s || null;
}

// Try to mine an author name out of a CSE item (title/snippet/metatags)
function extractAuthorCandidates(item: CSEItem): string[] {
  const out = new Set<string>();
  const pieces: string[] = [];

  if (item.title) pieces.push(item.title);
  if (item.snippet) pieces.push(item.snippet);
  if (item.htmlSnippet) pieces.push(item.htmlSnippet);
  if (item.pagemap?.metatags) {
    const mt = item.pagemap.metatags[0] || {};
    const metaCandidates = [
      mt["og:title"],
      mt["twitter:title"],
      mt["og:description"],
      mt["twitter:description"],
    ].filter(Boolean);
    pieces.push(...metaCandidates);
  }

  // Patterns
  const PATTERNS: RegExp[] = [
    /\bby\s+([A-Z][\p{L}\-']+(?:\s+[A-Z][\p{L}\-']+){0,3})\b/giu,
    /\bauthor:\s*([A-Z][\p{L}\-']+(?:\s+[A-Z][\p{L}\-']+){0,3})\b/giu,
    /\bwritten by\s+([A-Z][\p{L}\-']+(?:\s+[A-Z][\p{L}\-']+){0,3})\b/giu,
    /\b(Diane|Yuval|Barack|Malcolm|Melinda)\s+[A-Z][\p{L}\-']+\b/giu, // light bias for common given names (heuristic)
  ];

  for (const p of pieces) {
    for (const re of PATTERNS) {
      let m: RegExpExecArray | null;
      const hay = p.replace(/<[^>]+>/g, " "); // strip HTML tags if any
      while ((m = re.exec(hay))) {
        const cand = normalizeNameCandidate(m[1]);
        if (cand) out.add(cand);
      }
    }
  }

  // Amazon title forms: “… [Author], [Illustrator]”
  if (/amazon\./i.test(item.displayLink) && item.title) {
    const m = item.title.match(/:\s*([^:]+?)\s*\[(?:Hardcover|Paperback|.*)\]/i);
    if (m) {
      const parts = m[1].split(/,\s*/);
      if (parts.length) {
        const cand = normalizeNameCandidate(parts[0]);
        if (cand) out.add(cand);
      }
    }
  }

  return [...out];
}

// Score a candidate using host + signal count
function scoreCandidate(
  item: CSEItem,
  name: string,
  counts: Record<string, number>
): number {
  const host = (item.displayLink || "").toLowerCase();
  const base = HOST_WEIGHTS[host] ?? 0.5;
  const signal = counts[name] ?? 1;

  let penalty = 0;

  // PDFs often quote other sources -> downweight
  if (item.mime === "application/pdf" || /pdf/i.test(item.fileFormat || ""))
    penalty += 0.2;

  // avoid names that contain “Price” or obvious commerce mis-parses
  if (/\bPrice\b/i.test(name)) penalty += 0.5;

  return Math.max(0, Math.min(1, base + Math.min(0.4, Math.log2(1 + signal) / 4) - penalty));
}

// Pick the best author from CSE items
function inferAuthorFromCSE(items: CSEItem[]) {
  const candidates = new Map<string, number>(); // name -> hits
  const perItemCandidates: Array<{ item: CSEItem; names: string[] }> = [];

  for (const it of items) {
    const names = extractAuthorCandidates(it);
    perItemCandidates.push({ item: it, names });
    for (const n of names) {
      candidates.set(n, (candidates.get(n) || 0) + 1);
    }
  }

  let bestName: string | null = null;
  let bestScore = 0;

  for (const { item, names } of perItemCandidates) {
    for (const n of names) {
      const s = scoreCandidate(item, n, Object.fromEntries(candidates));
      if (s > bestScore) {
        bestScore = s;
        bestName = n;
      }
    }
  }

  return { name: bestName, confidence: bestScore, tallies: candidates };
}

// Find an official-looking author website
function pickAuthorWebsite(items: CSEItem[], author: string) {
  // prefer .org / .com personal sites that include the author’s name in host or title
  let best: { url: string; score: number } | null = null;

  for (const it of items) {
    const host = (it.displayLink || "").toLowerCase();
    if (WEBSITE_BAD_HOSTS.has(host)) continue;

    const title = `${it.title || ""} ${it.snippet || ""}`.toLowerCase();
    const a = author.toLowerCase();

    let score = 0;
    if (host.includes(a.replace(/\s+/g, ""))) score += 0.6; // yuvalnoahharari.com
    if (title.includes("official")) score += 0.25;
    if (title.includes("author")) score += 0.15;

    // small host quality bias
    if (/\.(com|org|net)$/i.test(host)) score += 0.05;

    if (score > (best?.score ?? 0)) {
      best = { url: it.link, score };
    }
  }

  if (!best || best.score < WEBSITE_MIN_SCORE) return { url: null, score: best?.score ?? 0 };
  return best;
}

// ---- API Handler -------------------------------------------------------------

type ResolveBody = {
  book_title?: string;
};

type ResolveResponse = {
  book_title: string;
  inferred_author: string | null;
  author_confidence?: number;
  author_url: string | null;
  confidence: number; // website confidence (0–1)
  error?: string;
  _diag?: any;
};

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<ResolveResponse | { error: string }>
) {
  try {
    // auth (optional)
    if (AUTH_TOKEN) {
      const x = req.headers["x-auth"];
      if (!x || x !== AUTH_TOKEN) {
        return res.status(401).json({ error: "unauthorized" });
      }
    }

    if (req.method !== "POST") {
      return res.status(405).json({ error: "method_not_allowed" });
    }

    const body = req.body as ResolveBody;
    const titleInput = (body?.book_title || "").trim();
    if (!titleInput) {
      return res.status(400).json({ error: "missing_book_title" });
    }

    // 1) Quick CSE to gather evidence titles for expansion
    const probeQ = `"${cleanTitleLike(titleInput)}" book`;
    const probeJson = await fetchJSON<CSEResult>(cseUrl(probeQ, 10));
    const evidenceTitles =
      probeJson.items?.map((i) => i.title).filter(Boolean) ?? [];

    const expanded = expandBookTitleForSearch(titleInput, evidenceTitles);

    // 2) Build a safer author query
    const authorQuery = buildAuthorQuery(expanded.full);

    const authorJson = await fetchJSON<CSEResult>(cseUrl(authorQuery, 10));
    const authorItems = authorJson.items ?? [];

    const authorPick = inferAuthorFromCSE(authorItems);
    const inferredAuthor = authorPick.name;

    // If no author -> report and exit
    if (!inferredAuthor) {
      return res.status(200).json({
        book_title: titleInput,
        inferred_author: null,
        author_url: null,
        confidence: 0,
        error: "no_author_found",
        _diag: {
          flags: {
            USE_SEARCH: true,
            CSE_KEY: Boolean(CSE_KEY),
            CSE_ID: Boolean(CSE_ID),
            WEBSITE_MIN_SCORE,
          },
          author: {
            expanded: {
              full: expanded.full,
              usedShort: expanded.usedShort,
              debug: {
                q: probeQ,
                bestFull: expanded.full === titleInput ? null : expanded.full,
                evidence: evidenceTitles.slice(0, 10),
              },
            },
            query: authorQuery,
            items: authorItems.slice(0, 10),
            candidates: Object.fromEntries(authorPick.tallies),
          },
        },
      });
    }

    // 3) Look for the author's website
    const siteQueries = [
      `"${inferredAuthor}" author website`,
      `"${inferredAuthor}" official site`,
      `"${inferredAuthor}" homepage`,
    ];

    let bestSite: { url: string | null; score: number } = { url: null, score: 0 };
    const tried: string[] = [];

    for (const q of siteQueries) {
      tried.push(q);
      const js = await fetchJSON<CSEResult>(cseUrl(q, 10));
      const items = js.items ?? [];
      const pick = pickAuthorWebsite(items, inferredAuthor);
      if (pick.url && pick.score > bestSite.score) bestSite = pick;
      if (bestSite.score >= WEBSITE_MIN_SCORE) break; // good enough
    }

    return res.status(200).json({
      book_title: titleInput,
      inferred_author: inferredAuthor,
      author_confidence: Number(authorPick.confidence.toFixed(2)),
      author_url: bestSite.url,
      confidence: Number((bestSite.score || 0).toFixed(2)),
      _diag: {
        flags: {
          USE_SEARCH: true,
          CSE_KEY: Boolean(CSE_KEY),
          CSE_ID: Boolean(CSE_ID),
          WEBSITE_MIN_SCORE,
        },
        author: {
          expanded: {
            full: expanded.full,
            usedShort: expanded.usedShort,
            debug: {
              q: probeQ,
              bestFull: expanded.full === titleInput ? null : expanded.full,
              evidence: evidenceTitles.slice(0, 10),
            },
          },
          query: authorQuery,
          items: authorItems.slice(0, 10),
          candidates: Object.fromEntries(authorPick.tallies),
        },
        site: {
          tried: siteQueries,
          threshold: WEBSITE_MIN_SCORE,
        },
      },
    });
  } catch (err: any) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
