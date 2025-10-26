// api/book-author-website.ts
//
// Framework-agnostic (no `next` import). Works as a Vercel Node serverless
// function. Includes the previously discussed fixes:
//
// • Safer title expansion (ignores “- Wikipedia”, etc.)
// • Robust author-query builder (handles ALL CAPS, partial titles)
// • Stronger name normalization (filters “Poet Lovelace”, “… Price” artefacts)
// • Lightweight scoring with host trust & PDF penalization
// • Website picker that avoids retailer/social hosts

// ---- Config -----------------------------------------------------------------

const CSE_KEY = process.env.CSE_KEY!;
const CSE_ID = process.env.CSE_ID!;
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
  "en.wikipedia.org": 0.3, // good for subtitle/title, not for author text
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

const SITE_SUFFIX =
  /\s*[-–:]\s*(Wikipedia|Goodreads|Amazon(?:\.com)?|Barnes\s*&\s*Noble|Penguin\s*Random\s*House|Macmillan|HarperCollins|Simon\s*&\s*Schuster|PRH)\s*$/i;

function cleanTitleLike(s: string): string {
  return (s || "")
    .replace(SITE_SUFFIX, "")
    .replace(/\s*\|.*$/, "")
    .replace(/[“”"']+/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

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

type CSEResult = { items?: CSEItem[] };

function cseUrl(q: string, num = 10): string {
  const url = new URL("https://www.googleapis.com/customsearch/v1");
  url.searchParams.set("key", CSE_KEY);
  url.searchParams.set("cx", CSE_ID);
  url.searchParams.set("q", q);
  url.searchParams.set("num", String(num));
  return url.toString();
}

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

function normalizeNameCandidate(raw: string): string | null {
  if (!raw) return null;
  let s = raw;

  s = s.replace(/&amp;|&quot;|&apos;|&lt;|&gt;/g, " ");
  s = s.replace(/\[[^\]]+\]|\([^)]+\)|\{[^}]+\}/g, " ");

  s = s.replace(
    /\s*,\s*(?:Ph\.?D\.?|MD|M\.?D\.?|Ed\.?D\.?|MBA|J\.?D\.?|Prof\.?|Professor|Editor|Ed\.|Illustrator|Illustrated by|Translator|Foreword by|Preface by|Introduction by)\b.*$/i,
    ""
  );

  s = s
    .replace(/\b(price|prices|pricing|sale|discount|deal)\b/gi, " ")
    .replace(
      /[\$£€¥]\s*\d+(?:[\.,]\d{2})?|\b\d+(?:\.\d{2})?\s*(?:USD|EUR|GBP|JPY)\b/gi,
      " "
    );

  s = s.replace(/^\s*(?:author|by|written by|writer|creator)\s*[:\-]\s*/i, "");
  s = s.replace(/[–—-]/g, " ").replace(/\s{2,}/g, " ").trim();
  s = s.replace(/^[^\p{L}]+|[^\p{L}]+$/gu, "");

  if (!s) return null;

  const toks = s.split(/\s+/);
  if (toks.some((t) => /\w+\.\w{2,}/.test(t))) return null;
  if (toks.length > 5) return null;

  if (/^(poet|sir|dame|lord|lady|dr|doctor|prof|professor)$/i.test(toks[0])) {
    s = toks.slice(1).join(" ");
  }

  s = s.replace(/\s+\b(and|with)\b.*$/i, "").trim();

  const alphaTokens = s.split(/\s+/).filter((t) => /[A-Za-z\u00C0-\u017F]/.test(t));
  if (alphaTokens.length < 2) return null;

  s = s
    .split(/\s+/)
    .map((t) =>
      t.length <= 3 && t === t.toLowerCase()
        ? t
        : t.charAt(0).toUpperCase() + t.slice(1)
    )
    .join(" ");

  return s || null;
}

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

  const PATTERNS: RegExp[] = [
    /\bby\s+([A-Z][\p{L}\-']+(?:\s+[A-Z][\p{L}\-']+){0,3})\b/giu,
    /\bauthor:\s*([A-Z][\p{L}\-']+(?:\s+[A-Z][\p{L}\-']+){0,3})\b/giu,
    /\bwritten by\s+([A-Z][\p{L}\-']+(?:\s+[A-Z][\p{L}\-']+){0,3})\b/giu,
    /\b(Diane|Yuval|Barack|Malcolm|Melinda)\s+[A-Z][\p{L}\-']+\b/giu,
  ];

  for (const p of pieces) {
    for (const re of PATTERNS) {
      let m: RegExpExecArray | null;
      const hay = p.replace(/<[^>]+>/g, " ");
      while ((m = re.exec(hay))) {
        const cand = normalizeNameCandidate(m[1]);
        if (cand) out.add(cand);
      }
    }
  }

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

function scoreCandidate(
  item: CSEItem,
  name: string,
  counts: Record<string, number>
): number {
  const host = (item.displayLink || "").toLowerCase();
  const base = HOST_WEIGHTS[host] ?? 0.5;
  const signal = counts[name] ?? 1;

  let penalty = 0;
  if (item.mime === "application/pdf" || /pdf/i.test(item.fileFormat || ""))
    penalty += 0.2;
  if (/\bPrice\b/i.test(name)) penalty += 0.5;

  return Math.max(
    0,
    Math.min(1, base + Math.min(0.4, Math.log2(1 + signal) / 4) - penalty)
  );
}

function inferAuthorFromCSE(items: CSEItem[]) {
  const candidates = new Map<string, number>();
  const perItem: Array<{ item: CSEItem; names: string[] }> = [];

  for (const it of items) {
    const names = extractAuthorCandidates(it);
    perItem.push({ item: it, names });
    for (const n of names) {
      candidates.set(n, (candidates.get(n) || 0) + 1);
    }
  }

  let bestName: string | null = null;
  let bestScore = 0;

  const counts = Object.fromEntries(candidates);

  for (const { item, names } of perItem) {
    for (const n of names) {
      const s = scoreCandidate(item, n, counts);
      if (s > bestScore) {
        bestScore = s;
        bestName = n;
      }
    }
  }

  return { name: bestName, confidence: bestScore, tallies: candidates };
}

function pickAuthorWebsite(items: CSEItem[], author: string) {
  let best: { url: string; score: number } | null = null;

  for (const it of items) {
    const host = (it.displayLink || "").toLowerCase();
    if (WEBSITE_BAD_HOSTS.has(host)) continue;

    const title = `${it.title || ""} ${it.snippet || ""}`.toLowerCase();
    const a = author.toLowerCase();

    let score = 0;
    if (host.includes(a.replace(/\s+/g, ""))) score += 0.6;
    if (title.includes("official")) score += 0.25;
    if (title.includes("author")) score += 0.15;
    if (/\.(com|org|net)$/i.test(host)) score += 0.05;

    if (!best || score > best.score) {
      best = { url: it.link, score };
    }
  }

  if (!best || best.score < WEBSITE_MIN_SCORE)
    return { url: null as string | null, score: best?.score ?? 0 };
  return best;
}

// ---- API Handler -------------------------------------------------------------

type ResolveBody = { book_title?: string };

type ResolveResponse = {
  book_title: string;
  inferred_author: string | null;
  author_confidence?: number;
  author_url: string | null;
  confidence: number;
  error?: string;
  _diag?: any;
};

export default async function handler(req: any, res: any) {
  try {
    if (AUTH_TOKEN) {
      const x = req.headers?.["x-auth"];
      if (!x || x !== AUTH_TOKEN) {
        res.status(401).json({ error: "unauthorized" });
        return;
      }
    }

    if (req.method !== "POST") {
      res.status(405).json({ error: "method_not_allowed" });
      return;
    }

    const body: ResolveBody =
      typeof req.body === "object" && req.body
        ? req.body
        : JSON.parse(req.body || "{}");

    const titleInput = (body?.book_title || "").trim();
    if (!titleInput) {
      res.status(400).json({ error: "missing_book_title" });
      return;
    }

    // 1) Probe for title expansion
    const probeQ = `"${cleanTitleLike(titleInput)}" book`;
    const probeJson = await fetchJSON<CSEResult>(cseUrl(probeQ, 10));
    const evidenceTitles = probeJson.items?.map((i) => i.title).filter(Boolean) ?? [];
    const expanded = expandBookTitleForSearch(titleInput, evidenceTitles);

    // 2) Author query
    const authorQuery = buildAuthorQuery(expanded.full);
    const authorJson = await fetchJSON<CSEResult>(cseUrl(authorQuery, 10));
    const authorItems = authorJson.items ?? [];
    const authorPick = inferAuthorFromCSE(authorItems);
    const inferredAuthor = authorPick.name;

    if (!inferredAuthor) {
      const payload: ResolveResponse = {
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
      };
      res.status(200).json(payload);
      return;
    }

    // 3) Author website
    const siteQueries = [
      `"${inferredAuthor}" author website`,
      `"${inferredAuthor}" official site`,
      `"${inferredAuthor}" homepage`,
    ];

    let bestSite: { url: string | null; score: number } = { url: null, score: 0 };
    for (const q of siteQueries) {
      const js = await fetchJSON<CSEResult>(cseUrl(q, 10));
      const items = js.items ?? [];
      const pick = pickAuthorWebsite(items, inferredAuthor);
      if (pick.url && pick.score > bestSite.score) bestSite = pick;
      if (bestSite.score >= WEBSITE_MIN_SCORE) break;
    }

    const payload: ResolveResponse = {
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
          threshold: WEBSITE_MIN_SCORE,
        },
      },
    };

    res.status(200).json(payload);
  } catch (err: any) {
    res.status(500).json({ error: String(err?.message || err) });
  }
}
