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
  url.search
