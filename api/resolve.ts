// api/resolve.ts
import type { VercelRequest, VercelResponse } from '@vercel/node';

const UPSTREAM_PATH = '/api/book-author-website'; // your protected function
const AUTH_HEADER = 'X-Auth';
const SECRET = process.env.AUTHOR_UPDATES_SECRET || '';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }
  if (!SECRET) {
    return res.status(500).json({ error: 'Missing AUTHOR_UPDATES_SECRET env var' });
  }

  const url = `https://${req.headers.host}${UPSTREAM_PATH}`;
  const upstream = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      [AUTH_HEADER]: SECRET,
    },
    body: typeof req.body === 'string' ? req.body : JSON.stringify(req.body ?? {}),
  });

  const type = upstream.headers.get('content-type') || '';
  const data = type.includes('application/json') ? await upstream.json() : await upstream.text();
  return res.status(upstream.status).send(type.includes('application/json') ? data : { raw: data });
}
