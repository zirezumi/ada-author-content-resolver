// api/resolve.ts
// No @vercel/node import needed — works in Vercel Node.js runtime.

const UPSTREAM_PATH = '/api/book-author-website'; // your protected function
const AUTH_HEADER   = 'X-Auth';
const SECRET        = process.env.AUTHOR_UPDATES_SECRET || '';

export default async function handler(req: any, res: any) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  if (!SECRET) {
    return res.status(500).json({ error: 'Missing AUTHOR_UPDATES_SECRET env var' });
  }

  // Build same-origin URL for the upstream function
  const host = (req.headers['x-forwarded-host'] || req.headers.host) as string;
  const proto = (req.headers['x-forwarded-proto'] || 'https') as string;
  const url = `${proto}://${host}${UPSTREAM_PATH}`;

  const body =
    typeof req.body === 'string' ? req.body : JSON.stringify(req.body ?? {});

  // Use global fetch; if TS complains, cast via globalThis as any.
  const upstream = await (globalThis as any).fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      [AUTH_HEADER]: SECRET,
    },
    body,
  });

  const contentType = upstream.headers.get('content-type') || '';
  const text = await upstream.text();

  res.status(upstream.status);
  if (contentType) res.setHeader('content-type', contentType);
  // Pass through JSON or text
  try {
    res.send(contentType.includes('application/json') ? JSON.parse(text) : text);
  } catch {
    res.send(text);
  }
}
