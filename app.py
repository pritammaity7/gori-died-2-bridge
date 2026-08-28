"""Telegram streaming bridge for the Gori Died 2 course site.

Deployed on Render (web service). Streams video/PDF bytes straight from the
NEW Telegram group over HTTP with Range support, so the course site never has
to proxy media through Cloudflare.

Design notes
------------
* Bot tokens only - never a user StringSession. User sessions are in active use
  by the migration fleet and connecting one from a second IP burns it
  (AuthKeyDuplicatedError, learned the hard way 2026-08-25).
* Multiple bot tokens supported (comma separated), used round-robin so
  concurrent viewers spread across connections.
* Every response is Range-capable => seeking does not re-download.
* All configuration comes from environment variables (Render secrets).

Env: API_ID, API_HASH, BOT_TOKENS (comma separated), BRIDGE_TOKEN, PEER
"""
import asyncio
import logging
import os
import re
import time
from itertools import cycle

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from telethon import TelegramClient, connection

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('bridge')

API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
BOT_TOKENS = [t.strip() for t in os.getenv('BOT_TOKENS', '').split(',') if t.strip()]
BRIDGE_TOKEN = os.getenv('BRIDGE_TOKEN', '')
DEFAULT_PEER = os.getenv('PEER', '')
CHUNK = 1024 * 1024          # 1 MB request size: good throughput, low memory
META_TTL = 3600              # cache message metadata for an hour

app = FastAPI(title='Gori Died 2 stream bridge', docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'],
                   allow_headers=['*'],
                   expose_headers=['Content-Range', 'Accept-Ranges',
                                   'Content-Length', 'Content-Type'])

clients = []
_rr = None
_meta_cache = {}


def _peer(value):
    v = (value or DEFAULT_PEER or '').strip()
    if not v:
        return None
    return int(v) if v.lstrip('-').isdigit() else v


def pick():
    """Round-robin over connected bots so load spreads evenly."""
    global _rr
    live = [c for c in clients if c.is_connected()]
    if not live:
        return None
    if _rr is None:
        _rr = cycle(range(1 << 30))
    return live[next(_rr) % len(live)]


async def _start(c, i, token):
    try:
        await c.start(bot_token=token)
        me = await c.get_me()
        log.info('worker %d online as @%s', i, me.username)
    except Exception as e:
        log.error('worker %d failed: %s: %s', i, type(e).__name__, e)


@app.on_event('startup')
async def startup():
    if not API_ID or not API_HASH or not BOT_TOKENS:
        log.error('missing API_ID / API_HASH / BOT_TOKENS')
        return
    for i, token in enumerate(BOT_TOKENS):
        c = TelegramClient(f'/tmp/bridge_{i}', API_ID, API_HASH,
                           connection=connection.ConnectionTcpFull,
                           connection_retries=None, auto_reconnect=True, timeout=30)
        clients.append(c)
        asyncio.create_task(_start(c, i, token))


@app.on_event('shutdown')
async def shutdown():
    for c in clients:
        try:
            await c.disconnect()
        except Exception:
            pass


def _auth(token):
    return (not BRIDGE_TOKEN) or token == BRIDGE_TOKEN


@app.api_route('/', methods=['GET', 'HEAD'])
async def root(request: Request):
    if request.method == 'HEAD':
        return Response(status_code=200)
    return {
        'status': 'gori-died-2 bridge up',
        'workers_total': len(clients),
        'workers_connected': sum(1 for c in clients if c.is_connected()),
        'cached_meta': len(_meta_cache),
    }


@app.get('/health')
async def health():
    ok = any(c.is_connected() for c in clients)
    return JSONResponse({'ok': ok}, status_code=200 if ok else 503)


async def _media(worker, peer, msg_id):
    """Return (media, size, mime, filename) with a short-lived cache."""
    key = (str(peer), msg_id)
    hit = _meta_cache.get(key)
    if hit and time.time() - hit[0] < META_TTL:
        return hit[1], hit[2], hit[3], hit[4]

    msgs = await worker.get_messages(peer, ids=[msg_id])
    if not msgs or not msgs[0] or not msgs[0].media:
        return None, 0, '', ''
    media = msgs[0].media
    media = getattr(media, 'document', None) or getattr(media, 'photo', None) or media
    size = getattr(media, 'size', 0) or 0
    mime = getattr(media, 'mime_type', '') or 'application/octet-stream'
    name = ''
    for a in getattr(media, 'attributes', []) or []:
        if hasattr(a, 'file_name'):
            name = a.file_name
            break
    _meta_cache[key] = (time.time(), media, size, mime, name)
    return media, size, mime, name


@app.api_route('/stream/{msg_id}', methods=['GET', 'HEAD'])
async def stream(msg_id: int, request: Request, peer: str = '', token: str = '',
                 dl: int = 0):
    """Range-capable byte stream for one Telegram message.

    dl=1 -> Content-Disposition: attachment (used by the download button).
    """
    if not _auth(token):
        return Response('unauthorized', status_code=401)
    worker = pick()
    if worker is None:
        return JSONResponse({'error': 'workers connecting, retry shortly'}, status_code=503)
    target = _peer(peer)
    if target is None:
        return Response('peer not configured', status_code=400)

    try:
        media, size, mime, name = await _media(worker, target, msg_id)
    except Exception as e:
        log.error('meta %s: %s', msg_id, e)
        return Response(f'metadata error: {type(e).__name__}', status_code=502)
    if media is None:
        return Response('file not found', status_code=404)

    fname = (name or f'file_{msg_id}').replace('"', '')
    base = {
        'Accept-Ranges': 'bytes',
        'Content-Type': mime,
        'Content-Disposition': f'{"attachment" if dl else "inline"}; filename="{fname}"',
        'Cache-Control': 'public, max-age=86400',
    }
    if request.method == 'HEAD':
        return Response(status_code=200, headers={**base, 'Content-Length': str(size)})

    start, end = 0, size - 1
    rng = request.headers.get('Range')
    if rng:
        m = re.search(r'bytes=(\d*)-(\d*)', rng)
        if m:
            if m.group(1):
                start = int(m.group(1))
            if m.group(2):
                end = min(int(m.group(2)), size - 1)
    if start >= size:
        return Response(status_code=416, headers={**base, 'Content-Range': f'bytes */{size}'})
    want = end - start + 1

    async def sender():
        sent = 0
        try:
            async for chunk in worker.iter_download(media, offset=start, request_size=CHUNK):
                if sent + len(chunk) >= want:
                    yield chunk[:want - sent]
                    break
                yield chunk
                sent += len(chunk)
        except Exception as e:                       # client disconnect lands here
            log.info('stream %s ended: %s', msg_id, type(e).__name__)

    return StreamingResponse(sender(), status_code=206 if rng else 200, headers={
        **base,
        'Content-Length': str(want),
        **({'Content-Range': f'bytes {start}-{end}/{size}'} if rng else {}),
    })


@app.get('/thumb/{msg_id}')
async def thumb(msg_id: int, peer: str = '', token: str = ''):
    """Poster image for a video (Telegram already stores thumbnails)."""
    if not _auth(token):
        return Response('unauthorized', status_code=401)
    worker = pick()
    if worker is None:
        return Response(status_code=503)
    target = _peer(peer)
    if target is None:
        return Response('peer not configured', status_code=400)
    try:
        msgs = await worker.get_messages(target, ids=[msg_id])
        if not msgs or not msgs[0]:
            return Response(status_code=404)
        data = await worker.download_media(msgs[0], thumb=-1, file=bytes)
        if not data:
            return Response(status_code=404)
        return Response(data, media_type='image/jpeg',
                        headers={'Cache-Control': 'public, max-age=604800'})
    except Exception as e:
        log.info('thumb %s failed: %s', msg_id, type(e).__name__)
        return Response(status_code=404)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT', '8080')))


