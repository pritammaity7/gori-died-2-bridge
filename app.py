"""Telegram streaming bridge - optimized build (2026-08-28).

Serves video/PDF bytes from the "My courses" group over HTTP with Range support.

Optimizations over the first version, driven by measurements + the official
MTProto file docs (core.telegram.org/api/files):

1. BOT POOL. One bot connection was the throughput ceiling: a single stream got
   3.4 MB/s while two parallel streams got 6.4 MB/s combined. Requests now
   round-robin across every configured token, so opening a second video no
   longer starves the first.
2. 1 MB-ALIGNED READS. Telegram requires a requested part to sit within one
   1 MB chunk. We floor the offset to a 1 MB boundary and discard the leading
   bytes; misaligned reads previously made Telegram re-serve data.
3. WARM START. All clients connect at startup and a keepalive keeps the MTProto
   sessions hot, removing the cold ~1s penalty on the first request.
4. METADATA CACHE. Message lookups are cached, so seeks skip get_messages.
5. HEAD CACHE. The first N MB of each file (ftyp + moov + opening seconds) is
   kept in memory. The moov atom in this library is 5-7 MB, so this is what
   removes the black screen before the first frame.

Configuration comes from environment variables only. No credentials in code.
Env: API_ID, API_HASH, BOT_TOKENS (comma separated), BRIDGE_TOKEN, PEER,
     HEAD_CACHE_MB (default 10), HEAD_CACHE_FILES (default 12)
"""
import asyncio
import logging
import os
import re
import time
from collections import OrderedDict
from itertools import cycle
from urllib.parse import quote

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from telethon import TelegramClient, connection, errors

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('bridge')

API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
BOT_TOKENS = [t.strip() for t in os.getenv('BOT_TOKENS', '').split(',') if t.strip()]
BRIDGE_TOKEN = os.getenv('BRIDGE_TOKEN', '')
DEFAULT_PEER = os.getenv('PEER', '')

MB = 1024 * 1024
ALIGN = MB                      # Telegram 1 MB chunk rule
CHUNK = MB                      # request_size handed to Telethon
META_TTL = 6 * 3600
HEAD_MB = int(os.getenv('HEAD_CACHE_MB', '10'))
HEAD_FILES = int(os.getenv('HEAD_CACHE_FILES', '12'))

app = FastAPI(title='SSC courses stream bridge', docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'],
                   allow_headers=['*'],
                   expose_headers=['Content-Range', 'Accept-Ranges',
                                   'Content-Length', 'Content-Type'])

clients = []
_rr = None
_meta = {}
_heads = OrderedDict()          # (peer, msg_id) -> bytes (first HEAD_MB)
_head_locks = {}
_stats = {'requests': 0, 'head_hits': 0, 'meta_hits': 0, 'parallel_heads': 0}


def _peer(value):
    v = (value or DEFAULT_PEER or '').strip()
    if not v:
        return None
    return int(v) if v.lstrip('-').isdigit() else v


def pick():
    """Round-robin over connected bots so concurrent viewers never share one."""
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
        # touch the peer once so the entity + DC handshake are already cached
        if DEFAULT_PEER:
            try:
                await c.get_entity(_peer(''))
            except Exception:
                pass
    except Exception as e:
        log.error('worker %d failed: %s: %s', i, type(e).__name__, e)


async def _keepalive():
    """Cheap periodic ping: keeps MTProto sessions warm so the first real
    request does not pay connection setup (~1s in measurements)."""
    while True:
        await asyncio.sleep(240)
        for c in clients:
            try:
                if c.is_connected():
                    await c.get_me()
            except Exception:
                pass


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
    asyncio.create_task(_keepalive())
    log.info('pool size: %d bots | head cache: %d MB x %d files',
             len(BOT_TOKENS), HEAD_MB, HEAD_FILES)


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
        'status': 'ssc-courses bridge up',
        'workers_total': len(clients),
        'workers_connected': sum(1 for c in clients if c.is_connected()),
        'cached_meta': len(_meta),
        'cached_heads': len(_heads),
        'stats': _stats,
    }


@app.get('/health')
async def health():
    ok = any(c.is_connected() for c in clients)
    return JSONResponse({'ok': ok, 'workers': sum(1 for c in clients if c.is_connected())},
                        status_code=200 if ok else 503)


async def _media(worker, peer, msg_id, force=False):
    """(media, size, mime, filename), cached PER BOT.

    Critical: a media object is only valid for the account that fetched it.
    Telegram's docs are explicit - "the access_hash will always be the same for
    a given account, but different accounts will each see their own, different
    access_hash... it is impossible to get a media object from one account and
    use it in another". file_reference also expires after a few hours.
    So the cache key includes the worker, and callers retry with force=True on
    FileReferenceExpiredError.
    """
    key = (id(worker), str(peer), msg_id)
    hit = _meta.get(key)
    if hit and not force and time.time() - hit[0] < META_TTL:
        _stats['meta_hits'] += 1
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
    _meta[key] = (time.time(), media, size, mime, name)
    return media, size, mime, name



async def _aligned_read(worker, peer, msg_id, media, start, want):
    """Read `want` bytes from `start`, honouring Telegram's 1 MB chunk rule.

    Telegram requires the requested part to sit within a single 1 MB chunk, so
    we floor the offset to a 1 MB boundary and drop the leading bytes.
    A stale file_reference is refreshed once and the read restarts.
    """
    base = (start // ALIGN) * ALIGN
    skip = start - base
    sent = 0
    for attempt in (0, 1):
        try:
            async for chunk in worker.iter_download(media, offset=base, request_size=CHUNK):
                if skip:
                    if len(chunk) <= skip:
                        skip -= len(chunk)
                        continue
                    chunk = chunk[skip:]
                    skip = 0
                if sent + len(chunk) >= want:
                    yield chunk[:want - sent]
                    return
                yield chunk
                sent += len(chunk)
            return
        except errors.FileReferenceExpiredError:
            if attempt or sent:
                raise
            log.info('file_reference expired for %s, refreshing', msg_id)
            media, _, _, _ = await _media(worker, peer, msg_id, force=True)
            if media is None:
                raise
            skip = start - base


async def _parallel_head(peer, msg_id, size, want):
    """Fetch the opening `want` bytes using SEVERAL bots at once.

    Research (FastTelethon / mautrix-telegram parallel_file_transfer, plus
    core.telegram.org/api/files) shows a single MTProto sender is the throughput
    ceiling: the reference implementation opens up to 20 senders per file and
    strides their offsets. We do the same shape with the bots we already have -
    each bot fetches a different 1 MB-aligned slice concurrently, then the
    slices are concatenated in order. No extra accounts needed.
    """
    live = [c for c in clients if c.is_connected()]
    if not live:
        raise RuntimeError('no workers')
    slices = []
    off = 0
    while off < want:
        n = min(ALIGN, want - off)
        slices.append((off, n))
        off += n
    workers = (live * ((len(slices) // len(live)) + 1))[:len(slices)]

    async def one(w, start, length):
        media, _, _, _ = await _media(w, peer, msg_id)
        if media is None:
            raise RuntimeError('not found')
        buf = bytearray()
        async for chunk in _aligned_read(w, peer, msg_id, media, start, length):
            buf += chunk
            if len(buf) >= length:
                break
        return bytes(buf[:length])

    parts = await asyncio.gather(*[one(w, s, n) for w, (s, n) in zip(workers, slices)])
    return b''.join(parts)


async def _get_head(worker, peer, msg_id, media, size):
    """Cache the first HEAD_MB of a file: ftyp + moov (5-7 MB here) + opening
    seconds. This is what removes the wait before the first frame.

    Keyed by peer+msg only - the bytes are account-independent even though the
    media object is not, so one fetch serves every bot in the pool.
    """
    key = (str(peer), msg_id)
    cached = _heads.get(key)
    if cached is not None:
        _heads.move_to_end(key)
        _stats['head_hits'] += 1
        return cached

    lock = _head_locks.setdefault(key, asyncio.Lock())
    async with lock:
        cached = _heads.get(key)
        if cached is not None:
            return cached
        want = min(HEAD_MB * MB, size)
        try:
            data = await _parallel_head(peer, msg_id, size, want)
            _stats['parallel_heads'] += 1
        except Exception as e:
            log.info('parallel head failed (%s), falling back to single sender',
                     type(e).__name__)
            buf = bytearray()
            async for chunk in _aligned_read(worker, peer, msg_id, media, 0, want):
                buf += chunk
            data = bytes(buf)
        _heads[key] = data
        while len(_heads) > HEAD_FILES:
            _heads.popitem(last=False)
        _head_locks.pop(key, None)
        return data



@app.api_route('/stream/{msg_id}', methods=['GET', 'HEAD'])
async def stream(msg_id: int, request: Request, peer: str = '', token: str = '',
                 dl: int = 0):
    """Range-capable byte stream. dl=1 sets an attachment disposition."""
    _stats['requests'] += 1
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

    # HTTP headers must be latin-1 encodable. Many files here have emoji and
    # non-Latin characters in their names (e.g. "MAURYAN EMPIRE BY VIVID ..."),
    # which made uvicorn raise while writing the response -> HTTP 500.
    # RFC 5987 filename* carries the real name safely; filename= keeps an ASCII
    # fallback for old clients.
    raw_name = name or f'file_{msg_id}'
    ascii_name = raw_name.encode('ascii', 'ignore').decode().replace('"', '').strip() \
        or f'file_{msg_id}'
    disp = 'attachment' if dl else 'inline'
    base = {
        'Accept-Ranges': 'bytes',
        'Content-Type': mime,
        'Content-Disposition': (f'{disp}; filename="{ascii_name}"; '
                                f"filename*=UTF-8''{quote(raw_name)}"),
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

    # Serve the opening bytes from the in-memory head whenever the request
    # starts inside that window - including a plain full-file GET, which is what
    # a browser issues first. Previously this only applied to small ranges, so
    # the common case still waited on Telegram.
    head = None
    if start < HEAD_MB * MB:
        try:
            head = await _get_head(worker, target, msg_id, media, size)
        except Exception as e:
            log.info('head cache miss %s: %s', msg_id, type(e).__name__)

    async def sender():
        if head is not None and start < len(head):
            yield head[start:start + want]
            served = min(want, len(head) - start)
            if served >= want:
                return
            async for chunk in _aligned_read(worker, target, msg_id, media,
                                             start + served, want - served):
                yield chunk
            return
        try:
            async for chunk in _aligned_read(worker, target, msg_id, media, start, want):
                yield chunk
        except Exception as e:
            log.info('stream %s ended: %s', msg_id, type(e).__name__)

    return StreamingResponse(sender(), status_code=206 if rng else 200, headers={
        **base,
        'Content-Length': str(want),
        **({'Content-Range': f'bytes {start}-{end}/{size}'} if rng else {}),
    })


@app.get('/warm/{msg_id}')
async def warm(msg_id: int, peer: str = '', token: str = ''):
    """Prefetch a file's head into cache so playback starts instantly.

    The site calls this when a watch page opens and for the next lesson, so the
    5-7 MB moov atom is already in memory before Play is pressed.
    """
    if not _auth(token):
        return Response('unauthorized', status_code=401)
    worker = pick()
    if worker is None:
        return JSONResponse({'warmed': False, 'reason': 'no workers'}, status_code=503)
    target = _peer(peer)
    if target is None:
        return Response('peer not configured', status_code=400)
    try:
        media, size, _, _ = await _media(worker, target, msg_id)
        if media is None:
            return JSONResponse({'warmed': False, 'reason': 'not found'}, status_code=404)
        data = await _get_head(worker, target, msg_id, media, size)
        return {'warmed': True, 'bytes': len(data), 'of': size}
    except Exception as e:
        return JSONResponse({'warmed': False, 'reason': type(e).__name__}, status_code=502)


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



