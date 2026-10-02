const XOR_KEY = 0xAA;
const DB_NAME = 'VLC_PRO_STREAM_DB';
const STORE_NAME = 'meta_store';

self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    req.onsuccess = (e) => resolve(e.target.result);
    req.onerror = (e) => reject(e.target.error);
  });
}

function getStoredMeta() {
  return openDB().then((db) => {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readonly');
      const req = tx.objectStore(STORE_NAME).get('active_stream');
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  });
}

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Intercept the virtual video stream URL
  if (!url.pathname.endsWith('/virtual-stream.mp4')) {
    return;
  }

  event.respondWith((async () => {
    const meta = await getStoredMeta();
    if (!meta) {
      return new Response('No active stream metadata found in storage', { status: 404 });
    }

    const { file, chunksMap, totalSize, mime } = meta;
    const rangeHeader = event.request.headers.get('range');

    let start = 0;
    let end = totalSize - 1;

    if (rangeHeader) {
      const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (match) {
        start = parseInt(match[1], 10);
        if (match[2]) end = parseInt(match[2], 10);
      }
    }

    if (start >= totalSize || end >= totalSize) {
      return new Response(null, {
        status: 416,
        headers: { 'Content-Range': `bytes */${totalSize}` }
      });
    }

    // Serve a bounded 2MB slice per range request so RAM is strictly capped at ~2MB
    const MAX_RANGE_BLOCK = 2 * 1024 * 1024;
    end = Math.min(end, start + MAX_RANGE_BLOCK - 1);
    const contentLength = (end - start) + 1;

    // Stream on-demand chunks directly from disk
    const stream = new ReadableStream({
      async start(controller) {
        let currentPos = start;

        // Binary search / find the starting chunk in the map
        let cIdx = 0;
        while (cIdx < chunksMap.length && (chunksMap[cIdx].startOffset + chunksMap[cIdx].size) <= currentPos) {
          cIdx++;
        }

        while (currentPos <= end && cIdx < chunksMap.length) {
          const item = chunksMap[cIdx];
          const chunkInPos = currentPos - item.startOffset;
          const chunkRem = item.size - chunkInPos;
          const toRead = Math.min(chunkRem, (end - currentPos) + 1);

          // Read only the requested slice directly from the ZIP file on disk
          const sliceStart = item.dataOffset + chunkInPos;
          const raw = await file.slice(sliceStart, sliceStart + toRead).arrayBuffer();

          const u8 = new Uint8Array(raw);
          for (let b = 0; b < u8.length; b++) {
            u8[b] ^= XOR_KEY;
          }

          controller.enqueue(u8);
          currentPos += toRead;
          cIdx++;
        }

        controller.close();
      }
    });

    return new Response(stream, {
      status: 206,
      headers: {
        'Content-Type': mime || 'video/mp4',
        'Content-Range': `bytes ${start}-${end}/${totalSize}`,
        'Content-Length': contentLength.toString(),
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'no-cache'
      }
    });
  })());
});
