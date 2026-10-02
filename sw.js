const XOR_KEY = 0xAA;
let meta = null;

self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'INIT_META') {
    meta = event.data.payload;
    if (event.ports && event.ports[0]) {
      event.ports[0].postMessage({ status: 'READY' });
    }
  }
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  if (!url.pathname.endsWith('/stream-video.mp4') || !meta) {
    return;
  }

  event.respondWith((async () => {
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

    // Serve max 2MB per request to enforce strict < 10MB RAM ceiling
    const MAX_CHUNK = 2 * 1024 * 1024;
    end = Math.min(end, start + MAX_CHUNK - 1);
    const contentLength = (end - start) + 1;

    const stream = new ReadableStream({
      async start(controller) {
        let currentPos = start;

        // Locate start chunk in the index map
        let cIdx = 0;
        while (cIdx < chunksMap.length && (chunksMap[cIdx].startOffset + chunksMap[cIdx].size) <= currentPos) {
          cIdx++;
        }

        while (currentPos <= end && cIdx < chunksMap.length) {
          const item = chunksMap[cIdx];
          const chunkInPos = currentPos - item.startOffset;
          const chunkRem = item.size - chunkInPos;
          const toRead = Math.min(chunkRem, (end - currentPos) + 1);

          // Read only the requested byte slice directly from disk
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
