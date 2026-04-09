const CACHE_NAME = 'v205-cache-v1';
const URLS = ['./v205_fixed_url_app.html','./manifest.json','./service-worker.js'];
self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(URLS)).then(()=>self.skipWaiting()));
});
self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(keys.map(k => k !== CACHE_NAME ? caches.delete(k) : null))).then(()=>self.clients.claim()));
});
self.addEventListener('fetch', event => {
  event.respondWith(caches.match(event.request).then(resp => resp || fetch(event.request).catch(() => caches.match('./v205_fixed_url_app.html'))));
});
