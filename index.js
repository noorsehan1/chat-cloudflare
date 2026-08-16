// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance management
const instanceCache = new Map();
const CACHE_TTL = 60000; // 1 menit

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== GAME SERVER - WebSocket dengan Load Balancing ==========
      if (pathname === "/game/ws") {
        // Ambil room dari query parameter untuk distribusi
        const room = url.searchParams.get("room") || "default";
        
        // Dapatkan instance yang sesuai dengan room
        const obj = await getGameInstance(room, env);
        
        // 🔥 KRITICAL: Tambahkan timeout untuk mencegah hang
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
          controller.abort();
        }, 3000); // 3 detik timeout
        
        try {
          const response = await obj.fetch(request, {
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          return response;
        } catch (error) {
          clearTimeout(timeoutId);
          if (error.name === 'AbortError') {
            return new Response(JSON.stringify({
              error: "Server busy, please retry",
              retryAfter: 5
            }), { 
              status: 503,
              headers: { 
                'Retry-After': '5',
                'Content-Type': 'application/json'
              }
            });
          }
          throw error;
        }
      }
      
      // ========== GAME SERVER - Health Check ==========
      if (pathname === "/game/health") {
        try {
          // Cek beberapa instance
          const instances = [];
          for (let i = 0; i < 5; i++) {
            try {
              const id = env.GAME_SERVER.idFromName(`game_${i}`);
              const obj = env.GAME_SERVER.get(id);
              const resp = await obj.fetch(new Request("https://dummy/health"), {
                signal: AbortSignal.timeout(2000)
              });
              if (resp.ok) {
                const data = await resp.json();
                instances.push({ 
                  id: i, 
                  status: "healthy",
                  connections: data.connections || 0,
                  games: data.games || 0,
                  queue: data.queue || 0
                });
              } else {
                instances.push({ id: i, status: "unhealthy" });
              }
            } catch(e) {
              instances.push({ id: i, status: "error", error: e.message });
            }
          }
          
          return new Response(JSON.stringify({
            status: "ok",
            timestamp: Date.now(),
            instances: instances,
            totalConnections: instances.reduce((sum, inst) => sum + (inst.connections || 0), 0),
            totalGames: instances.reduce((sum, inst) => sum + (inst.games || 0), 0)
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({
            status: "degraded",
            error: e.message
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // ========== GAME SERVER - Status ==========
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "1.0.0",
          timestamp: Date.now(),
          endpoints: {
            websocket: "/game/ws?room={room_name}",
            health: "/game/health"
          },
          instances: 5
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== DEFAULT ==========
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 
          'Retry-After': '30',
          'Content-Type': 'application/json'
        }
      });
    }
  }
};

// ========== HELPER: Get Game Instance dengan Caching ==========
async function getGameInstance(room, env) {
  const cacheKey = `game_${room}`;
  const cached = instanceCache.get(cacheKey);
  
  // Gunakan cache jika masih valid
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.instance;
  }
  
  // Hash room untuk distribusi ke 5 instance
  const hash = await hashString(room);
  const instanceId = Math.abs(hash) % 5; // 5 instance game
  
  // Buat ID dan dapatkan object
  const id = env.GAME_SERVER.idFromName(`game_${instanceId}`);
  const obj = env.GAME_SERVER.get(id);
  
  // Simpan di cache
  instanceCache.set(cacheKey, {
    instance: obj,
    timestamp: Date.now()
  });
  
  // Cleanup cache periodically
  if (instanceCache.size > 100) {
    const now = Date.now();
    for (const [key, value] of instanceCache) {
      if (now - value.timestamp > CACHE_TTL) {
        instanceCache.delete(key);
      }
    }
  }
  
  return obj;
}

// ========== HELPER: Simple Hash Function ==========
async function hashString(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.reduce((acc, byte) => acc + byte, 0);
}

export { ChatServer, GameServer };
