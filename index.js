// ==================== INDEX.JS - FIXED ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance
const instanceCache = new Map();
const CACHE_TTL = 60000; // 1 menit

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // CHAT SERVER
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== ✅ GAME SERVER - FIXED ==========
      if (pathname === "/game/ws") {
        // Ambil room dari query parameter
        const room = url.searchParams.get("room") || "default";
        
        // ✅ Coba semua instance (max 3 percobaan)
        let lastError = null;
        
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            // Hash dengan attempt untuk distribusi
            const hash = await hashString(room + attempt);
            const instanceId = Math.abs(hash) % 3;
            
            // ✅ Cache key gabung room + instanceId
            const cacheKey = `game_${room}_${instanceId}`;
            let cached = instanceCache.get(cacheKey);
            
            // ✅ Cek cache expired
            if (cached && (Date.now() - cached.timestamp > CACHE_TTL)) {
              instanceCache.delete(cacheKey);
              cached = null;
            }
            
            let obj;
            if (cached) {
              obj = cached.instance;
            } else {
              const id = env.GAME_SERVER.idFromName(`game_${instanceId}`);
              obj = env.GAME_SERVER.get(id);
              instanceCache.set(cacheKey, {
                instance: obj,
                timestamp: Date.now()
              });
            }
            
            // ✅ TIMEOUT 3 DETIK
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000);
            
            try {
              const response = await obj.fetch(request, {
                signal: controller.signal
              });
              clearTimeout(timeoutId);
              
              // ✅ Jika response sukses, return
              if (response.status === 200 || response.status === 101) {
                return response;
              }
              
              // Jika error, coba instance lain
              if (response.status === 503 || response.status === 429) {
                throw new Error('Instance busy');
              }
              
              return response;
              
            } catch (error) {
              clearTimeout(timeoutId);
              lastError = error;
              
              // Jika timeout atau busy, coba instance lain
              if (error.name === 'AbortError' || error.message === 'Instance busy') {
                // Hapus cache yang bermasalah
                const badKey = `game_${room}_${instanceId}`;
                instanceCache.delete(badKey);
                console.log(`Instance ${instanceId} busy, retrying... (${attempt + 1}/3)`);
                continue;
              }
              throw error;
            }
            
          } catch (error) {
            lastError = error;
            if (attempt === 2) throw error;
            await new Promise(resolve => setTimeout(resolve, 100));
          }
        }
        
        // Jika semua retry gagal
        return new Response(JSON.stringify({
          error: "All game servers busy, please retry",
          retryAfter: 5
        }), { 
          status: 503,
          headers: { 
            'Retry-After': '5',
            'Content-Type': 'application/json'
          }
        });
      }
      
      if (pathname === "/game/health") {
        // Health check
        const results = [];
        for (let i = 0; i < 3; i++) {
          try {
            const id = env.GAME_SERVER.idFromName(`game_${i}`);
            const obj = env.GAME_SERVER.get(id);
            const resp = await obj.fetch(new Request("https://dummy/health"), {
              signal: AbortSignal.timeout(2000)
            });
            if (resp.ok) {
              const data = await resp.json();
              results.push({ 
                id: i, 
                status: "healthy", 
                connections: data.connections || 0,
                games: data.games || 0,
                queue: data.queue || 0
              });
            } else {
              results.push({ id: i, status: "unhealthy" });
            }
          } catch(e) {
            results.push({ id: i, status: "error", error: e.message });
          }
        }
        
        const totalConnections = results.reduce((sum, r) => sum + (r.connections || 0), 0);
        
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          instances: results,
          totalConnections: totalConnections,
          totalGames: results.reduce((sum, r) => sum + (r.games || 0), 0)
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "3.0.7",
          instances: 3,
          maxConnections: 150,
          timestamp: Date.now(),
          endpoints: {
            websocket: "/game/ws?room={room_name}",
            health: "/game/health"
          }
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
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

// Helper hash
async function hashString(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.reduce((acc, byte) => acc + byte, 0);
}

export { ChatServer, GameServer };
