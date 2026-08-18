// ==================== INDEX.JS - FIXED ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance
const instanceCache = new Map();

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
        
        // Hash untuk distribusi ke 3 instance
        const hash = await hashString(room);
        const instanceId = Math.abs(hash) % 3; // 3 INSTANCE!
        
        const cacheKey = `game_${instanceId}`;
        let obj = instanceCache.get(cacheKey);
        
        if (!obj) {
          const id = env.GAME_SERVER.idFromName(`game_${instanceId}`);
          obj = env.GAME_SERVER.get(id);
          instanceCache.set(cacheKey, obj);
        }
        
        // ✅ TIMEOUT 3 DETIK!
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);
        
        try {
          const response = await obj.fetch(request, {
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          return response;
        } catch (error) {
          clearTimeout(timeoutId);
          if (error.name === 'AbortError') {
            return new Response("Server busy, please retry", { 
              status: 503,
              headers: { 'Retry-After': '5' }
            });
          }
          throw error;
        }
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
              results.push({ id: i, status: "healthy", ...data });
            } else {
              results.push({ id: i, status: "unhealthy" });
            }
          } catch(e) {
            results.push({ id: i, status: "error" });
          }
        }
        return new Response(JSON.stringify({
          status: "ok",
          instances: results,
          totalConnections: results.reduce((sum, r) => sum + (r.connections || 0), 0)
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          instances: 3,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
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