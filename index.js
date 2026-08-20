// ==================== INDEX.JS - VERSION 4.0.0 ====================
// FULL ALARM SYSTEM

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

const instanceCache = new Map();
const CACHE_TTL = 60000;

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
      
      // GAME SERVER
      if (pathname === "/game/ws") {
        const room = url.searchParams.get("room") || "default";
        
        let lastError = null;
        
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            const hash = await hashString(room + attempt);
            const instanceId = Math.abs(hash) % 3;
            
            const cacheKey = `game_${room}_${instanceId}`;
            let cached = instanceCache.get(cacheKey);
            
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
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000);
            
            try {
              const response = await obj.fetch(request, {
                signal: controller.signal
              });
              clearTimeout(timeoutId);
              
              if (response.status === 200 || response.status === 101) {
                return response;
              }
              
              if (response.status === 503 || response.status === 429) {
                throw new Error('Instance busy');
              }
              
              return response;
              
            } catch (error) {
              clearTimeout(timeoutId);
              lastError = error;
              
              if (error.name === 'AbortError' || error.message === 'Instance busy') {
                const badKey = `game_${room}_${instanceId}`;
                instanceCache.delete(badKey);
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
        
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          instances: results,
          totalConnections: results.reduce((sum, r) => sum + (r.connections || 0), 0),
          totalGames: results.reduce((sum, r) => sum + (r.games || 0), 0)
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "4.0.0",
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
  },

  // ✅ SCHEDULED HANDLER UNTUK CRON (OPSIONAL)
  async scheduled(event, env, ctx) {
    // Bisa digunakan untuk trigger manual jika diperlukan
    // Tapi alarm sudah handle semuanya
  }
};

async function hashString(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.reduce((acc, byte) => acc + byte, 0);
}

export { ChatServer, GameServer };
