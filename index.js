// ==================== INDEX.JS - FIXED ====================
// VERSION: 3.2.0 - ALARM SYSTEM + OPTIMIZED CACHE

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance (dengan TTL)
const instanceCache = new Map();
const CACHE_TTL = 30000; // 30 detik (lebih pendek)

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
      
      // ========== GAME SERVER - OPTIMIZED ==========
      if (pathname === "/game/ws") {
        const room = url.searchParams.get("room") || "default";
        
        // Hash untuk distribusi (3 instance)
        const hash = await hashString(room);
        const instanceId = Math.abs(hash) % 3;
        
        const cacheKey = `game_${room}_${instanceId}`;
        let cached = instanceCache.get(cacheKey);
        
        // Cek cache expired
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
          return response;
        } catch (error) {
          clearTimeout(timeoutId);
          
          // Jika timeout, coba instance lain
          if (error.name === 'AbortError') {
            const fallbackId = (instanceId + 1) % 3;
            const fallbackObj = env.GAME_SERVER.get(
              env.GAME_SERVER.idFromName(`game_${fallbackId}`)
            );
            return fallbackObj.fetch(request);
          }
          throw error;
        }
      }
      
      // ========== HEALTH CHECK ==========
      if (pathname === "/game/health") {
        const results = [];
        for (let i = 0; i < 3; i++) {
          try {
            const id = env.GAME_SERVER.idFromName(`game_${i}`);
            const obj = env.GAME_SERVER.get(id);
            const resp = await obj.fetch(
              new Request("https://dummy/health"),
              { signal: AbortSignal.timeout(2000) }
            );
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
          version: "3.2.0",
          instances: 3,
          maxConnections: 150,
          timestamp: Date.now()
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
        headers: { 'Content-Type': 'application/json' }
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
