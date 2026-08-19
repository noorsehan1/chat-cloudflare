// ==================== INDEX.JS - VERSION 4.0 (STABLE 24H) ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache kecil
const instanceCache = new Map();
const CACHE_TTL = 60000;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== CHAT SERVER (1 INSTANCE) ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== GAME SERVER - 1 INSTANCE SAJA ==========
      if (pathname === "/game/ws") {
        // ✅ HANYA 1 INSTANCE - TIDAK ADA RETRY
        const room = url.searchParams.get("room") || "default";
        
        try {
          const id = env.GAME_SERVER.idFromName("game_main");
          const obj = env.GAME_SERVER.get(id);
          
          // Timeout 2 detik saja
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 2000);
          
          const response = await obj.fetch(request, {
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          
          return response;
          
        } catch (error) {
          return new Response(JSON.stringify({
            error: "Game server busy, please retry",
            retryAfter: 5
          }), { 
            status: 503,
            headers: { 
              'Retry-After': '5',
              'Content-Type': 'application/json'
            }
          });
        }
      }
      
      // Health check
      if (pathname === "/game/health") {
        try {
          const id = env.GAME_SERVER.idFromName("game_main");
          const obj = env.GAME_SERVER.get(id);
          const resp = await obj.fetch(new Request("https://dummy/health"), {
            signal: AbortSignal.timeout(1500)
          });
          const data = await resp.json();
          return new Response(JSON.stringify({
            status: "ok",
            instance: "game_main",
            connections: data.connections || 0,
            games: data.games || 0,
            queue: data.queue || 0
          }), { headers: { 'Content-Type': 'application/json' } });
        } catch(e) {
          return new Response(JSON.stringify({ 
            status: "error",
            message: e.message 
          }), { 
            status: 503,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "4.0.0",
          instance: "game_main",
          maxConnections: 50,
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
        headers: { 
          'Retry-After': '30',
          'Content-Type': 'application/json'
        }
      });
    }
  }
};

export { ChatServer, GameServer };
