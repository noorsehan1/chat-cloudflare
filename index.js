// ==================== INDEX.JS - VERSION 5.0 (HIBERNATION READY) ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache Instance (Opsional jika dibutuhkan di handler kustom)
const instanceCache = new Map();
const CACHE_TTL = 60000;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      // ========== CHAT SERVER (1 GLOBAL INSTANCE) ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        
        // Langsung teruskan request ke ChatServer Durable Object
        return obj.fetch(request);
      }

      // ========== GAME SERVER (1 SINGLE INSTANCE) ==========
      if (pathname === "/game/ws") {
        try {
          const id = env.GAME_SERVER.idFromName("game_main");
          const obj = env.GAME_SERVER.get(id);

          // PENTING: Jangan gunakan AbortController/Signal pada WebSocket Handshake!
          // WebSocket Upgrade memerlukan pass-through murni agar Hibernation API bekerja presisi.
          return await obj.fetch(request);

        } catch (error) {
          return new Response(JSON.stringify({
            error: "Game server busy or initializing, please retry",
            retryAfter: 3
          }), { 
            status: 503,
            headers: { 
              'Retry-After': '3',
              'Content-Type': 'application/json'
            }
          });
        }
      }

      // ========== HEALTH CHECK ROUTE ==========
      if (pathname === "/game/health") {
        try {
          const id = env.GAME_SERVER.idFromName("game_main");
          const obj = env.GAME_SERVER.get(id);

          // Panggilan HTTP standar boleh menggunakan AbortSignal timeout
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
          }), { 
            status: 200,
            headers: { 'Content-Type': 'application/json' } 
          });

        } catch (e) {
          return new Response(JSON.stringify({ 
            status: "error",
            message: e.message || "Health check failed"
          }), { 
            status: 503,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // ========== INFO ROUTE ==========
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "5.0.0-hibernation",
          instance: "game_main",
          maxConnections: 150,
          timestamp: Date.now()
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // Default Response untuk Root Path Non-WS
      return new Response("Server running", { status: 200 });

    } catch (e) {
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 
          'Retry-After': '5',
          'Content-Type': 'application/json'
        }
      });
    }
  }
};

// Re-export Class Durable Object agar Dikenali oleh Wrangler/Cloudflare System
export { ChatServer, GameServer };
