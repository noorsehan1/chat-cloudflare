// ==================== INDEX.JS - FULL ROUTER ====================
// VERSION: 4.0.0 - WITH ADMIN ENDPOINTS

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

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
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws" || 
          pathname.startsWith("/admin/") || 
          pathname === "/game/health") {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== ROOT ==========
      return new Response("Server running v4.0.0 - Full Admin", { 
        status: 200,
        headers: { 'Content-Type': 'text/plain' }
      });
      
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

export { ChatServer, GameServer };