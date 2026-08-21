// ==================== INDEX.JS ====================
// VERSION: 3.3.8 - SEDERHANA

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      // ========== CHAT SERVER ==========
      // Handle semua request chat (termasuk root untuk WebSocket)
      if (
        pathname === "/ws" || 
        pathname === "/chat" || 
        pathname === "/reset" || 
        pathname === "/health" ||
        (pathname === "/" && upgrade === "websocket") // ← root untuk WebSocket
      ) {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== GAME SERVER ==========
      if (pathname.startsWith("/game")) {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== ROOT (HTTP) ==========
      if (pathname === "/") {
        return new Response("Chat Server Running ✅", { 
          status: 200,
          headers: { 'Content-Type': 'text/plain' }
        });
      }
      
      return new Response("Not Found", { status: 404 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};

export { ChatServer, GameServer };
