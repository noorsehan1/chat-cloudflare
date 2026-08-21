// ==================== INDEX.JS ====================
// VERSION: 3.4.0 - SUPPORT WSS:// TANPA PATH

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      // SEMUA WEBSOCKET REQUEST LANGSUNG KE CHAT SERVER
      if (upgrade === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // CHAT SERVER HTTP
      if (pathname === "/reset" || pathname === "/health") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // GAME SERVER
      if (pathname.startsWith("/game")) {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ROOT
      return new Response("Chat Server Running", { 
        status: 200,
        headers: { 'Content-Type': 'text/plain' }
      });
      
    } catch(e) {
      return new Response("Error", { status: 500 });
    }
  }
};

export { ChatServer, GameServer };
