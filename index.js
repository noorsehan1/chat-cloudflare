// ==================== INDEX.JS ====================
// VERSION: 3.3.6 - MINIMALIS

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // CHAT SERVER
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/" || pathname === "/reset") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // GAME SERVER
      if (pathname === "/game/ws" || pathname === "/game/health") {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      return new Response("Server running", { 
        status: 200,
        headers: { 'Content-Type': 'text/plain' }
      });
      
    } catch(e) {
      return new Response("Internal Server Error", { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

export { ChatServer, GameServer };
