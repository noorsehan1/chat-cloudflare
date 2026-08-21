// ==================== INDEX.JS ====================
// VERSION: 3.4.3 - WITH GAME SERVER

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // === GAME SERVER ===
      if (pathname.startsWith("/game")) {
        if (!env.GAME_SERVER) {
          return new Response("GAME_SERVER binding not found", { status: 500 });
        }
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // === CHAT SERVER (SEMUA REQUEST LAIN) ===
      if (!env.CHAT_SERVER) {
        return new Response("CHAT_SERVER binding not found", { status: 500 });
      }
      const id = env.CHAT_SERVER.idFromName("global");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(request);
      
    } catch(error) {
      return new Response("Error: " + error.message, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

export { ChatServer, GameServer };
