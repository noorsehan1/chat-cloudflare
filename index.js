// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      if (pathname === "/game/ws" || pathname === "/game") {
        const id = env.GAME_SERVER.idFromName("global");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};

export { ChatServer, GameServer };
