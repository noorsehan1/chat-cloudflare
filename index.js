// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // Route ke ChatServer
      if (pathname === "/ws" || pathname === "/chat") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // Route ke GameServer
      if (pathname === "/game/ws" || pathname === "/game") {
        const id = env.GAME_SERVER.idFromName("global");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response(e.message, { status: 500 });
    }
  }
};

// Durable Object exports
export { ChatServer, GameServer };
