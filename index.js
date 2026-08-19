// ==================== INDEX.JS - PURE WORKER ====================
import { getGameServer } from "./game-server.js";
import { getChatServer } from "./chat-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // CHAT SERVER - pure worker
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const chatServer = getChatServer(env);
        return chatServer.fetch(request);
      }
      
      // GAME SERVER - pure worker
      if (pathname === "/game/ws" || pathname === "/game") {
        const gameServer = getGameServer(env);
        return gameServer.fetch(request);
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};

export { GameServer };
