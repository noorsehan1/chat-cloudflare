// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

let chatInstance = null;
let gameInstance = null;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // ✅ CHAT SERVER
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/" || pathname === "/test" || pathname === "/status") {
      if (!chatInstance) {
        chatInstance = new ChatServer(env);
      }
      return chatInstance.fetch(request);
    }

    // ✅ GAME SERVER
    if (pathname === "/game" || pathname === "/game/ws" || pathname === "/game/health") {
      if (!gameInstance) {
        gameInstance = new GameServer(env);
      }
      return gameInstance.fetch(request);
    }

    return new Response("Server running", { status: 200 });
  }
};

export { ChatServer, GameServer };
