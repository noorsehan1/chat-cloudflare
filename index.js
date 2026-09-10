// ==================== INDEX.JS - SIMPLE ROUTER ====================
// VERSION: 4.0.0 - SIMPLE

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // ========== CHAT SERVER ==========
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      const id = env.CHAT_SERVER.idFromName("global");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(request);
    }

    // ========== GAME SERVER ==========
    if (pathname === "/game/ws") {
      const id = env.GAME_SERVER.idFromName("game");
      const obj = env.GAME_SERVER.get(id);
      return obj.fetch(request);
    }

    // ========== FALLBACK ==========
    return new Response("Server running", {
      status: 200,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
};

export { ChatServer, GameServer };
