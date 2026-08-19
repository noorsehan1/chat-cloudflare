// ==================== INDEX.JS ====================
// VERSION: 4.0.0 - ROUTER UTAMA

import { ChatServer } from "./chat.js";
import { GameServer } from "./game.js";

// ==================== INSTANCE CACHE ====================
let chatInstance = null;
let gameInstance = null;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // ============================================================
    // CHAT SERVER - /ws, /chat, /
    // ============================================================
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      if (!chatInstance) {
        chatInstance = new ChatServer(env);
      }
      return chatInstance.fetch(request);
    }

    // ============================================================
    // GAME SERVER - /game, /game/ws
    // ============================================================
    if (pathname === "/game" || pathname === "/game/ws" || pathname === "/game/health") {
      if (!gameInstance) {
        gameInstance = new GameServer(env);
      }
      return gameInstance.fetch(request);
    }

    // ============================================================
    // STATUS
    // ============================================================
    if (pathname === "/status") {
      return new Response(JSON.stringify({
        status: "ok",
        chat: chatInstance ? "active" : "inactive",
        game: gameInstance ? "active" : "inactive",
        timestamp: Date.now()
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response("Server running", { status: 200 });
  }
};
