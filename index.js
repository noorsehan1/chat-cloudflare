// ==================== INDEX.JS ====================
// VERSION: 4.0.0 - PURE WORKER TANPA HIBERNATE

import { ChatServer } from './chat-server.js';
import { GameServer } from './game-server.js';

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      // ========== HEALTH CHECK ==========
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          service: "chat-game-server",
          version: "4.0.0",
        }), {
          headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
        });
      }

      // ========== CHAT WEBSOCKET ==========
      if (pathname === "/chat/ws") {
        const chatServer = new ChatServer();
        return await chatServer.fetch(request);
      }

      // ========== GAME WEBSOCKET ==========
      if (pathname === "/game/ws") {
        const gameServer = new GameServer(env);
        return await gameServer.fetch(request);
      }

      // ========== ROOT ==========
      return new Response("Chat & Game Server Running", {
        status: 200,
        headers: { "Content-Type": "text/plain", "Cache-Control": "no-cache" },
      });

    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: error.message || "Unknown error",
        timestamp: Date.now(),
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
      });
    }
  },
};