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
      if (pathname === "/health" || pathname === "/") {
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          service: "chat-game-server",
          version: "4.0.1",
          paths: {
            chat: "/chat/ws",
            game: "/game/ws"
          }
        }), {
          headers: { 
            "Content-Type": "application/json", 
            "Cache-Control": "no-cache" 
          },
        });
      }

      // ========== CHAT WEBSOCKET ==========
      if (pathname === "/chat/ws") {
        const chatServer = new ChatServer();
        const response = await chatServer.fetch(request);
        return response;
      }

      // ========== GAME WEBSOCKET ==========
      if (pathname === "/game/ws") {
        const gameServer = new GameServer(env);
        const response = await gameServer.fetch(request);
        return response;
      }

      // ========== 404 ==========
      return new Response(JSON.stringify({
        error: "Not Found",
        path: pathname,
        available: ["/", "/health", "/chat/ws", "/game/ws"]
      }), {
        status: 404,
        headers: { "Content-Type": "application/json" }
      });

    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: error.message || "Unknown error",
        timestamp: Date.now(),
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};
