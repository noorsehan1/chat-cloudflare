// ==================== INDEX.JS ====================
// VERSION: 4.0.1

import { ChatServer } from './chat-server.js';
import { GameServer } from './game-server.js';

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      console.log('📨 Request:', pathname);

      // ===== ROOT & HEALTH =====
      if (pathname === "/" || pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          service: "chat-game-server",
          version: "4.0.1",
          endpoints: {
            chat: "/chat/ws",
            game: "/game/ws",
            health: "/health",
            "game-health": "/game/health"
          },
          test: {
            chat: "new WebSocket('wss://your-worker.workers.dev/chat/ws')",
            game: "new WebSocket('wss://your-worker.workers.dev/game/ws')"
          }
        }), {
          headers: { 
            "Content-Type": "application/json",
            "Cache-Control": "no-cache" 
          }
        });
      }

      // ===== CHAT WEBSOCKET =====
      if (pathname === "/chat/ws") {
        console.log('🔗 Chat WebSocket request');
        const chatServer = new ChatServer();
        return await chatServer.fetch(request);
      }

      // ===== GAME WEBSOCKET =====
      if (pathname === "/game/ws" || pathname === "/game/health") {
        console.log('🎮 Game request:', pathname);
        const gameServer = new GameServer(env);
        return await gameServer.fetch(request);
      }

      // ===== 404 =====
      return new Response(JSON.stringify({
        error: "Not Found",
        path: pathname,
        available: ["/", "/health", "/chat/ws", "/game/ws", "/game/health"]
      }), {
        status: 404,
        headers: { "Content-Type": "application/json" }
      });

    } catch (error) {
      console.error('❌ Error:', error);
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
