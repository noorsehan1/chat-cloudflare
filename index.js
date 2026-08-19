// ==================== INDEX.JS ====================
// VERSION: 5.0.0 - OPTIMIZED FOR JAVA CLIENT
// 🔥 URL: wss://your-worker.workers.dev (tanpa /ws)

import { ChatServer } from './chat-server.js';
import { GameServer } from './game-server.js';

// ============================================================
// ✅ INSTANCE (SINGLETON) - 1 INSTANCE SAJA!
// ============================================================
let chatServerInstance = null;
let gameServerInstance = null;

function getChatServer() {
  if (!chatServerInstance) {
    chatServerInstance = new ChatServer();
    chatServerInstance.start();
  }
  return chatServerInstance;
}

function getGameServer(env) {
  if (!gameServerInstance) {
    gameServerInstance = new GameServer(env);
  }
  return gameServerInstance;
}

// ============================================================
// ✅ MAIN WORKER
// ============================================================
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      // ============================================================
      // ✅ ROOT / WEBSOCKET (tanpa /ws)
      // ============================================================
      // 🔥 INI YANG DI PAKAI JAVA: wss://your-worker.workers.dev
      // Tanpa /ws di akhir!
      if (pathname === "/" || pathname === "") {
        const upgrade = request.headers.get("Upgrade");
        
        // Jika ada Upgrade header → WebSocket
        if (upgrade === "websocket") {
          const chat = getChatServer();
          return await chat.fetch(request);
        }
        
        // Jika tidak → Health check
        const chat = getChatServer();
        const game = getGameServer(env);

        return new Response(JSON.stringify({
          status: "online",
          timestamp: Date.now(),
          service: "chat-game-server",
          version: "5.0.0",
          type: "websocket",
          instance: "single",
          endpoints: {
            chat: "wss://your-worker.workers.dev",
            game: "/game/ws",
            health: "/health"
          },
          chat: {
            connections: chat?.wsSet?.size || 0,
            rooms: chat?.rooms?.size || 0,
            users: chat?.roomUsers?.size || 0
          },
          game: {
            connections: game?.wsMap?.size || 0,
            games: game?.activeGames?.size || 0
          }
        }), {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          }
        });
      }

      // ============================================================
      // ✅ CHAT WEBSOCKET (via /ws atau /chat)
      // ============================================================
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/chat/ws") {
        const chat = getChatServer();
        return await chat.fetch(request);
      }

      // ============================================================
      // ✅ GAME WEBSOCKET
      // ============================================================
      if (pathname === "/game/ws") {
        const game = getGameServer(env);
        return await game.fetch(request);
      }

      // ============================================================
      // ✅ HEALTH
      // ============================================================
      if (pathname === "/health") {
        const chat = getChatServer();
        const game = getGameServer(env);

        return new Response(JSON.stringify({
          status: "online",
          timestamp: Date.now(),
          service: "chat-game-server",
          version: "5.0.0",
          chat: {
            connections: chat?.wsSet?.size || 0,
            rooms: chat?.rooms?.size || 0
          },
          game: {
            connections: game?.wsMap?.size || 0,
            games: game?.activeGames?.size || 0
          }
        }), {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          }
        });
      }

      // ============================================================
      // ✅ 404
      // ============================================================
      return new Response(JSON.stringify({
        error: "Not Found",
        path: pathname,
        available: ["/", "/health", "/ws", "/game/ws"]
      }), {
        status: 404,
        headers: { "Content-Type": "application/json" }
      });

    } catch (error) {
      console.error('❌ Error:', error);

      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: error.message || "Unknown error",
        timestamp: Date.now()
      }), {
        status: 500,
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "no-cache"
        }
      });
    }
  }
};

export { ChatServer, GameServer };
