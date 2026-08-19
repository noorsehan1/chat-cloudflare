// ==================== INDEX.JS ====================
// VERSION: 6.0.0 - GABUNGAN CHAT + GAME (1 INSTANCE)
// 🔥 SEMUA USER DI 1 TEMPAT → REALTIME!

import { GameServer } from './game-server.js';

// ============================================================
// ✅ INSTANCE (SINGLETON) - 1 INSTANCE SAJA!
// ============================================================
let serverInstance = null;

function getServer(env) {
  if (!serverInstance) {
    serverInstance = new GameServer(env);
  }
  return serverInstance;
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
      // ✅ ROOT / HEALTH
      // ============================================================
      if (pathname === "/" || pathname === "/health") {
        const server = getServer(env);

        return new Response(JSON.stringify({
          status: "online",
          timestamp: Date.now(),
          service: "chat-game-server",
          version: "6.0.0",
          type: "websocket",
          instance: "single",
          endpoints: {
            chat: "/ws",
            game: "/game/ws",
            health: "/health"
          },
          connections: server?.wsMap?.size || 0,
          games: server?.activeGames?.size || 0,
          rooms: server?.wsClients?.size || 0,
          chatUsers: server?.roomUsers?.size || 0,
          diceActive: server?.currentDiceRoll ? true : false,
          dicePoints: server?.dicePoints?.size || 0,
          uptime: server ? Math.floor((Date.now() - server._startTime) / 1000) : 0
        }), {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          }
        });
      }

      // ============================================================
      // ✅ CHAT WEBSOCKET (Untuk App Inventor - Websocketvalf)
      // ============================================================
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/chat/ws") {
        const server = getServer(env);
        return server.handleChatWebSocket(request);
      }

      // ============================================================
      // ✅ GAME WEBSOCKET (Untuk Lowcard + Dice)
      // ============================================================
      if (pathname === "/game/ws") {
        const server = getServer(env);
        return server.handleGameWebSocket(request);
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

export { GameServer };
