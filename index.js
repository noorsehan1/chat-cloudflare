// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

let chatServerInstance = null;
let gameServerInstance = null;
let isShuttingDown = false;

export default {
  async fetch(request, env) {
    try {
      if (isShuttingDown) {
        return new Response("Server is shutting down", { status: 503 });
      }

      const url = new URL(request.url);
      const pathname = url.pathname;

      // ==================== ROOT / CHAT (TAMBAHKAN UNTUK WEBSOCKET DI ROOT) ====================
      // ✅ SEKARANG BISA PAKAI URL TANPA /ws
      if (pathname === "/" || pathname === "/ws" || pathname === "/chat") {
        if (!chatServerInstance) {
          chatServerInstance = new ChatServer(env);
        }
        
        if (!chatServerInstance._initialized) {
          chatServerInstance._initialized = true;
          chatServerInstance._onAlarm().catch(() => {});
        }
        
        return chatServerInstance.fetch(request);
      }

      // ==================== GAME ====================
      if (pathname === "/game" || pathname === "/game/ws") {
        if (!gameServerInstance) {
          gameServerInstance = new GameServer(env);
        }
        return gameServerInstance.fetch(request);
      }

      // ==================== HEALTH ====================
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          chatServer: chatServerInstance ? "active" : "inactive",
          gameServer: gameServerInstance ? "active" : "inactive",
          timestamp: Date.now()
        }), {
          headers: { 
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
          }
        });
      }

      return new Response("Server running", { 
        status: 200,
        headers: {
          "Access-Control-Allow-Origin": "*"
        }
      });

    } catch(e) {
      console.error("Fetch error:", e);
      return new Response("Error: " + e.message, { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    if (isShuttingDown) return;

    try {
      if (chatServerInstance) {
        await chatServerInstance._onAlarm();
      }
    } catch(e) {
      console.error("Scheduled error:", e);
    }
  },

  async shutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;

    if (chatServerInstance) {
      await chatServerInstance.destroy();
      chatServerInstance = null;
    }

    if (gameServerInstance) {
      await gameServerInstance.destroy();
      gameServerInstance = null;
    }
  }
};

export { ChatServer, GameServer };
