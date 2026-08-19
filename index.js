// ==================== INDEX.JS ====================
// ==================== FIXED VERSION ====================

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ==================== INSTANCE GLOBAL ====================
let chatServerInstance = null;
let gameServerInstance = null;
let isShuttingDown = false;

export default {
  async fetch(request, env) {
    try {
      // CEK SHUTDOWN
      if (isShuttingDown) {
        return new Response("Server is shutting down", { status: 503 });
      }

      const url = new URL(request.url);
      const pathname = url.pathname;

      // ==================== ROOT / CHAT ====================
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

      // ==================== HEALTH CHECK ====================
      if (pathname === "/health") {
        const status = {
          status: "ok",
          chatServer: chatServerInstance ? "active" : "inactive",
          gameServer: gameServerInstance ? "active" : "inactive",
          chatConnections: chatServerInstance?.wsSet?.size || 0,
          gameConnections: gameServerInstance?.wsMap?.size || 0,
          gamesRunning: gameServerInstance?.activeGames?.size || 0,
          timestamp: Date.now()
        };
        return new Response(JSON.stringify(status), {
          headers: { 
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
          }
        });
      }

      // ==================== SHUTDOWN ====================
      if (pathname === "/shutdown" && request.method === "POST") {
        await this.shutdown();
        return new Response("Shutting down", { status: 200 });
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

  // ==================== SCHEDULED ====================
  async scheduled(event, env, ctx) {
    if (isShuttingDown) return;

    try {
      if (chatServerInstance) {
        await chatServerInstance._onAlarm();
      }

      if (gameServerInstance) {
        if (typeof gameServerInstance._onScheduled === 'function') {
          await gameServerInstance._onScheduled();
        }
      }
    } catch(e) {
      console.error("Scheduled error:", e);
    }
  },

  // ==================== SHUTDOWN ====================
  async shutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log("Shutting down servers...");

    try {
      if (chatServerInstance) {
        await chatServerInstance.destroy();
        chatServerInstance = null;
      }
    } catch(e) {
      console.error("Chat server destroy error:", e);
    }

    try {
      if (gameServerInstance) {
        await gameServerInstance.destroy();
        gameServerInstance = null;
      }
    } catch(e) {
      console.error("Game server destroy error:", e);
    }

    console.log("Shutdown complete");
  }
};

export { ChatServer, GameServer };
