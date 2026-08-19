// ==================== INDEX.JS - PURE WORKER SIMPLE ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ========== SINGLETON ==========
let chatServer = null;
let gameServer = null;
let initialized = false;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== INIT ONCE ==========
      if (!initialized) {
        // Chat Server
        chatServer = new ChatServer({
          env: env,
          state: {
            storage: {
              get: async (key) => null,
              put: async (key, value) => {},
              delete: async (key) => {},
              setAlarm: async (ms) => {}
            }
          },
          ctx: {
            acceptWebSocket: (ws) => {
              try { ws.accept(); } catch(e) {}
            }
          }
        });
        
        // Game Server
        gameServer = new GameServer({
          env: env,
          state: {
            storage: {
              get: async (key) => null,
              put: async (key, value) => {},
              delete: async (key) => {}
            }
          }
        });
        
        initialized = true;
      }
      
      // ========== ROUTING ==========
      // Chat WebSocket
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return chatServer.fetch(request);
      }
      
      // Game WebSocket
      if (pathname === "/game/ws" || pathname === "/game") {
        return gameServer.fetch(request);
      }
      
      // Health check
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "running",
          chatConnections: chatServer?.wsSet?.size || 0,
          gameConnections: gameServer?.wsMap?.size || 0,
          rooms: chatServer?.rooms?.size || 0,
          games: gameServer?.activeGames?.size || 0,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response(JSON.stringify({
        error: e.message || "Internal Server Error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};
