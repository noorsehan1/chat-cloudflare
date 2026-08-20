// ==================== INDEX.JS - FIXED ====================
// VERSION: 3.3.1 - SINGLE INSTANCE FOR CHAT & GAME

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
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
        // SINGLE INSTANCE - LANGSUNG PAKAI ID TETAP
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      if (pathname === "/game/health") {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      if (pathname === "/game") {
        return this._handleGameInfo();
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 
          'Retry-After': '30',
          'Content-Type': 'application/json'
        }
      });
    }
  },

  // ========== HANDLE GAME INFO ==========
  _handleGameInfo() {
    return new Response(JSON.stringify({
      status: "running",
      version: "3.3.1",
      instances: 1,
      maxConnections: 150,
      timestamp: Date.now(),
      endpoints: {
        websocket: "/game/ws?room={room_name}",
        health: "/game/health"
      },
      schedule: {
        sessions: [
          { start: "01:00", end: "02:00" },
          { start: "14:00", end: "15:00" },
          { start: "22:00", end: "23:00" }
        ],
        timezone: "WITA (UTC+8)"
      }
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }
};

export { ChatServer, GameServer };
