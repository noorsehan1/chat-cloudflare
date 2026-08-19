// ==================== INDEX.JS ====================
// VERSION: 8.0.0 - PURE WORKER (NO DO)
// ROUTER UNTUK CHAT SERVER & GAME SERVER

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ✅ INSTANCE SINGLETON CHAT SERVER
const chatServer = new ChatServer();

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ============================================================
      // ✅ CHAT SERVER - PURE WORKER
      // ============================================================
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        // ✅ START INTERVAL SAAT PERTAMA KALI DIPANGGIL
        chatServer.start();
        return chatServer.fetch(request);
      }
      
      // ============================================================
      // ✅ GAME SERVER - PURE WORKER
      // ============================================================
      if (pathname === "/game/ws") {
        return GameServer.fetch(request, env);
      }
      
      if (pathname === "/game/health") {
        return GameServer.fetch(request, env);
      }
      
      // ============================================================
      // ✅ HEALTH CHECK
      // ============================================================
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: chatServer.wsSet?.size || 0,
          rooms: chatServer.rooms?.size || 0,
          users: chatServer.userSeat?.size || 0,
          timestamp: Date.now(),
          uptime: Math.floor((Date.now() - chatServer._startTime) / 1000)
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      // ============================================================
      // ✅ GAME INFO
      // ============================================================
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "8.0.0",
          type: "pure-worker",
          maxConnections: 300,
          timestamp: Date.now(),
          endpoints: {
            chat: "/ws",
            game: "/game/ws?room={room_name}",
            health: "/health",
            stats: "/stats",
            gameHealth: "/game/health"
          }
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      }
      
      // ============================================================
      // ✅ STATS
      // ============================================================
      if (pathname === "/stats") {
        const roomStats = {};
        for (const [room, clients] of chatServer.roomClients) {
          roomStats[room] = clients?.size || 0;
        }
        
        return new Response(JSON.stringify({
          connections: chatServer.wsSet?.size || 0,
          rooms: chatServer.rooms?.size || 0,
          roomStats: roomStats,
          users: chatServer.userSeat?.size || 0,
          totalMessages: chatServer._stats?.messages || 0,
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      // ============================================================
      // ✅ DEFAULT
      // ============================================================
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 
          'Content-Type': 'application/json'
        }
      });
    }
  }
};

// ✅ EXPORT UNTUK COMPATIBILITY
export { ChatServer, GameServer };