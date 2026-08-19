// ==================== INDEX.JS ====================
// VERSION: 8.0.0 - PURE WORKER (NO DO)
// AUTO CLEAN MEMORY ON DEPLOY

 import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ============================================================
// ✅ VERSION TIMESTAMP - AKAN BERUBAH SETIAP DEPLOY
// ============================================================
const DEPLOY_VERSION = Date.now(); // ← AKAN BERUBAH SETIAP DEPLOY!

// ============================================================
// ✅ CHAT SERVER INSTANCE (BARU SETIAP DEPLOY)
// ============================================================
const chatServer = new ChatServer();

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ============================================================
      // ✅ CHAT SERVER
      // ============================================================
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        chatServer.start();
        return chatServer.fetch(request);
      }
      
      // ============================================================
      // ✅ GAME SERVER (BARU SETIAP DEPLOY)
      // ============================================================
      if (pathname === "/game/ws") {
        // ✅ KIRIM DEPLOY_VERSION KE GAME SERVER
        return GameServer.fetch(request, env, DEPLOY_VERSION);
      }
      
      if (pathname === "/game/health") {
        return GameServer.fetch(request, env, DEPLOY_VERSION);
      }
      
      // ============================================================
      // ✅ HEALTH CHECK
      // ============================================================
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          deployVersion: DEPLOY_VERSION,
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
      // ✅ VERSION INFO
      // ============================================================
      if (pathname === "/version") {
        return new Response(JSON.stringify({
          deployVersion: DEPLOY_VERSION,
          timestamp: new Date(DEPLOY_VERSION).toISOString(),
          type: "pure-worker"
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
          deployVersion: DEPLOY_VERSION,
          type: "pure-worker",
          maxConnections: 300,
          timestamp: Date.now(),
          endpoints: {
            chat: "/ws",
            game: "/game/ws?room={room_name}",
            health: "/health",
            stats: "/stats",
            version: "/version"
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
          deployVersion: DEPLOY_VERSION,
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
      // ✅ RESET (ADMIN) - FORCE CLEAN
      // ============================================================
      if (pathname === "/admin/reset") {
        // Reset chat server
        chatServer.destroy();
        // Re-init
        const newChatServer = new ChatServer();
        // Replace instance
        Object.assign(chatServer, newChatServer);
        
        return new Response(JSON.stringify({
          status: "reset",
          deployVersion: DEPLOY_VERSION,
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

export { ChatServer, GameServer };
