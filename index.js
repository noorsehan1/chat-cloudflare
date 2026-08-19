// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ✅ INSTANCE SINGLETON
const chatServer = new ChatServer();

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ✅ CHAT SERVER - PAKAI INSTANCE
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return chatServer.fetch(request);
      }
      
      // ✅ GAME SERVER
      if (pathname === "/game/ws") {
        return GameServer.fetch(request, env);
      }
      
      if (pathname === "/game/health") {
        return GameServer.fetch(request, env);
      }
      
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: chatServer.wsSet?.size || 0,
          rooms: chatServer.rooms?.size || 0,
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};

export { ChatServer, GameServer };