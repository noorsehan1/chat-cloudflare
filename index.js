// ==================== INDEX.JS (FIXED) ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      // === CHAT SERVER ===
      // Tambahkan kondisi untuk root WebSocket
      if (
        pathname === "/ws" || 
        pathname === "/chat" || 
        pathname === "/reset" ||
        pathname === "/health" ||
        (pathname === "/" && upgrade === "websocket")  // ← INI TAMBAHAN
      ) {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // === GAME SERVER ===
      if (pathname === "/game/ws" || pathname === "/game/health" || pathname === "/game" || pathname === "/game/") {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // === ROOT (HTTP) ===
      if (pathname === "/") {
        return new Response("Chat Server Running\nWebSocket: wss://" + url.host + "/ws\n", { 
          status: 200,
          headers: { 'Content-Type': 'text/plain' }
        });
      }
      
      return new Response("Not Found", { status: 404 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

export { ChatServer, GameServer };
