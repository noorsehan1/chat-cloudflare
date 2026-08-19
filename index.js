// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Inisialisasi singleton servers
let chatServerInstance = null;
let gameServerInstance = null;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // Inisialisasi server jika belum ada
      if (!chatServerInstance) {
        chatServerInstance = new ChatServer(env);
      }
      
      if (!gameServerInstance) {
        gameServerInstance = new GameServer(env);
      }
      
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return chatServerInstance.fetch(request);
      }
      
      if (pathname === "/game/ws" || pathname === "/game") {
        return gameServerInstance.fetch(request);
      }
      
      // Health check endpoint
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          chatConnections: chatServerInstance?.wsSet?.size || 0,
          gameConnections: gameServerInstance?.wsMap?.size || 0,
          activeGames: gameServerInstance?.activeGames?.size || 0,
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};

export { ChatServer, GameServer };
