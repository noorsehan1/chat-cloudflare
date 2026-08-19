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
      
      // SEMUA WEBSOCKET KONEKSI LEWAT ROOT PATH "/"
      // Tidak peduli path apapun, semua WebSocket ditangani oleh ChatServer
      const upgrade = request.headers.get("Upgrade");
      if (upgrade === "websocket") {
        // Cek apakah ini koneksi game (dari user agent atau header khusus)
        const userAgent = request.headers.get("User-Agent") || "";
        const isGameConnection = request.headers.get("X-Connection-Type") === "game" || 
                                 pathname === "/game" ||
                                 userAgent.includes("Game");
        
        if (isGameConnection) {
          return gameServerInstance.fetch(request);
        }
        
        // Default: ChatServer untuk semua koneksi WebSocket
        return chatServerInstance.fetch(request);
      }
      
      // Health check
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
      
      return new Response("Server running - WebSocket endpoint: wss://your-worker.workers.dev/", { 
        status: 200,
        headers: {
          "Content-Type": "text/plain"
        }
      });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};

export { ChatServer, GameServer };
