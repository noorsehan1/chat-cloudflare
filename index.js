// ==================== INDEX-FIXED.js ====================

import { ChatServer } from "./chat-server-fixed.js";
import { GameServer } from "./game-server-fixed.js";

// Simpan instance server di global
let chatServerInstance = null;
let gameServerInstance = null;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ==================== CHAT SERVER ====================
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        if (!chatServerInstance) {
          chatServerInstance = new ChatServer(env);
        }
        return chatServerInstance.fetch(request);
      }
      
      // ==================== GAME SERVER ====================
      if (pathname === "/game/ws" || pathname === "/game") {
        if (!gameServerInstance) {
          gameServerInstance = new GameServer(env);
        }
        return gameServerInstance.fetch(request);
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      console.error("Error:", e);
      return new Response("Error: " + e.message, { status: 500 });
    }
  },
  
  // ==================== CLEANUP PADA SHUTDOWN ====================
  async shutdown() {
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

// Ekspor kelas untuk kompatibilitas
export { ChatServer, GameServer };
