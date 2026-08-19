// ==================== INDEX.JS - PURE WORKER ====================
import { GameServer } from "./game-server.js";
import { getChatServer } from "./chat-server.js";

// ==================== SINGLETON GAME SERVER ====================
let gameServerInstance = null;

function getGameServer(env) {
  if (!gameServerInstance) {
    // GameServer membutuhkan state dan env, tapi kita bisa mock state
    // atau buat GameServer yang tidak membutuhkan Durable Object state
    gameServerInstance = new GameServer(null, env);
  }
  return gameServerInstance;
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // CHAT SERVER - pure worker, semua user satu alam
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const chatServer = getChatServer(env);
        return chatServer.fetch(request);
      }
      
      // GAME SERVER - pure worker
      if (pathname === "/game/ws" || pathname === "/game") {
        const gameServer = getGameServer(env);
        return gameServer.fetch(request);
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  },
  
  // Untuk cleanup saat worker di-unload
  async scheduled(event, env, ctx) {
    // Tidak ada scheduled cleanup yang diperlukan
  }
};

export { GameServer };
