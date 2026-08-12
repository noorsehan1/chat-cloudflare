// ==================== INDEX.JS - TANPA DURABLE OBJECTS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ✅ INSTANCE GLOBAL (Reuse untuk semua request)
let chatServer = null;
let gameServer = null;
let isInitializing = false;

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ==================== CHAT ROUTES ====================
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        // ✅ BUAT ATAU REUSE CHAT SERVER
        if (!chatServer || chatServer.isDestroyed) {
          chatServer = new ChatServer(env); // ✅ TANPA state
        }
        return chatServer.fetch(request);
      }
      
      // ==================== GAME ROUTES ====================
      if (pathname === "/game/ws" || pathname === "/game") {
        // ✅ BUAT ATAU REUSE GAME SERVER
        if (!gameServer || gameServer.isDestroyed) {
          gameServer = new GameServer(env); // ✅ TANPA state
          // Init async (jika ada method init)
          if (gameServer._initAsync) {
            await gameServer._initAsync();
          }
        }
        return gameServer.fetch(request);
      }
      
      // ==================== HEALTH CHECK ====================
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          chat: chatServer ? "active" : "inactive",
          game: gameServer ? "active" : "inactive",
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      // ==================== ROOT ====================
      return new Response("Server running - Use /ws for chat or /game/ws for game", { 
        status: 200,
        headers: { "Content-Type": "text/plain" }
      });
      
    } catch(error) {
      console.error('[Worker] Error:', error);
      return new Response(JSON.stringify({ 
        error: "Internal server error",
        message: error.message 
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      });
    }
  },
  
  // ✅ CLEANUP SAAT WORKER DIHENTIKAN
  async cleanup() {
    console.log('[Worker] Cleaning up...');
    
    if (chatServer && !chatServer.isDestroyed) {
      try {
        await chatServer.destroy();
      } catch(e) {
        console.error('[Worker] ChatServer destroy error:', e);
      }
    }
    
    if (gameServer && !gameServer.isDestroyed) {
      try {
        await gameServer.destroy();
      } catch(e) {
        console.error('[Worker] GameServer destroy error:', e);
      }
    }
    
    chatServer = null;
    gameServer = null;
  }
};

// ✅ EKSPOR CLASS UNTUK MIGRASI (jika diperlukan)
export { ChatServer, GameServer };
