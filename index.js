// ==================== INDEX.JS - PAKAI D1 ====================
// VERSION: 5.0.0

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

const globalState = {
  chatServer: null,
  gameServer: null,
  initialized: false,
  initPromise: null,
};

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      if (!globalState.initialized) {
        if (!globalState.initPromise) {
          globalState.initPromise = (async () => {
            try {
              // CHAT SERVER
              globalState.chatServer = new ChatServer(env);
              
              // GAME SERVER
              globalState.gameServer = new GameServer(env);
              
              globalState.initialized = true;
              
              // Load state dari D1
              await globalState.chatServer.loadState();
              await globalState.gameServer.loadState();
              
              // Auto-save setiap 5 detik
              setInterval(async () => {
                try {
                  await globalState.chatServer.saveState();
                  await globalState.gameServer.saveState();
                } catch(e) {}
              }, 5000);
              
              return true;
            } catch(e) {
              console.error('Init error:', e);
              globalState.initialized = false;
              globalState.initPromise = null;
              throw e;
            }
          })();
        }
        await globalState.initPromise;
      }
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return globalState.chatServer.fetch(request);
      }
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws") {
        return globalState.gameServer.handleWebSocket(request);
      }
      
      if (pathname === "/game/health") {
        return globalState.gameServer.handleHealth();
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "5.0.0-d1",
          mode: "d1",
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          timestamp: Date.now(),
          db: "chat-db"
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (pathname === "/status") {
        return new Response(JSON.stringify({
          status: "ok",
          initialized: globalState.initialized,
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
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
  }
};

export { ChatServer, GameServer };
