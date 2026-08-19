// ==================== INDEX.JS - FIX ====================
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
      
      console.log(`[Index] Request: ${pathname}`);
      
      // ========== INIT ==========
      if (!globalState.initialized) {
        if (!globalState.initPromise) {
          globalState.initPromise = (async () => {
            try {
              console.log('[Index] Initializing servers...');
              console.log('[Index] DB available:', !!env.DB);
              
              globalState.chatServer = new ChatServer(env);
              globalState.gameServer = new GameServer(env);
              
              globalState.initialized = true;
              console.log('[Index] Servers initialized');
              
              return true;
            } catch(e) {
              console.error('[Index] Init error:', e);
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
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (pathname === "/status") {
        let dbStatus = "unknown";
        try {
          if (env.DB) {
            const result = await env.DB.prepare("SELECT 1").first();
            dbStatus = result ? "connected" : "no result";
          } else {
            dbStatus = "no DB binding";
          }
        } catch(e) {
          dbStatus = "error: " + e.message;
        }
        
        let totalSeats = 0;
        const roomSeats = {};
        if (globalState.chatServer) {
          for (const [room, data] of globalState.chatServer.rooms) {
            roomSeats[room] = data.seats.size;
            totalSeats += data.seats.size;
          }
        }
        
        return new Response(JSON.stringify({
          status: "ok",
          initialized: globalState.initialized,
          dbStatus: dbStatus,
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          totalSeats: totalSeats,
          roomSeats: roomSeats,
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
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};

export { ChatServer, GameServer };
