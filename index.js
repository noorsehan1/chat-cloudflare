// ==================== INDEX.JS - PURE WORKER WITH MEMORY STATE ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ========== GLOBAL STATE (Shared across all requests) ==========
const globalState = {
  chatServer: null,
  gameServer: null,
  initialized: false,
  initPromise: null,
  stateCache: new Map(),
  lastSave: null,
};

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== INIT SERVER (ONCE) ==========
      if (!globalState.initialized) {
        if (!globalState.initPromise) {
          globalState.initPromise = (async () => {
            try {
              // Init Chat Server - Pure Memory State
              globalState.chatServer = new ChatServer({
                storage: {
                  get: async (key) => globalState.stateCache.get(key) || null,
                  put: async (key, value) => {
                    globalState.stateCache.set(key, value);
                  },
                  delete: async (key) => {
                    globalState.stateCache.delete(key);
                  },
                  setAlarm: async (ms) => {
                    // No-op untuk pure worker - pakai setTimeout
                    if (ms) {
                      setTimeout(async () => {
                        if (globalState.chatServer && !globalState.chatServer.closing) {
                          await globalState.chatServer.alarm?.();
                        }
                      }, Math.min(ms - Date.now(), 900000));
                    }
                  }
                },
                env: env,
                ctx: {
                  acceptWebSocket: (ws) => {
                    try { ws.accept(); } catch(e) {}
                  }
                }
              });
              
              // Init Game Server - Pure Memory State
              globalState.gameServer = new GameServer({
                env: env,
                state: {
                  storage: {
                    get: async (key) => globalState.stateCache.get(key) || null,
                    put: async (key, value) => {
                      globalState.stateCache.set(key, value);
                    },
                    delete: async (key) => {
                      globalState.stateCache.delete(key);
                    }
                  }
                }
              });
              
              globalState.initialized = true;
              
              // Health check setiap 30 detik
              setInterval(async () => {
                try {
                  if (globalState.chatServer) {
                    globalState.chatServer._cleanupDeadConnections?.();
                    globalState.chatServer._cleanupMemory?.();
                  }
                  if (globalState.gameServer) {
                    globalState.gameServer._cleanupDeadConnections?.();
                    globalState.gameServer._cleanupMemory?.();
                  }
                } catch(e) {}
              }, 30000);
              
              return true;
            } catch(e) {
              globalState.initialized = false;
              globalState.initPromise = null;
              throw e;
            }
          })();
        }
        await globalState.initPromise;
      }
      
      // ========== ROUTING ==========
      // Chat Server endpoints
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return globalState.chatServer.fetch(request);
      }
      
      // Game Server endpoints
      if (pathname === "/game/ws" || pathname === "/game") {
        return globalState.gameServer.fetch(request);
      }
      
      // Health check semua server
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "running",
          version: "5.0.0-pure",
          mode: "pure-worker-memory",
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          chatRooms: globalState.chatServer?.rooms?.size || 0,
          memoryCache: globalState.stateCache.size,
          uptime: Date.now() - (globalState._startTime || Date.now()),
          timestamp: Date.now()
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
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
