// ==================== INDEX.JS - PURE WORKER FULL ====================
// VERSION: 4.0.0

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ========== GLOBAL STATE (Shared across all requests) ==========
const globalState = {
  chatServer: null,
  gameServer: null,
  initialized: false,
  initPromise: null,
  lastSave: 0,
};

// ========== CACHE NAME ==========
const CACHE_NAME = 'app_state_cache';
const CHAT_STATE_KEY = 'chat_server_state';
const GAME_STATE_KEY = 'game_server_state';

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== INIT SERVERS (ONCE) ==========
      if (!globalState.initialized) {
        if (!globalState.initPromise) {
          globalState.initPromise = (async () => {
            try {
              const cache = await caches.open(CACHE_NAME);
              
              // Load Chat State
              let chatState = null;
              try {
                const chatResp = await cache.match(CHAT_STATE_KEY);
                if (chatResp) {
                  chatState = await chatResp.json();
                }
              } catch(e) {}
              
              // Load Game State
              let gameState = null;
              try {
                const gameResp = await cache.match(GAME_STATE_KEY);
                if (gameResp) {
                  gameState = await gameResp.json();
                }
              } catch(e) {}
              
              // ========== INIT CHAT SERVER ==========
              globalState.chatServer = new ChatServer({
                storage: {
                  get: async (key) => {
                    try {
                      const cache = await caches.open(CACHE_NAME);
                      const resp = await cache.match(key);
                      if (resp) {
                        const data = await resp.json();
                        return data.value;
                      }
                      return null;
                    } catch(e) { return null; }
                  },
                  put: async (key, value) => {
                    try {
                      const cache = await caches.open(CACHE_NAME);
                      const response = new Response(JSON.stringify({ value }), {
                        headers: { 'Content-Type': 'application/json' }
                      });
                      await cache.put(key, response);
                    } catch(e) {}
                  },
                  delete: async (key) => {
                    try {
                      const cache = await caches.open(CACHE_NAME);
                      await cache.delete(key);
                    } catch(e) {}
                  },
                  setAlarm: async (ms) => {}
                },
                env: env,
                ctx: {
                  acceptWebSocket: (ws) => {
                    try { ws.accept(); } catch(e) {}
                  }
                }
              }, chatState);
              
              // ========== INIT GAME SERVER ==========
              globalState.gameServer = new GameServer(env, gameState);
              
              globalState.initialized = true;
              globalState.lastSave = Date.now();
              
              // Auto-save every 30 seconds
              setInterval(async () => {
                try {
                  await globalState.chatServer.saveState();
                  await globalState.gameServer.saveState();
                  globalState.lastSave = Date.now();
                } catch(e) {}
              }, 30000);
              
              // Auto-save on close
              if (typeof addEventListener !== 'undefined') {
                addEventListener('beforeunload', async () => {
                  try {
                    await globalState.chatServer.saveState();
                    await globalState.gameServer.saveState();
                  } catch(e) {}
                });
              }
              
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
          version: "4.0.0-pure",
          mode: "pure-worker",
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          timestamp: Date.now(),
          endpoints: {
            chat: "/ws",
            game: "/game/ws?room={room_name}",
            health: "/game/health"
          }
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== STATUS ==========
      if (pathname === "/status") {
        return new Response(JSON.stringify({
          status: "ok",
          initialized: globalState.initialized,
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          lastSave: new Date(globalState.lastSave).toISOString(),
          uptime: Date.now() - (globalState.chatServer?._startTime || Date.now()),
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
