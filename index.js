// ==================== INDEX.JS - TANPA D1 ====================
// VERSION: 6.0.0 - PURE WORKER WITH CACHE

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ========== GLOBAL STATE ==========
const globalState = {
  chatServer: null,
  gameServer: null,
  initialized: false,
  initPromise: null,
  instanceId: null,
};

// ========== CACHE KEY ==========
const CACHE_KEY = 'chat_global_state';

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== INIT ==========
      if (!globalState.initialized) {
        if (!globalState.initPromise) {
          globalState.initPromise = (async () => {
            try {
              console.log('[Index] Initializing servers...');
              
              // Generate unique instance ID
              globalState.instanceId = `instance_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
              console.log(`[Index] Instance ID: ${globalState.instanceId}`);
              
              // ========== LOAD STATE FROM CACHE ==========
              let savedState = null;
              try {
                const cache = await caches.open('app_cache');
                const cached = await cache.match(CACHE_KEY);
                if (cached) {
                  savedState = await cached.json();
                  console.log('[Index] Loaded state from cache');
                }
              } catch(e) {
                console.log('[Index] No cached state found');
              }
              
              // ========== CREATE CHAT SERVER ==========
              globalState.chatServer = new ChatServer(env, savedState?.chat || null);
              
              // ========== CREATE GAME SERVER ==========
              globalState.gameServer = new GameServer(env, savedState?.game || null);
              
              globalState.initialized = true;
              
              // ========== AUTO SAVE EVERY 2 SECONDS ==========
              setInterval(async () => {
                try {
                  const state = {
                    chat: globalState.chatServer.getState(),
                    game: globalState.gameServer.getState(),
                    timestamp: Date.now(),
                    instanceId: globalState.instanceId
                  };
                  
                  const cache = await caches.open('app_cache');
                  const response = new Response(JSON.stringify(state), {
                    headers: { 'Content-Type': 'application/json' }
                  });
                  await cache.put(CACHE_KEY, response);
                } catch(e) {}
              }, 2000);
              
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
      
      // ========== ROUTES ==========
      // Chat
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return globalState.chatServer.fetch(request);
      }
      
      // Game
      if (pathname === "/game/ws") {
        return globalState.gameServer.handleWebSocket(request);
      }
      
      if (pathname === "/game/health") {
        return globalState.gameServer.handleHealth();
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "6.0.0-pure",
          mode: "cache-shared",
          instanceId: globalState.instanceId,
          chatConnections: globalState.chatServer?.wsSet?.size || 0,
          gameConnections: globalState.gameServer?.wsMap?.size || 0,
          games: globalState.gameServer?.activeGames?.size || 0,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (pathname === "/status") {
        return new Response(JSON.stringify({
          status: "ok",
          instanceId: globalState.instanceId,
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
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};

export { ChatServer, GameServer };
