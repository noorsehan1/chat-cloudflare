// ==================== INDEX.JS - WITH GLOBAL STATE ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ========== GLOBAL STATE - SHARED ACROSS ALL REQUESTS ==========
// Ini akan di-share di semua instance worker karena menggunakan Cache API
const globalState = {
  chatServer: null,
  gameServer: null,
  initialized: false,
  initPromise: null,
  _startTime: Date.now(),
};

// Cache untuk state persistence antar request
const STATE_CACHE = {
  chatRooms: new Map(),
  gameRooms: new Map(),
  wsConnections: new Map(),
  lastSync: Date.now()
};

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== SINGLETON INIT ==========
      if (!globalState.initialized) {
        if (!globalState.initPromise) {
          globalState.initPromise = (async () => {
            try {
              // Coba load state dari Cache API
              const cache = await caches.open('game_state');
              const cachedResponse = await cache.match('/state');
              let savedState = null;
              
              if (cachedResponse) {
                try {
                  savedState = await cachedResponse.json();
                } catch(e) {}
              }
              
              // INIT CHAT SERVER dengan shared state
              globalState.chatServer = new ChatServer({
                env: env,
                sharedState: STATE_CACHE,
                isGlobal: true
              });
              
              // INIT GAME SERVER dengan shared state
              globalState.gameServer = new GameServer({
                env: env,
                sharedState: STATE_CACHE,
                isGlobal: true
              });
              
              // Restore state jika ada
              if (savedState) {
                if (savedState.chatRooms) {
                  for (const [key, value] of Object.entries(savedState.chatRooms)) {
                    STATE_CACHE.chatRooms.set(key, value);
                  }
                }
                if (savedState.gameRooms) {
                  for (const [key, value] of Object.entries(savedState.gameRooms)) {
                    STATE_CACHE.gameRooms.set(key, value);
                  }
                }
              }
              
              globalState.initialized = true;
              
              // SAVE STATE setiap 10 detik ke Cache API
              setInterval(async () => {
                try {
                  const stateToSave = {
                    chatRooms: Object.fromEntries(STATE_CACHE.chatRooms),
                    gameRooms: Object.fromEntries(STATE_CACHE.gameRooms),
                    timestamp: Date.now()
                  };
                  
                  const cache = await caches.open('game_state');
                  const response = new Response(JSON.stringify(stateToSave), {
                    headers: { 'Content-Type': 'application/json' }
                  });
                  await cache.put('/state', response);
                } catch(e) {}
              }, 10000);
              
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
      // Chat Server
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return globalState.chatServer.fetch(request);
      }
      
      // Game Server
      if (pathname === "/game/ws" || pathname === "/game") {
        return globalState.gameServer.fetch(request);
      }
      
      // Health Check
      if (pathname === "/health") {
        const chatConnections = globalState.chatServer?.wsSet?.size || 0;
        const gameConnections = globalState.gameServer?.wsMap?.size || 0;
        
        return new Response(JSON.stringify({
          status: "running",
          version: "5.0.0-global",
          mode: "global-shared-state",
          chatConnections,
          gameConnections,
          totalConnections: chatConnections + gameConnections,
          chatRooms: STATE_CACHE.chatRooms.size,
          gameRooms: STATE_CACHE.gameRooms.size,
          uptime: Date.now() - globalState._startTime,
          timestamp: Date.now()
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // Debug - lihat semua user di room
      if (pathname === "/debug/rooms") {
        const chatRooms = {};
        for (const [room, data] of STATE_CACHE.chatRooms) {
          chatRooms[room] = {
            users: data.users ? Array.from(data.users) : [],
            count: data.users?.size || 0
          };
        }
        
        return new Response(JSON.stringify({
          chatRooms,
          gameRooms: Array.from(STATE_CACHE.gameRooms.keys()),
          timestamp: Date.now()
        }), {
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
