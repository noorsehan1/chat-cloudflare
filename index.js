// ==================== INDEX.JS - FIXED CHAT CONNECTION ====================
// VERSION: 3.2.1 - FIXED CHAT CONNECTION

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ==================== CONFIGURATION ====================
const CONFIG = {
  CACHE_TTL_MS: 60000,
  INSTANCE_COUNT: 3,
  MAX_RETRIES: 3,
  RETRY_DELAY_MS: 100,
  REQUEST_TIMEOUT_MS: 3000,
  HEALTH_TIMEOUT_MS: 2000,
  CIRCUIT_BREAKER_THRESHOLD: 5,
  CIRCUIT_BREAKER_TIMEOUT_MS: 30000,
};

// ==================== CACHE MANAGER ====================
class InstanceCache {
  constructor() {
    this.cache = new Map();
    this.circuitBreaker = new Map();
    this.ttl = CONFIG.CACHE_TTL_MS;
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return null;
    }
    if (this.isCircuitOpen(key)) {
      return null;
    }
    return entry.instance;
  }

  set(key, instance) {
    this.cache.set(key, {
      instance: instance,
      timestamp: Date.now()
    });
  }

  delete(key) {
    this.cache.delete(key);
  }

  isCircuitOpen(key) {
    const breaker = this.circuitBreaker.get(key);
    if (!breaker) return false;
    if (Date.now() - breaker.timestamp > CONFIG.CIRCUIT_BREAKER_TIMEOUT_MS) {
      this.circuitBreaker.delete(key);
      return false;
    }
    return breaker.failures >= CONFIG.CIRCUIT_BREAKER_THRESHOLD;
  }

  recordFailure(key) {
    const breaker = this.circuitBreaker.get(key) || {
      failures: 0,
      timestamp: Date.now()
    };
    breaker.failures++;
    breaker.timestamp = Date.now();
    this.circuitBreaker.set(key, breaker);
  }

  recordSuccess(key) {
    this.circuitBreaker.delete(key);
  }

  cleanup() {
    const now = Date.now();
    for (const [key, entry] of this.cache) {
      if (now - entry.timestamp > this.ttl * 2) {
        this.cache.delete(key);
      }
    }
    for (const [key, breaker] of this.circuitBreaker) {
      if (now - breaker.timestamp > CONFIG.CIRCUIT_BREAKER_TIMEOUT_MS * 2) {
        this.circuitBreaker.delete(key);
      }
    }
  }

  clear() {
    this.cache.clear();
    this.circuitBreaker.clear();
  }
}

// ==================== INSTANCE POOL ====================
class InstancePool {
  constructor(env, className) {
    this.env = env;
    this.className = className;
    this.cache = new InstanceCache();
    this.pendingRequests = new Map();
  }

  getInstance(room, attempt = 0) {
    try {
      const hash = this._hashString(room + attempt);
      const instanceId = Math.abs(hash) % CONFIG.INSTANCE_COUNT;
      const cacheKey = `instance_${instanceId}`;
      
      let cached = this.cache.get(cacheKey);
      if (cached) {
        return cached;
      }
      
      const id = this.env[this.className].idFromName(`game_${instanceId}`);
      const obj = this.env[this.className].get(id);
      this.cache.set(cacheKey, obj);
      return obj;
      
    } catch(e) {
      throw new Error(`Failed to get instance: ${e.message}`);
    }
  }

  async fetch(request, room) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), CONFIG.REQUEST_TIMEOUT_MS);
    
    let lastError = null;
    let triedInstances = new Set();
    
    for (let attempt = 0; attempt < CONFIG.MAX_RETRIES; attempt++) {
      try {
        let obj = null;
        let instanceKey = null;
        
        for (let innerAttempt = 0; innerAttempt < 3; innerAttempt++) {
          try {
            const hash = this._hashString(room + attempt + innerAttempt);
            const instanceId = Math.abs(hash) % CONFIG.INSTANCE_COUNT;
            const cacheKey = `instance_${instanceId}`;
            
            if (triedInstances.has(cacheKey)) continue;
            triedInstances.add(cacheKey);
            
            if (this.cache.isCircuitOpen(cacheKey)) continue;
            
            obj = this.cache.get(cacheKey);
            if (!obj) {
              const id = this.env[this.className].idFromName(`game_${instanceId}`);
              obj = this.env[this.className].get(id);
              this.cache.set(cacheKey, obj);
            }
            
            instanceKey = cacheKey;
            break;
            
          } catch(e) {
            continue;
          }
        }
        
        if (!obj || !instanceKey) {
          throw new Error('No available instances');
        }
        
        const response = await obj.fetch(request, {
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        this.cache.recordSuccess(instanceKey);
        
        if (response.status === 200 || response.status === 101) {
          return response;
        }
        
        if (response.status === 503 || response.status === 429) {
          this.cache.recordFailure(instanceKey);
          throw new Error('Instance busy or overloaded');
        }
        
        return response;
        
      } catch(error) {
        clearTimeout(timeoutId);
        lastError = error;
        
        if (error.name === 'AbortError') {
          throw new Error('Request timeout');
        }
        
        if (error.message === 'Instance busy or overloaded') {
          await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY_MS));
          continue;
        }
        
        throw error;
      }
    }
    
    clearTimeout(timeoutId);
    
    return new Response(JSON.stringify({
      error: "All game servers busy, please retry",
      retryAfter: 5,
      detail: lastError?.message || "Unknown error"
    }), {
      status: 503,
      headers: {
        'Retry-After': '5',
        'Content-Type': 'application/json'
      }
    });
  }

  async healthCheck() {
    const results = [];
    let totalConnections = 0;
    let totalGames = 0;
    let healthyCount = 0;
    
    for (let i = 0; i < CONFIG.INSTANCE_COUNT; i++) {
      try {
        const cacheKey = `instance_${i}`;
        let obj = this.cache.get(cacheKey);
        
        if (!obj) {
          const id = this.env[this.className].idFromName(`game_${i}`);
          obj = this.env[this.className].get(id);
          this.cache.set(cacheKey, obj);
        }
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), CONFIG.HEALTH_TIMEOUT_MS);
        
        const resp = await obj.fetch(new Request("https://dummy/health"), {
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (resp.ok) {
          const data = await resp.json();
          results.push({
            id: i,
            status: "healthy",
            connections: data.connections || 0,
            games: data.games || 0,
            queue: data.queue || 0,
            initialized: data.initialized || false,
            uptime: data.uptime || 0
          });
          totalConnections += data.connections || 0;
          totalGames += data.games || 0;
          healthyCount++;
          this.cache.recordSuccess(cacheKey);
        } else {
          results.push({
            id: i,
            status: "unhealthy",
            code: resp.status
          });
          this.cache.recordFailure(cacheKey);
        }
        
      } catch(e) {
        results.push({
          id: i,
          status: "error",
          error: e.message || "Timeout"
        });
        this.cache.recordFailure(`instance_${i}`);
      }
    }
    
    return {
      status: healthyCount === CONFIG.INSTANCE_COUNT ? "ok" : "degraded",
      timestamp: Date.now(),
      instances: results,
      totalConnections,
      totalGames,
      healthyCount,
      totalInstances: CONFIG.INSTANCE_COUNT,
      circuitBreakers: Array.from(this.cache.circuitBreaker.keys())
    };
  }

  _hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return Math.abs(hash);
  }

  cleanup() {
    this.cache.cleanup();
  }

  clear() {
    this.cache.clear();
  }
}

// ==================== SINGLETON CHAT HANDLER ====================
class ChatHandler {
  constructor(env) {
    this.env = env;
    this._instance = null;
    this._cacheKey = 'chat_global';
    this.cache = new InstanceCache();
  }

  getInstance() {
    // Cek cache
    let cached = this.cache.get(this._cacheKey);
    if (cached) {
      return cached;
    }
    
    // Buat instance baru
    const id = this.env.CHAT_SERVER.idFromName("global");
    const obj = this.env.CHAT_SERVER.get(id);
    this.cache.set(this._cacheKey, obj);
    return obj;
  }

  async fetch(request) {
    try {
      const obj = this.getInstance();
      const response = await obj.fetch(request);
      
      // Record success
      this.cache.recordSuccess(this._cacheKey);
      
      // ✅ Tambahkan CORS untuk chat
      const corsResponse = new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers
      });
      
      corsResponse.headers.set('Access-Control-Allow-Origin', '*');
      corsResponse.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      corsResponse.headers.set('Access-Control-Allow-Headers', 'Content-Type, Upgrade');
      
      return corsResponse;
      
    } catch(e) {
      this.cache.recordFailure(this._cacheKey);
      return new Response(JSON.stringify({
        error: "Chat server unavailable",
        message: e.message
      }), {
        status: 503,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  cleanup() {
    this.cache.cleanup();
  }
}

// ==================== MAIN EXPORT ====================
const instanceCache = new InstanceCache();
let chatHandler = null;
let gamePool = null;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== PREFLIGHT (CORS) ==========
      if (request.method === 'OPTIONS') {
        return new Response(null, {
          status: 204,
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Upgrade, Sec-WebSocket-Key, Sec-WebSocket-Version, Sec-WebSocket-Extensions',
            'Access-Control-Max-Age': '86400'
          }
        });
      }
      
      // ========== CHAT SERVER - SEMUA PATH CHAT ==========
      // ✅ PERBAIKAN: Tangani semua path yang berhubungan dengan chat
      if (pathname === "/ws" || 
          pathname === "/chat" || 
          pathname === "/chat/ws" ||
          pathname === "/socket" ||
          pathname === "/websocket" ||
          pathname === "/" ||
          pathname === "/chat-socket" ||
          pathname.startsWith("/chat/")) {
        
        // Initialize chat handler
        if (!chatHandler) {
          chatHandler = new ChatHandler(env);
        }
        
        // ✅ Untuk root path, cek apakah request WebSocket
        if (pathname === "/") {
          const upgrade = request.headers.get("Upgrade");
          if (upgrade === "websocket") {
            // Ini request WebSocket ke chat
            return chatHandler.fetch(request);
          }
          // Bukan WebSocket, return info
          return new Response(JSON.stringify({
            status: "Chat Server Running",
            version: "3.3.10",
            endpoints: {
              websocket: "/ws",
              chat: "/chat"
            }
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
          });
        }
        
        // ✅ Semua request chat (termasuk WebSocket)
        return chatHandler.fetch(request);
      }
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws") {
        if (!gamePool) {
          gamePool = new InstancePool(env, 'GAME_SERVER');
        }
        
        const room = url.searchParams.get("room") || "default";
        
        const newRequest = new Request(request.url, {
          method: request.method,
          headers: request.headers,
          body: request.body,
          duplex: 'half'
        });
        
        newRequest.headers.set('X-Room', room);
        
        const response = await gamePool.fetch(newRequest, room);
        
        const corsResponse = new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers
        });
        
        corsResponse.headers.set('Access-Control-Allow-Origin', '*');
        corsResponse.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        corsResponse.headers.set('Access-Control-Allow-Headers', 'Content-Type, Upgrade');
        
        return corsResponse;
      }
      
      // ========== GAME HEALTH ==========
      if (pathname === "/game/health") {
        if (!gamePool) {
          gamePool = new InstancePool(env, 'GAME_SERVER');
        }
        const health = await gamePool.healthCheck();
        return new Response(JSON.stringify(health), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== GAME INFO ==========
      if (pathname === "/game") {
        const health = gamePool ? await gamePool.healthCheck() : { status: "not_initialized" };
        return new Response(JSON.stringify({
          status: "running",
          version: "3.2.0",
          instances: CONFIG.INSTANCE_COUNT,
          maxConnections: 150,
          timestamp: Date.now(),
          health: health,
          endpoints: {
            websocket: "/game/ws?room={room_name}",
            health: "/game/health"
          }
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== CACHE CLEANUP ==========
      if (pathname === "/cache/cleanup") {
        if (gamePool) gamePool.cleanup();
        if (chatHandler) chatHandler.cleanup();
        instanceCache.cleanup();
        return new Response(JSON.stringify({
          status: "ok",
          message: "Cache cleaned"
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== CACHE CLEAR ==========
      if (pathname === "/cache/clear") {
        if (gamePool) gamePool.clear();
        if (chatHandler) chatHandler.clear();
        instanceCache.clear();
        return new Response(JSON.stringify({
          status: "ok",
          message: "Cache cleared"
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== HEALTH CHECK GLOBAL ==========
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          version: "3.2.1",
          timestamp: Date.now(),
          services: {
            chat: chatHandler ? "ready" : "not_initialized",
            game: gamePool ? "ready" : "not_initialized"
          }
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== DEFAULT ==========
      return new Response(JSON.stringify({
        status: "running",
        version: "3.2.1",
        timestamp: Date.now(),
        endpoints: {
          chat: {
            websocket: "/ws",
            chat: "/chat",
            root: "/"
          },
          game: {
            websocket: "/game/ws?room={room_name}",
            health: "/game/health",
            info: "/game"
          },
          system: {
            health: "/health",
            cacheCleanup: "/cache/cleanup",
            cacheClear: "/cache/clear"
          }
        }
      }), {
        status: 200,
        headers: { 
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache'
        }
      });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error",
        timestamp: Date.now()
      }), {
        status: 500,
        headers: {
          'Retry-After': '30',
          'Content-Type': 'application/json'
        }
      });
    }
  },
  
  // ========== SCHEDULED CLEANUP ==========
  async scheduled(event, env, ctx) {
    if (gamePool) gamePool.cleanup();
    if (chatHandler) chatHandler.cleanup();
    instanceCache.cleanup();
    console.log(`Cache cleaned at ${new Date().toISOString()}`);
  }
};

// ==================== EXPORTS ====================
export { ChatServer, GameServer };
