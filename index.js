// ==================== INDEX.JS - FULLY FIXED ====================
// VERSION: 3.2.0 - ALARM SYSTEM WITH CONNECTION POOLING
// COMPATIBLE WITH ChatServer 3.3.10 & GameServer 3.2.0

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// ==================== CONFIGURATION ====================
const CONFIG = {
  // Cache
  CACHE_TTL_MS: 60000,
  INSTANCE_COUNT: 3,
  
  // Retry
  MAX_RETRIES: 3,
  RETRY_DELAY_MS: 100,
  REQUEST_TIMEOUT_MS: 3000,
  
  // Health
  HEALTH_TIMEOUT_MS: 2000,
  
  // Circuit Breaker
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
    
    // Check TTL
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return null;
    }
    
    // Check circuit breaker
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

  // ========== CIRCUIT BREAKER ==========
  isCircuitOpen(key) {
    const breaker = this.circuitBreaker.get(key);
    if (!breaker) return false;
    
    // Check if timeout has passed
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

  // ========== CLEANUP ==========
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

  // ========== GET INSTANCE ==========
  getInstance(room, attempt = 0) {
    try {
      // Hash room untuk distribusi
      const hash = this._hashString(room + attempt);
      const instanceId = Math.abs(hash) % CONFIG.INSTANCE_COUNT;
      
      // Cache key
      const cacheKey = `instance_${instanceId}`;
      
      // Cek cache
      let cached = this.cache.get(cacheKey);
      if (cached) {
        return cached;
      }
      
      // Buat instance baru
      const id = this.env[this.className].idFromName(`game_${instanceId}`);
      const obj = this.env[this.className].get(id);
      
      // Simpan ke cache
      this.cache.set(cacheKey, obj);
      
      return obj;
      
    } catch(e) {
      throw new Error(`Failed to get instance: ${e.message}`);
    }
  }

  // ========== FETCH WITH RETRY ==========
  async fetch(request, room) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), CONFIG.REQUEST_TIMEOUT_MS);
    
    let lastError = null;
    let triedInstances = new Set();
    
    for (let attempt = 0; attempt < CONFIG.MAX_RETRIES; attempt++) {
      try {
        // Get instance dengan retry logic
        let obj = null;
        let instanceKey = null;
        
        for (let innerAttempt = 0; innerAttempt < 3; innerAttempt++) {
          try {
            const hash = this._hashString(room + attempt + innerAttempt);
            const instanceId = Math.abs(hash) % CONFIG.INSTANCE_COUNT;
            const cacheKey = `instance_${instanceId}`;
            
            // Skip jika sudah dicoba
            if (triedInstances.has(cacheKey)) {
              continue;
            }
            triedInstances.add(cacheKey);
            
            // Check circuit breaker
            if (this.cache.isCircuitOpen(cacheKey)) {
              continue;
            }
            
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
        
        // Execute request
        const response = await obj.fetch(request, {
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        // Record success
        this.cache.recordSuccess(instanceKey);
        
        // Handle response
        if (response.status === 200 || response.status === 101) {
          return response;
        }
        
        // Handle error responses
        if (response.status === 503 || response.status === 429) {
          this.cache.recordFailure(instanceKey);
          throw new Error('Instance busy or overloaded');
        }
        
        return response;
        
      } catch(error) {
        clearTimeout(timeoutId);
        lastError = error;
        
        // If abort or timeout
        if (error.name === 'AbortError') {
          throw new Error('Request timeout');
        }
        
        // If instance busy, retry
        if (error.message === 'Instance busy or overloaded') {
          await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY_MS));
          continue;
        }
        
        // For other errors, return error response
        throw error;
      }
    }
    
    clearTimeout(timeoutId);
    
    // All retries failed
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

  // ========== HEALTH CHECK ==========
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

  // ========== HASH HELPER ==========
  _hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return Math.abs(hash);
  }

  // ========== CLEANUP ==========
  cleanup() {
    this.cache.cleanup();
  }

  clear() {
    this.cache.clear();
  }
}

// ==================== MAIN EXPORT ====================
const instanceCache = new InstanceCache();
const chatPool = null; // Chat server uses single instance
let gamePool = null;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        try {
          const id = env.CHAT_SERVER.idFromName("global");
          const obj = env.CHAT_SERVER.get(id);
          return obj.fetch(request);
        } catch(e) {
          return new Response(JSON.stringify({
            error: "Chat server unavailable",
            message: e.message
          }), {
            status: 503,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws") {
        // Initialize game pool
        if (!gamePool) {
          gamePool = new InstancePool(env, 'GAME_SERVER');
        }
        
        // Get room from query parameter
        const room = url.searchParams.get("room") || "default";
        
        // Add room to request for routing
        const newRequest = new Request(request.url, {
          method: request.method,
          headers: request.headers,
          body: request.body,
          duplex: 'half'
        });
        
        // Add room header
        newRequest.headers.set('X-Room', room);
        
        // Fetch with retry
        const response = await gamePool.fetch(newRequest, room);
        
        // Add CORS headers
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
        instanceCache.clear();
        return new Response(JSON.stringify({
          status: "ok",
          message: "Cache cleared"
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== DEFAULT ==========
      return new Response(JSON.stringify({
        status: "running",
        version: "3.2.0",
        timestamp: Date.now(),
        endpoints: {
          chat: "/ws",
          chatHealth: "/chat/health",
          game: "/game",
          gameWebsocket: "/game/ws?room={room_name}",
          gameHealth: "/game/health"
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
    // Cleanup cache periodically
    if (gamePool) {
      gamePool.cleanup();
    }
    instanceCache.cleanup();
    
    console.log(`Cache cleaned at ${new Date().toISOString()}`);
  }
};

// ==================== EXPORTS FOR DU ====================
export { ChatServer, GameServer };
