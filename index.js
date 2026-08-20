// ==================== INDEX.JS - OPTIMIZED FREE TIER ====================
// VERSION: 3.1.1 - MINIMALIST & OPTIMIZED

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance Durable Objects
const instanceCache = new Map();
const CACHE_TTL = 30000; // 30 detik

// Rate limiting sederhana
const rateLimitCache = new Map();
const RATE_LIMIT_WINDOW = 60000;
const RATE_LIMIT_MAX = 100;

// Helper hash
async function hashString(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.reduce((acc, byte) => acc + byte, 0);
}

// Helper rate limit
function checkRateLimit(ip) {
  const now = Date.now();
  const record = rateLimitCache.get(ip);
  
  if (!record) {
    rateLimitCache.set(ip, { count: 1, timestamp: now });
    return true;
  }
  
  if (now - record.timestamp > RATE_LIMIT_WINDOW) {
    rateLimitCache.set(ip, { count: 1, timestamp: now });
    return true;
  }
  
  if (record.count >= RATE_LIMIT_MAX) return false;
  
  record.count++;
  return true;
}

// Cleanup rate limit
setInterval(() => {
  const now = Date.now();
  for (const [ip, record] of rateLimitCache) {
    if (now - record.timestamp > RATE_LIMIT_WINDOW) {
      rateLimitCache.delete(ip);
    }
  }
}, 300000);

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== HEALTH CHECK ==========
      if (pathname === "/health" || pathname === "/game/health") {
        try {
          const results = [];
          let totalConnections = 0;
          let totalGames = 0;
          let healthyCount = 0;
          
          // Cek 3 instance game server
          for (let i = 0; i < 3; i++) {
            try {
              const id = env.GAME_SERVER.idFromName(`game_${i}`);
              const obj = env.GAME_SERVER.get(id);
              
              const controller = new AbortController();
              const timeoutId = setTimeout(() => controller.abort(), 1500);
              
              try {
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
                    queue: data.queue || 0
                  });
                  totalConnections += data.connections || 0;
                  totalGames += data.games || 0;
                  healthyCount++;
                } else {
                  results.push({ id: i, status: "unhealthy" });
                }
              } catch(e) {
                clearTimeout(timeoutId);
                results.push({ id: i, status: "error", error: e.message });
              }
            } catch(e) {
              results.push({ id: i, status: "error", error: e.message });
            }
          }
          
          return new Response(JSON.stringify({
            status: healthyCount >= 2 ? "ok" : "degraded",
            timestamp: Date.now(),
            instances: results,
            totalConnections,
            totalGames,
            cacheSize: instanceCache.size,
            chatServer: "running"
          }), {
            headers: { 
              'Content-Type': 'application/json',
              'Cache-Control': 'no-cache'
            }
          });
        } catch(e) {
          return new Response(JSON.stringify({
            status: "error",
            message: e.message
          }), { 
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        try {
          const id = env.CHAT_SERVER.idFromName("global");
          const obj = env.CHAT_SERVER.get(id);
          return obj.fetch(request);
        } catch(e) {
          return new Response(JSON.stringify({
            error: "Chat server unavailable"
          }), { 
            status: 503,
            headers: { 
              'Retry-After': '10',
              'Content-Type': 'application/json'
            }
          });
        }
      }
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws") {
        // Rate limiting
        const clientIP = request.headers.get("CF-Connecting-IP") || 
                         request.headers.get("X-Forwarded-For") || 
                         "unknown";
        
        if (!checkRateLimit(clientIP)) {
          return new Response(JSON.stringify({
            error: "Rate limit exceeded",
            retryAfter: 60
          }), { 
            status: 429,
            headers: { 
              'Retry-After': '60',
              'Content-Type': 'application/json'
            }
          });
        }
        
        const room = url.searchParams.get("room") || "default";
        const username = url.searchParams.get("username") || "guest";
        
        // Validasi
        if (room.length > 50 || username.length > 50) {
          return new Response(JSON.stringify({
            error: "Invalid input"
          }), { 
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          });
        }
        
        let lastError = null;
        
        // Coba 3 instance
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            const hash = await hashString(room + username + attempt);
            const instanceId = Math.abs(hash) % 3;
            
            const cacheKey = `game_${room}_${instanceId}`;
            let cached = instanceCache.get(cacheKey);
            
            if (cached && (Date.now() - cached.timestamp > CACHE_TTL)) {
              instanceCache.delete(cacheKey);
              cached = null;
            }
            
            let obj;
            if (cached) {
              obj = cached.instance;
            } else {
              const id = env.GAME_SERVER.idFromName(`game_${instanceId}`);
              obj = env.GAME_SERVER.get(id);
              instanceCache.set(cacheKey, {
                instance: obj,
                timestamp: Date.now()
              });
            }
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 2000);
            
            try {
              const response = await obj.fetch(request, {
                signal: controller.signal
              });
              clearTimeout(timeoutId);
              
              if (response.status === 200 || response.status === 101) {
                return response;
              }
              
              if (response.status === 503 || response.status === 429) {
                throw new Error('Instance busy');
              }
              
              return response;
              
            } catch (error) {
              clearTimeout(timeoutId);
              lastError = error;
              
              if (error.name === 'AbortError' || error.message === 'Instance busy') {
                const badKey = `game_${room}_${instanceId}`;
                instanceCache.delete(badKey);
                continue;
              }
              throw error;
            }
            
          } catch (error) {
            lastError = error;
            if (attempt === 2) break;
            await new Promise(resolve => setTimeout(resolve, 50 * (attempt + 1)));
          }
        }
        
        return new Response(JSON.stringify({
          error: "All game servers busy",
          retryAfter: 5
        }), { 
          status: 503,
          headers: { 
            'Retry-After': '5',
            'Content-Type': 'application/json'
          }
        });
      }
      
      // ========== GAME INFO ==========
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "3.1.1",
          instances: 3,
          maxConnections: 30,
          timestamp: Date.now(),
          endpoints: {
            websocket: "/game/ws?room={room}",
            health: "/game/health"
          }
        }), {
          headers: { 
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
          }
        });
      }
      
      // ========== GAME STATUS ==========
      if (pathname === "/game/status") {
        try {
          const instanceStatus = [];
          for (let i = 0; i < 3; i++) {
            try {
              const id = env.GAME_SERVER.idFromName(`game_${i}`);
              const obj = env.GAME_SERVER.get(id);
              
              const controller = new AbortController();
              const timeoutId = setTimeout(() => controller.abort(), 1000);
              
              try {
                const resp = await obj.fetch(new Request("https://dummy/metrics"), {
                  signal: controller.signal
                });
                clearTimeout(timeoutId);
                
                if (resp.ok) {
                  const data = await resp.json();
                  instanceStatus.push({
                    id: i,
                    status: "ok",
                    connections: data.connections || 0,
                    games: data.games || 0,
                    queue: data.queue || 0,
                    diceActive: data.diceActive || false
                  });
                } else {
                  instanceStatus.push({ id: i, status: "error", code: resp.status });
                }
              } catch(e) {
                clearTimeout(timeoutId);
                instanceStatus.push({ id: i, status: "error", error: e.message });
              }
            } catch(e) {
              instanceStatus.push({ id: i, status: "error", error: e.message });
            }
          }
          
          return new Response(JSON.stringify({
            timestamp: Date.now(),
            instances: instanceStatus,
            cacheSize: instanceCache.size
          }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({
            error: "Failed to get status"
          }), { 
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // ========== CLEAR CACHE ==========
      if (pathname === "/admin/clear-cache") {
        const authHeader = request.headers.get("Authorization");
        const expectedAuth = env.ADMIN_TOKEN || "admin-secret-token";
        
        if (authHeader !== `Bearer ${expectedAuth}`) {
          return new Response(JSON.stringify({
            error: "Unauthorized"
          }), { 
            status: 401,
            headers: { 'Content-Type': 'application/json' }
          });
        }
        
        const size = instanceCache.size;
        instanceCache.clear();
        rateLimitCache.clear();
        
        return new Response(JSON.stringify({
          success: true,
          cleared: size,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== 404 ==========
      return new Response(JSON.stringify({
        error: "Not Found",
        path: pathname,
        endpoints: [
          "/",
          "/ws",
          "/chat",
          "/game",
          "/game/ws",
          "/game/health",
          "/game/status",
          "/health"
        ]
      }), { 
        status: 404,
        headers: { 
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache'
        }
      });
      
    } catch(e) {
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        timestamp: Date.now()
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

// Export classes
export { ChatServer, GameServer };
