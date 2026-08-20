// ==================== INDEX.JS - FIXED ====================
// VERSION: 3.1.2 - NO GLOBAL setInterval

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance
const instanceCache = new Map();
const CACHE_TTL = 60000;

// Rate limiting - tanpa setInterval global
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

// Helper rate limit - dengan cleanup otomatis
function checkRateLimit(ip) {
  const now = Date.now();
  
  // Cleanup expired entries (hanya saat dipanggil)
  for (const [key, record] of rateLimitCache) {
    if (now - record.timestamp > RATE_LIMIT_WINDOW) {
      rateLimitCache.delete(key);
    }
  }
  
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

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ============================================================
      // CHAT SERVER
      // ============================================================
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
      
      // ============================================================
      // GAME SERVER - SAME ROOM = SAME INSTANCE
      // ============================================================
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
        
        // ✅ FIX: Hash HANYA berdasarkan ROOM
        const hash = await hashString(room);
        const instanceId = Math.abs(hash) % 3;
        
        // Cache key: room saja
        const cacheKey = `game_${room}`;
        let cached = instanceCache.get(cacheKey);
        
        // Cek cache expired
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
        
        // Timeout 2 detik
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
          
          // Jika error, coba instance lain
          if (response.status === 503 || response.status === 429) {
            instanceCache.delete(cacheKey);
            
            for (let attempt = 0; attempt < 2; attempt++) {
              try {
                const fallbackId = (instanceId + 1 + attempt) % 3;
                const fallbackObj = env.GAME_SERVER.get(
                  env.GAME_SERVER.idFromName(`game_${fallbackId}`)
                );
                
                const fallbackResponse = await fallbackObj.fetch(request, {
                  signal: AbortSignal.timeout(2000)
                });
                
                if (fallbackResponse.status === 200 || fallbackResponse.status === 101) {
                  return fallbackResponse;
                }
              } catch(e) {}
            }
            
            return new Response(JSON.stringify({
              error: "Game server busy",
              retryAfter: 5
            }), { 
              status: 503,
              headers: { 
                'Retry-After': '5',
                'Content-Type': 'application/json'
              }
            });
          }
          
          return response;
          
        } catch (error) {
          clearTimeout(timeoutId);
          
          // Timeout, coba instance lain
          for (let attempt = 0; attempt < 2; attempt++) {
            try {
              const fallbackId = (instanceId + 1 + attempt) % 3;
              const fallbackObj = env.GAME_SERVER.get(
                env.GAME_SERVER.idFromName(`game_${fallbackId}`)
              );
              
              const fallbackResponse = await fallbackObj.fetch(request, {
                signal: AbortSignal.timeout(2000)
              });
              
              if (fallbackResponse.status === 200 || fallbackResponse.status === 101) {
                return fallbackResponse;
              }
            } catch(e) {}
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
      }
      
      // ============================================================
      // GAME HEALTH CHECK
      // ============================================================
      if (pathname === "/game/health") {
        const results = [];
        let totalConnections = 0;
        let totalGames = 0;
        let healthyCount = 0;
        
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
          cacheSize: instanceCache.size
        }), {
          headers: { 
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
          }
        });
      }
      
      // ============================================================
      // GAME INFO
      // ============================================================
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "3.1.2",
          instances: 3,
          maxConnections: 30,
          timestamp: Date.now(),
          endpoints: {
            websocket: "/game/ws?room={room_name}&username={username}",
            health: "/game/health"
          },
          note: "Same room = same instance (1 room 1 instance)"
        }), {
          headers: { 
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
          }
        });
      }
      
      // ============================================================
      // CLEAR CACHE
      // ============================================================
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
      
      // ============================================================
      // 404
      // ============================================================
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
      console.error("Error:", e);
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
  }
};

export { ChatServer, GameServer };
