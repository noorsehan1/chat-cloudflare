// ==================== INDEX.JS - OPTIMIZED ====================
// VERSION: 3.1.1 - OPTIMIZED FREE TIER

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Cache untuk instance Durable Objects
const instanceCache = new Map();
const CACHE_TTL = 30000; // 30 detik

// Rate limiting per IP sederhana
const rateLimitCache = new Map();
const RATE_LIMIT_WINDOW = 60000; // 1 menit
const RATE_LIMIT_MAX = 100; // Max request per menit per IP

// Helper untuk hash string
async function hashString(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.reduce((acc, byte) => acc + byte, 0);
}

// Helper untuk rate limiting
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
  
  if (record.count >= RATE_LIMIT_MAX) {
    return false;
  }
  
  record.count++;
  rateLimitCache.set(ip, record);
  return true;
}

// Cleanup rate limit cache setiap 5 menit
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
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          version: "3.1.1",
          timestamp: Date.now(),
          uptime: process.uptime ? process.uptime() : 0,
          memory: process.memoryUsage ? process.memoryUsage() : {}
        }), {
          headers: { 
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
          }
        });
      }
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        try {
          const id = env.CHAT_SERVER.idFromName("global");
          const obj = env.CHAT_SERVER.get(id);
          return obj.fetch(request);
        } catch(e) {
          console.error("Chat server error:", e);
          return new Response(JSON.stringify({
            error: "Chat server unavailable",
            message: e.message
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
        
        // Validasi input
        if (room.length > 50 || username.length > 50) {
          return new Response(JSON.stringify({
            error: "Invalid room or username (max 50 chars)"
          }), { 
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          });
        }
        
        let lastError = null;
        
        // Coba 3 instance game server
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            // Hash untuk distribusi beban
            const hash = await hashString(room + username + attempt);
            const instanceId = Math.abs(hash) % 3;
            
            // Cache key
            const cacheKey = `game_${room}_${instanceId}`;
            let cached = instanceCache.get(cacheKey);
            
            // Cek expired cache
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
              
              // Jika sukses, return
              if (response.status === 200 || response.status === 101) {
                return response;
              }
              
              // Jika server sibuk, coba instance lain
              if (response.status === 503 || response.status === 429) {
                throw new Error('Instance busy');
              }
              
              // Jika error lain, return response
              return response;
              
            } catch (error) {
              clearTimeout(timeoutId);
              lastError = error;
              
              // Jika timeout atau busy, coba instance lain
              if (error.name === 'AbortError' || error.message === 'Instance busy') {
                // Hapus cache yang bermasalah
                const badKey = `game_${room}_${instanceId}`;
                instanceCache.delete(badKey);
                console.log(`Instance ${instanceId} busy, retrying... (${attempt + 1}/3)`);
                continue;
              }
              throw error;
            }
            
          } catch (error) {
            lastError = error;
            if (attempt === 2) {
              // Jika semua attempt gagal
              console.error("All game instances failed:", error);
            }
            // Tunggu sebentar sebelum retry
            await new Promise(resolve => setTimeout(resolve, 50 * (attempt + 1)));
          }
        }
        
        // Jika semua retry gagal
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
      
      // ========== GAME HEALTH CHECK ==========
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
                  queue: data.queue || 0,
                  uptime: data.uptime || 0
                });
                totalConnections += data.connections || 0;
                totalGames += data.games || 0;
                healthyCount++;
              } else {
                results.push({ id: i, status: "unhealthy", statusCode: resp.status });
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
          healthyCount,
          totalInstances: 3,
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
      
      // ========== GAME STATUS ==========
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "3.1.1",
          instances: 3,
          maxConnections: 30,
          maxGames: 5,
          timestamp: Date.now(),
          endpoints: {
            websocket: "/game/ws?room={room_name}&username={username}",
            health: "/game/health",
            status: "/game/status"
          },
          documentation: "For game server documentation, see README.md"
        }), {
          status: 200,
          headers: { 
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
          }
        });
      }
      
      // ========== GAME STATUS DETAIl ==========
      if (pathname === "/game/status") {
        try {
          // Cek 3 instance
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
                    ...data
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
            status: "ok",
            timestamp: Date.now(),
            instances: instanceStatus,
            cache: {
              size: instanceCache.size,
              ttl: CACHE_TTL
            }
          }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({
            error: "Failed to get status",
            message: e.message
          }), { 
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // ========== CLEAR CACHE (Admin) ==========
      if (pathname === "/admin/clear-cache") {
        // Simple auth check (gunakan environment variable)
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
          message: "Cache cleared",
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
        available: [
          "/",
          "/ws",
          "/chat",
          "/game",
          "/game/ws",
          "/game/health",
          "/game/status",
          "/health",
          "/admin/clear-cache"
        ]
      }), { 
        status: 404,
        headers: { 
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache'
        }
      });
      
    } catch(e) {
      console.error("Index fetch error:", e);
      
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error",
        timestamp: Date.now()
      }), { 
        status: 500,
        headers: { 
          'Retry-After': '30',
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache'
        }
      });
    }
  }
};

// Export classes untuk worker
export { ChatServer, GameServer };
