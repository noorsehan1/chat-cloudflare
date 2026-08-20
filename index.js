// ==================== INDEX.JS ====================
// VERSION: 4.0.1 - WITH ALARM SYSTEM

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

const instanceCache = new Map();
const CACHE_TTL = 60000;

async function hashString(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.reduce((acc, byte) => acc + byte, 0);
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws") {
        const room = url.searchParams.get("room") || "default";
        const username = url.searchParams.get("username") || "guest";
        
        if (room.length > 50 || username.length > 50) {
          return new Response("Invalid input", { status: 400 });
        }
        
        const hash = await hashString(room);
        const instanceId = Math.abs(hash) % 3;
        
        const cacheKey = `game_${room}`;
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
          instanceCache.set(cacheKey, { instance: obj, timestamp: Date.now() });
        }
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000);
        
        try {
          const response = await obj.fetch(request, { signal: controller.signal });
          clearTimeout(timeoutId);
          
          if (response.status === 200 || response.status === 101) {
            return response;
          }
          
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
              error: "Server busy, retry later",
              retryAfter: 5
            }), { 
              status: 503,
              headers: { 'Retry-After': '5', 'Content-Type': 'application/json' }
            });
          }
          
          return response;
          
        } catch (error) {
          clearTimeout(timeoutId);
          
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
            error: "All servers busy",
            retryAfter: 5
          }), { 
            status: 503,
            headers: { 'Retry-After': '5', 'Content-Type': 'application/json' }
          });
        }
      }
      
      // ========== HEALTH CHECK ==========
      if (pathname === "/health" || pathname === "/game/health") {
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          version: "4.0.1"
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== GAME INFO ==========
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "4.0.1",
          endpoints: {
            websocket: "/game/ws?room={room}"
          }
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      return new Response("Not Found", { status: 404 });
      
    } catch(e) {
      return new Response(JSON.stringify({
        error: "Internal Server Error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};

export { ChatServer, GameServer };
