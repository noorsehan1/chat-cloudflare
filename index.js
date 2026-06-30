import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export { ChatServer, GameServer };

// ✅ RATE LIMITER & DDOS PROTECTION
const RATE_LIMIT = {
  windowMs: 60000,
  maxRequests: 60,
  burstLimit: 10,
  burstWindowMs: 1000,
};

const burstTracker = new Map();
const rateLimiter = new Map();

function checkBurstLimit(ip) {
  const now = Date.now();
  const record = burstTracker.get(ip);
  
  if (!record || (now - record.timestamp) > RATE_LIMIT.burstWindowMs) {
    burstTracker.set(ip, { count: 1, timestamp: now });
    return true;
  }
  
  if (record.count >= RATE_LIMIT.burstLimit) {
    return false;
  }
  
  record.count++;
  return true;
}

function checkRateLimit(ip) {
  const now = Date.now();
  const record = rateLimiter.get(ip);
  
  if (!record || (now - record.timestamp) > RATE_LIMIT.windowMs) {
    rateLimiter.set(ip, { count: 1, timestamp: now });
    return true;
  }
  
  if (record.count >= RATE_LIMIT.maxRequests) {
    return false;
  }
  
  record.count++;
  return true;
}

function getClientIP(request) {
  try {
    return request.headers.get("CF-Connecting-IP") ||
           request.headers.get("X-Forwarded-For")?.split(",")[0].trim() ||
           request.headers.get("X-Real-IP") ||
           "unknown";
  } catch(e) {
    return "unknown";
  }
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const path = url.pathname;
      
      // ✅ DDOS PROTECTION - CEK BURST
      const ip = getClientIP(request);
      if (!checkBurstLimit(ip)) {
        return new Response("Too many requests. Please slow down.", { 
          status: 429,
          headers: { "Retry-After": "10" }
        });
      }
      
      // ✅ RATE LIMITING
      if (!checkRateLimit(ip)) {
        return new Response("Rate limit exceeded. Please wait.", { 
          status: 429,
          headers: { "Retry-After": "60" }
        });
      }
      
      // 🔥 CEK APAKAH WEBSOCKET?
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("WebSocket only", { 
          status: 400,
          headers: { "Content-Type": "text/plain" }
        });
      }
      
      // 🔥 CEK PATH UNTUK GAME ATAU CHAT
      if (path === "/game/ws") {
        const id = env.GAME_SERVER.idFromName("main");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // CHAT - WSS (DEFAULT)
      const id = env.CHAT_SERVER.idFromName("main");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(request);
      
    } catch(error) {
      return new Response("Internal Server Error", { 
        status: 500,
        headers: { "Content-Type": "text/plain" }
      });
    }
  }
};
