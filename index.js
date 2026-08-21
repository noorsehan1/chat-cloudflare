// ==================== INDEX.JS - IMPROVED ====================
// VERSION: 3.3.3 - WITH LOGGING & CORS

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // Log request
      console.log(`[INDEX] ${request.method} ${pathname}`);
      
      // ========== CORS HEADERS ==========
      const corsHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Upgrade',
      };
      
      // ========== HANDLE OPTIONS ==========
      if (request.method === 'OPTIONS') {
        return new Response(null, {
          status: 204,
          headers: corsHeaders
        });
      }
      
      // ========== CHAT SERVER ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        const response = await obj.fetch(request);
        // Add CORS headers
        const newHeaders = new Headers(response.headers);
        Object.entries(corsHeaders).forEach(([key, value]) => {
          newHeaders.set(key, value);
        });
        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: newHeaders
        });
      }
      
      // ========== GAME SERVER ==========
      if (pathname === "/game/ws") {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        const response = await obj.fetch(request);
        const newHeaders = new Headers(response.headers);
        Object.entries(corsHeaders).forEach(([key, value]) => {
          newHeaders.set(key, value);
        });
        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: newHeaders
        });
      }
      
      if (pathname === "/game/health") {
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // ========== HEALTH CHECK ==========
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          timestamp: Date.now(),
          services: {
            chat: "running",
            game: "running"
          }
        }), {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            ...corsHeaders
          }
        });
      }
      
      // ========== ROOT ==========
      return new Response(JSON.stringify({
        status: "running",
        services: {
          chat: "/ws",
          game: "/game/ws",
          health: "/health"
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
      
    } catch(e) {
      console.error("[INDEX] Error:", e);
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
