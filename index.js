// ==================== INDEX.JS ====================
// VERSION: 4.0.0 - D1 + KV DATABASE VERSION

import { ChatHandler } from "./chat-handler.js";
import { GameHandler } from "./game-handler.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // CHAT SERVER
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        const handler = new ChatHandler(env);
        return handler.handle(request);
      }
      
      // GAME SERVER
      if (pathname === "/game/ws") {
        const handler = new GameHandler(env);
        return handler.handle(request);
      }
      
      if (pathname === "/game/health") {
        const handler = new GameHandler(env);
        return handler.healthCheck(request);
      }
      
      if (pathname === "/game") {
        return new Response(JSON.stringify({
          status: "running",
          version: "4.0.0",
          type: "D1 + KV Database",
          timestamp: Date.now(),
          endpoints: {
            websocket: "/game/ws?room={room_name}",
            health: "/game/health",
            chat: "/ws"
          }
        }), {
          status: 200,
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
        headers: { 
          'Retry-After': '30',
          'Content-Type': 'application/json'
        }
      });
    }
  }
};

export { ChatHandler, GameHandler };
