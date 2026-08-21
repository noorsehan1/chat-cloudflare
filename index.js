// ==================== INDEX.JS (FIXED) ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      // === CEK APAKAH WEBSOCKET ===
      if (upgrade === "websocket") {
        // === ROUTING WEBSOCKET ===
        // Chat: /ws, /chat, atau root
        if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
          try {
            const id = env.CHAT_SERVER.idFromName("global");
            const obj = env.CHAT_SERVER.get(id);
            return obj.fetch(request);
          } catch(e) {
            console.error("ChatServer WebSocket error:", e);
            return new Response("ChatServer error: " + e.message, { status: 500 });
          }
        }
        
        // Game: /game/ws
        if (pathname === "/game/ws" || pathname === "/game") {
          try {
            const id = env.GAME_SERVER.idFromName("game");
            const obj = env.GAME_SERVER.get(id);
            return obj.fetch(request);
          } catch(e) {
            console.error("GameServer WebSocket error:", e);
            return new Response("GameServer error: " + e.message, { status: 500 });
          }
        }
        
        return new Response("WebSocket not found", { status: 404 });
      }
      
      // === HTTP ROUTES ===
      
      // === CHAT SERVER ROUTES ===
      if (pathname === "/health" || pathname === "/reset" || pathname === "/chat") {
        try {
          const id = env.CHAT_SERVER.idFromName("global");
          const obj = env.CHAT_SERVER.get(id);
          return obj.fetch(request);
        } catch(e) {
          return new Response(JSON.stringify({ 
            error: "ChatServer error", 
            details: e.message 
          }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // === GAME SERVER ROUTES ===
      if (pathname === "/game/health" || pathname === "/game") {
        try {
          const id = env.GAME_SERVER.idFromName("game");
          const obj = env.GAME_SERVER.get(id);
          return obj.fetch(request);
        } catch(e) {
          return new Response(JSON.stringify({ 
            error: "GameServer error", 
            details: e.message 
          }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      // === ROOT ===
      if (pathname === "/") {
        return new Response(
          "=== CHAT & GAME SERVER ===\n\n" +
          "Chat Server:\n" +
          "  WebSocket: wss://" + url.host + "/\n" +
          "  Health: " + url.origin + "/health\n" +
          "  Reset: POST " + url.origin + "/reset\n\n" +
          "Game Server:\n" +
          "  WebSocket: wss://" + url.host + "/game/ws\n" +
          "  Health: " + url.origin + "/game/health\n\n" +
          "Connect to Chat:\n" +
          "  const ws = new WebSocket('wss://" + url.host + "/');\n\n" +
          "Connect to Game:\n" +
          "  const ws = new WebSocket('wss://" + url.host + "/game/ws');\n",
          { 
            status: 200,
            headers: { 
              'Content-Type': 'text/plain',
              'Cache-Control': 'no-cache'
            }
          }
        );
      }
      
      // === 404 ===
      return new Response("Not Found", { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
      
    } catch(e) {
      console.error("Index.js error:", e);
      return new Response("Internal Server Error: " + e.message, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

// Export untuk Durable Objects
export { ChatServer, GameServer };
