// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      // === WEBSOCKET ROUTING ===
      // INI PENTING: Tangani SEMUA WebSocket
      if (upgrade === "websocket") {
        // Root path "/" atau "/ws" atau "/chat"
        if (pathname === "/" || pathname === "/ws" || pathname === "/chat") {
          const id = env.CHAT_SERVER.idFromName("global");
          const obj = env.CHAT_SERVER.get(id);
          return obj.fetch(request);
        }
        
        // Game WebSocket
        if (pathname === "/game/ws" || pathname === "/game") {
          const id = env.GAME_SERVER.idFromName("game");
          const obj = env.GAME_SERVER.get(id);
          return obj.fetch(request);
        }
        
        return new Response("WebSocket not found", { status: 404 });
      }
      
      // === HTTP ROUTES ===
      if (pathname === "/health") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      if (pathname === "/") {
        return new Response(
          "Chat Server Running\n" +
          "WebSocket: wss://" + url.host + "/\n" +
          "Health: " + url.origin + "/health\n",
          { 
            status: 200,
            headers: { 'Content-Type': 'text/plain' }
          }
        );
      }
      
      return new Response("Not Found", { status: 404 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};

export { ChatServer };
