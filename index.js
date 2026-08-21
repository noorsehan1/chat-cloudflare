// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      // === CHAT SERVER ===
      // Perhatikan: WebSocket harus dideteksi SEBELUM response HTTP
      if (upgrade === "websocket") {
        // Arahkan SEMUA WebSocket ke ChatServer
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // === HTTP ROUTES ===
      if (pathname === "/reset" && request.method === "POST") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      if (pathname === "/health") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // === ROOT (HTTP) ===
      if (pathname === "/") {
        return new Response("Chat Server Running\nWebSocket: wss://" + url.host + "/\n", { 
          status: 200,
          headers: { 'Content-Type': 'text/plain' }
        });
      }
      
      return new Response("Not Found", { status: 404 });
      
    } catch(e) {
      return new Response("Error: " + e.message, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

export { ChatServer };
