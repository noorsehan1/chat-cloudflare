// ==================== INDEX.JS - PURE WORKER WITH CACHE ====================
import { ChatServer } from "./chat-server.js";

let chatServer = null;
let initialized = false;
const CACHE_NAME = 'chat_persist';

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // ========== INIT ==========
      if (!initialized) {
        const cache = await caches.open(CACHE_NAME);
        
        chatServer = new ChatServer({
          env: env,
          cache: cache
        });
        
        initialized = true;
        
        // SAVE STATE SETIAP 2 DETIK
        setInterval(async () => {
          if (chatServer && !chatServer.closing) {
            try {
              await chatServer.saveToCache();
            } catch(e) {}
          }
        }, 2000);
      }
      
      // ========== ROUTING ==========
      if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
        return chatServer.fetch(request);
      }
      
      if (pathname === "/health") {
        const rooms = {};
        for (const [name, room] of chatServer.rooms) {
          rooms[name] = {
            seats: room.seats.size,
            users: Array.from(room.users || [])
          };
        }
        
        return new Response(JSON.stringify({
          status: "running",
          connections: chatServer.wsSet.size,
          rooms: rooms,
          totalUsers: chatServer.userSeat.size,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      return new Response("Server running", { status: 200 });
      
    } catch(e) {
      return new Response(JSON.stringify({ error: e.message }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};
