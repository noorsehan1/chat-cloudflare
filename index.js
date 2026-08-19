// ==================== INDEX.JS ====================
// VERSION: 1.0.0 - SIMPLE PURE WORKER CHAT

import { ChatServer } from "./chat-server.js";

// ============================================================
// ✅ CHAT SERVER INSTANCE
// ============================================================
const chatServer = new ChatServer();

// ============================================================
// ✅ MAIN WORKER
// ============================================================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // ========== WEBSOCKET ==========
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      chatServer.start();
      return chatServer.fetch(request);
    }

    // ========== HEALTH ==========
    if (pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: chatServer.wsSet?.size || 0,
        users: chatServer.userSeat?.size || 0,
        uptime: Math.floor((Date.now() - chatServer._startTime) / 1000)
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    // ========== STATS ==========
    if (pathname === "/stats") {
      const rooms = {};
      for (const [room, roomMan] of chatServer.rooms) {
        rooms[room] = roomMan?.getCount() || 0;
      }

      return new Response(JSON.stringify({
        connections: chatServer.wsSet?.size || 0,
        rooms: rooms,
        users: chatServer.userSeat?.size || 0,
        number: chatServer.currentNumber || 1
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    // ========== DEFAULT ==========
    return new Response("Chat Server Running", { status: 200 });
  }
};

export { ChatServer };
