 import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export { ChatServer, GameServer };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    
    // 🔥 GAME SERVER - handle /game/*
    if (path.startsWith("/game")) {
      const id = env.GAME_SERVER.idFromName("main");
      const obj = env.GAME_SERVER.get(id);
      return obj.fetch(request);
    }
    
    // 🔥 CHAT SERVER - handle SEMUA yang lain (termasuk /, /ws, /chat, /health)
    // Ini akan menangkap WebSocket upgrade juga
    const id = env.CHAT_SERVER.idFromName("main");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(request);
  }
};
