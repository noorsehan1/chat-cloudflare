import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export { ChatServer, GameServer };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    
    // 🔥 CEK WEBSOCKET DULU - SELALU KE CHAT SERVER
    const upgrade = request.headers.get("Upgrade");
    if (upgrade === "websocket") {
      // CEK APAKAH WEBSOCKET UNTUK GAME?
      if (path === "/game/ws") {
        const id = env.GAME_SERVER.idFromName("main");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // SELAIN ITU KE CHAT SERVER
      const id = env.CHAT_SERVER.idFromName("main");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(request);
    }
    
    // 🔥 GAME SERVER - handle /game/* (HTTP)
    if (path.startsWith("/game")) {
      const id = env.GAME_SERVER.idFromName("main");
      const obj = env.GAME_SERVER.get(id);
      return obj.fetch(request);
    }
    
    // 🔥 CHAT SERVER - handle SEMUA yang lain
    const id = env.CHAT_SERVER.idFromName("main");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(request);
  }
};
