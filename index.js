import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

export { ChatServer, GameServer };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    
    // ✅ HANYA WEBSOCKET
    const upgrade = request.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }
    
    // ✅ ROUTING KE DO (semua di memory)
    if (path === "/game/ws") {
      const id = env.GAME_SERVER.idFromName("main");
      const obj = env.GAME_SERVER.get(id);
      return obj.fetch(request);
    }
    
    const id = env.CHAT_SERVER.idFromName("main");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(request);
  }
};
