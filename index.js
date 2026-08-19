// ==================== INDEX.JS ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

// Inisialisasi singleton servers
let chatServerInstance = null;
let gameServerInstance = null;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      
      // Inisialisasi server jika belum ada
      if (!chatServerInstance) {
        chatServerInstance = new ChatServer(env);
      }
      
      if (!gameServerInstance) {
        gameServerInstance = new GameServer(env);
      }
      
      // ==== HEALTH CHECK ====
      if (pathname === "/health" || pathname === "/healthz") {
        const health = {
          status: "ok",
          timestamp: Date.now(),
          servers: {
            chat: {
              running: true,
              connections: chatServerInstance?.wsSet?.size || 0,
              rooms: chatServerInstance?.rooms?.size || 0,
            },
            game: {
              running: true,
              connections: gameServerInstance?.wsMap?.size || 0,
              activeGames: gameServerInstance?.activeGames?.size || 0,
              diceActive: !!gameServerInstance?.currentDiceRoll,
            }
          }
        };
        
        return new Response(JSON.stringify(health, null, 2), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          }
        });
      }
      
      // ==== WEBSOCKET ROUTING ====
      const upgrade = request.headers.get("Upgrade");
      if (upgrade === "websocket") {
        // ROUTING BERDASARKAN PATH
        // Chat: /chat atau /ws atau /
        // Game: /game
        
        if (pathname === "/game" || pathname === "/game/ws") {
          // Koneksi GAME
          console.log("🟢 Game connection to:", pathname);
          return gameServerInstance.fetch(request);
        } else {
          // Koneksi CHAT (default untuk /, /chat, /ws)
          console.log("🔵 Chat connection to:", pathname);
          return chatServerInstance.fetch(request);
        }
      }
      
      // ==== ROOT / INFO ====
      if (pathname === "/" || pathname === "") {
        return new Response(JSON.stringify({
          name: "Chat & Game Server",
          version: "1.0.0",
          endpoints: {
            chat: "wss://chat-cloudflare.chatmozapp.workers.dev/",
            game: "wss://chat-cloudflare.chatmozapp.workers.dev/game",
            health: "https://chat-cloudflare.chatmozapp.workers.dev/health"
          },
          rooms: ["LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party", "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES", "Happy Vibes", "The Chatter Room"]
        }, null, 2), {
          status: 200,
          headers: {
            "Content-Type": "application/json"
          }
        });
      }
      
      return new Response("Server running", { 
        status: 200,
        headers: {
          "Content-Type": "text/plain"
        }
      });
      
    } catch(e) {
      return new Response(JSON.stringify({
        error: e.message,
        stack: e.stack
      }), { 
        status: 500,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
  }
};

export { ChatServer, GameServer };
