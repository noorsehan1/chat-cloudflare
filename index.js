// ==================== INDEX.JS ====================
// VERSION: 1.0.0 - CHAT + GAME

import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

const chatServer = new ChatServer();
const gameServer = new GameServer();

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // Chat
    if (path === "/ws" || path === "/chat" || path === "/") {
      chatServer.start();
      return chatServer.fetch(request);
    }

    // Game
    if (path === "/game/ws" || path === "/game/health" || path === "/game/metrics") {
      return gameServer.fetch(request);
    }

    // Health
    if (path === "/health") {
      return new Response(JSON.stringify({
        chat: {
          connections: chatServer.wsSet?.size || 0,
          rooms: chatServer.rooms?.size || 0,
          users: chatServer.userSeat?.size || 0
        },
        game: {
          connections: gameServer.wsMap?.size || 0,
          games: gameServer.activeGames?.size || 0,
          diceActive: !!gameServer.currentDiceRoll,
          tieActive: gameServer._tieActive || false
        },
        uptime: Math.floor((Date.now() - chatServer._startTime) / 1000)
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    return new Response("Chat + Game Server Running", { status: 200 });
  }
};

export { ChatServer, GameServer };