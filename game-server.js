// ==================== GAME-SERVER.JS ====================
// VERSION: 4.0.0 - OUTPUT PERSIS SEPERTI ASLI

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const DICE_ROOM = "Quiz";

// ==================== WEBSOCKET STORAGE ====================
const wsConnections = new Map();

export class GameServer {
  constructor(env) {
    this.env = env;
    this.wsConnections = wsConnections;
    this.ROOMS = ROOMS;
    this.ROOMS_SET = ROOMS_SET;
    this.DICE_ROOM = DICE_ROOM;
  }

  // ========== FETCH ==========
  async fetch(request) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (pathname === "/game/ws" || pathname === "/game") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Game Server", { status: 200 });
      }

      const room = url.searchParams.get("room") || "LowCard";
      const username = url.searchParams.get("username") || "Anonymous";

      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];

      try {
        server.accept();
      } catch(e) {
        return new Response("WebSocket acceptance failed", { status: 500 });
      }

      const wsId = crypto.randomUUID();

      wsConnections.set(wsId, {
        ws: server,
        username: username,
        room: room,
        connectedAt: Date.now()
      });

      const env = this.env;
      const existing = await env.DB.prepare(
        "SELECT username FROM users WHERE username = ?"
      ).bind(username).first();

      if (existing) {
        await env.DB.prepare(
          "UPDATE users SET ws_id = ?, room = ?, active = 1, last_active = ? WHERE username = ?"
        ).bind(wsId, room, Date.now(), username).run();
      } else {
        await env.DB.prepare(
          "INSERT INTO users (username, ws_id, room, active, last_active) VALUES (?, ?, ?, 1, ?)"
        ).bind(username, wsId, room, Date.now()).run();
      }

      server.addEventListener("message", async (event) => {
        try {
          let data;
          try {
            data = JSON.parse(event.data);
          } catch(e) {
            return;
          }
          if (!Array.isArray(data) || !data.length) return;

          const [evt, ...args] = data;
          await this.handleEvent(server, evt, args, env);
        } catch(e) {
          console.error("Message error:", e);
        }
      });

      server.addEventListener("close", async () => {
        wsConnections.delete(wsId);
        await env.DB.prepare(
          "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
        ).bind(Date.now(), wsId).run();
      });

      server.addEventListener("error", async () => {
        wsConnections.delete(wsId);
        await env.DB.prepare(
          "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
        ).bind(Date.now(), wsId).run();
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    if (pathname === "/game/health") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: wsConnections.size,
        timestamp: Date.now()
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response("Game Server", { status: 200 });
  }

  // ========== HANDLE EVENT ==========
  async handleEvent(ws, evt, args, env) {
    switch(evt) {

      // ============================================================
      // SWITCH ROOM
      // ============================================================
      case "switchRoom": {
        const [room, username] = args;
        if (!room || !ROOMS_SET.has(room)) {
          ws.send(JSON.stringify(["gameLowCardError", "Invalid room"]));
          return;
        }

        await env.DB.prepare(
          "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
        ).bind(room, Date.now(), username).run();

        ws.send(JSON.stringify(["switchRoomSuccess", room]));
        await this.handleGetGameState(ws, room, env);
        break;
      }

      // ============================================================
      // GAME LOWCARD START
      // ============================================================
      case "gameLowCardStart": {
        const [bet, username] = args;
        await this.handleGameStart(ws, bet, username, env);
        break;
      }

      // ============================================================
      // GAME LOWCARD JOIN
      // ============================================================
      case "gameLowCardJoin": {
        const [username] = args;
        await this.handleGameJoin(ws, username, env);
        break;
      }

      // ============================================================
      // GAME LOWCARD NUMBER
      // ============================================================
      case "gameLowCardNumber": {
        const [number, tanda, username] = args;
        await this.handleGameNumber(ws, number, tanda, username, env);
        break;
      }

      // ============================================================
      // GAME LOWCARD LEAVE
      // ============================================================
      case "gameLowCardLeave": {
        const [username] = args;
        await this.handleGameLeave(ws, username, env);
        break;
      }

      // ============================================================
      // CHECK GAME RUNNING
      // ============================================================
      case "checkGameRunning": {
        const [room] = args;
        await this.handleCheckGame(ws, room, env);
        break;
      }

      // ============================================================
      // GET GAME STATE
      // ============================================================
      case "getGameState": {
        const [room] = args;
        await this.handleGetGameState(ws, room, env);
        break;
      }

      // ============================================================
      // DICE EVENTS
      // ============================================================
      case "submitDiceAnswer": {
        const [username, guess] = args;
        await this.handleDiceAnswer(ws, username, guess, env);
        break;
      }

      case "getDiceLastWeekWinner": {
        const winner = await env.QUESTIONS.get('dice_last_week_winner', 'json');
        if (winner) {
          ws.send(JSON.stringify(["diceLastWeekWinner", winner.username, winner.score || 0, winner.week || ""]));
        } else {
          ws.send(JSON.stringify(["diceLastWeekWinner", "", 0, ""]));
        }
        break;
      }

      case "getDiceLeaderboard": {
        const points = await env.QUESTIONS.get('dice_points', 'json') || {};
        const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]).slice(0, 10);
        ws.send(JSON.stringify(["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]));
        break;
      }

      case "deleteDiceLastWeekWinner": {
        await env.QUESTIONS.delete('dice_last_week_winner');
        ws.send(JSON.stringify(["diceLastWeekWinnerDeleted", true, "Deleted"]));
        await this.broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner deleted"], env);
        break;
      }

      case "getDiceStatus": {
        const dice = await env.QUESTIONS.get('dice_current', 'json') || {};
        ws.send(JSON.stringify(["diceStatus", !!dice.active, dice.round || 1]));
        break;
      }

      // ============================================================
      // RECORDING EVENTS
      // ============================================================
      case "startRecordingWinners": {
        const [roomName] = args;
        if (roomName) {
          await env.QUESTIONS.put(`lowcard_recording_status_${roomName}`, 'true');
          await this.broadcastToRoom(roomName, ["recordingStatus", true], env);
          ws.send(JSON.stringify(["startRecordingResult", { success: true, message: "Recording enabled" }]));
        }
        break;
      }

      case "stopRecordingWinners": {
        const [roomName] = args;
        if (roomName) {
          await env.QUESTIONS.delete(`lowcard_recording_status_${roomName}`);
          await env.QUESTIONS.delete(`lowcard_winner_${roomName}`);
          await this.broadcastToRoom(roomName, ["recordingStatus", false], env);
          ws.send(JSON.stringify(["stopRecordingResult", { success: true, message: "Recording stopped" }]));
        }
        break;
      }

      case "getRecordingStatus": {
        const [roomName] = args;
        if (roomName) {
          const status = await env.QUESTIONS.get(`lowcard_recording_status_${roomName}`);
          ws.send(JSON.stringify(["recordingStatus", status === 'true']));
        }
        break;
      }

      case "sendWinnersToRoom": {
        const [room] = args;
        if (room) {
          const winners = await env.QUESTIONS.get(`lowcard_winner_${room}`, 'json') || {};
          await this.broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }], env);
          ws.send(JSON.stringify(["sendWinnersResult", { success: true, message: "Winners refreshed" }]));
        }
        break;
      }

      case "getRoomWinners": {
        const [room] = args;
        if (room) {
          const winners = await env.QUESTIONS.get(`lowcard_winner_${room}`, 'json') || {};
          await this.broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }], env);
          ws.send(JSON.stringify(["sendWinnersResult", { success: true, message: "Winners updated" }]));
        }
        break;
      }

      case "startGameWithRecording": {
        const [_, room, bet, username] = args;
        await this.handleGameStart(ws, bet, username, env, true);
        break;
      }

      default:
        ws.send(JSON.stringify(["error", `Unknown event: ${evt}`]));
        break;
    }
  }

  // ============================================================
  // GAME HANDLERS - PERSIS KAYA ASLI
  // ============================================================

  // ========== HANDLE GAME START ==========
  async handleGameStart(ws, bet, username, env, withRecording = false) {
    const room = ws.room || "LowCard";

    const existing = await env.DB.prepare(
      "SELECT is_active FROM games WHERE room = ?"
    ).bind(room).first();

    if (existing?.is_active === 1) {
      ws.send(JSON.stringify(["gameLowCardError", "Game already running"]));
      return;
    }

    const betAmount = parseInt(bet) || 0;
    if (betAmount < 0 || betAmount > 100000) {
      ws.send(JSON.stringify(["gameLowCardError", "Invalid bet"]));
      return;
    }

    const isRecording = await env.QUESTIONS.get(`lowcard_recording_status_${room}`);
    if (isRecording === 'true' && !withRecording) {
      ws.send(JSON.stringify(["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]));
      return;
    }

    await env.DB.prepare(
      `INSERT OR REPLACE INTO games 
        (room, is_active, phase, round, bet, host, registration_open, created_at) 
        VALUES (?, 1, 'registration', 1, ?, ?, 1, ?)`
    ).bind(room, betAmount, username, Date.now()).run();

    await env.DB.prepare(
      "INSERT OR REPLACE INTO game_players (game_room, username, eliminated) VALUES (?, ?, 0)"
    ).bind(room, username).run();

    await this.broadcastToRoom(room, ["gameLowCardStart", betAmount], env);
    await this.broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount], env);

    // ✅ START REGISTRATION 20 DETIK
    setTimeout(async () => {
      await this.handleCloseRegistration(room, env);
    }, 20000);
  }

  // ========== HANDLE GAME JOIN ==========
  async handleGameJoin(ws, username, env) {
    const room = ws.room || "LowCard";

    const game = await env.DB.prepare(
      "SELECT is_active, registration_open, bet FROM games WHERE room = ?"
    ).bind(room).first();

    if (!game || game.is_active !== 1) {
      ws.send(JSON.stringify(["gameLowCardError", "No active game"]));
      return;
    }

    if (game.registration_open !== 1) {
      ws.send(JSON.stringify(["gameLowCardNoJoin", username, game.bet || 0]));
      ws.send(JSON.stringify(["gameLowCardError", "Registration is closed"]));
      return;
    }

    const players = await env.DB.prepare(
      "SELECT COUNT(*) as count FROM game_players WHERE game_room = ?"
    ).bind(room).first();

    if ((players?.count || 0) >= 45) {
      ws.send(JSON.stringify(["gameLowCardError", "Game is full"]));
      return;
    }

    await env.DB.prepare(
      "INSERT OR REPLACE INTO game_players (game_room, username, eliminated) VALUES (?, ?, 0)"
    ).bind(room, username).run();

    await this.broadcastToRoom(room, ["gameLowCardJoin", username, game.bet || 0], env);
  }

  // ========== HANDLE GAME NUMBER ==========
  async handleGameNumber(ws, number, tanda, username, env) {
    const room = ws.room || "LowCard";

    const game = await env.DB.prepare(
      "SELECT is_active, phase, round FROM games WHERE room = ?"
    ).bind(room).first();

    if (!game || game.is_active !== 1 || game.phase !== 'draw') {
      ws.send(JSON.stringify(["gameLowCardError", "Cannot submit now"]));
      return;
    }

    const n = parseInt(number);
    if (isNaN(n) || n < 1 || n > 12) {
      ws.send(JSON.stringify(["gameLowCardError", "Invalid number 1-12"]));
      return;
    }

    const player = await env.DB.prepare(
      "SELECT eliminated FROM game_players WHERE game_room = ? AND username = ?"
    ).bind(room, username).first();

    if (player?.eliminated === 1) {
      ws.send(JSON.stringify(["gameLowCardError", "You have been eliminated"]));
      return;
    }

    await env.DB.prepare(
      "UPDATE game_players SET number_drawn = ?, tanda = ? WHERE game_room = ? AND username = ?"
    ).bind(n, tanda || "", room, username).run();

    await this.broadcastToRoom(room, ["gameLowCardPlayerDraw", username, n, tanda || ""], env);

    const players = await env.DB.prepare(
      "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0"
    ).bind(room).all();

    const submitted = await env.DB.prepare(
      "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0 AND number_drawn > 0"
    ).bind(room).all();

    if ((players.results || []).length === (submitted.results || []).length) {
      await this.broadcastToRoom(room, ["gameLowCardWait", "wait results"], env);
      setTimeout(async () => {
        await this.handleEvaluateGame(room, env);
      }, 2000);
    }
  }

  // ========== HANDLE GAME LEAVE ==========
  async handleGameLeave(ws, username, env) {
    const room = ws.room || "LowCard";

    await env.DB.prepare(
      "UPDATE game_players SET eliminated = 1 WHERE game_room = ? AND username = ?"
    ).bind(room, username).run();

    await this.broadcastToRoom(room, ["gameLowCardError", `${username} has been eliminated`], env);
  }

  // ========== HANDLE CHECK GAME ==========
  async handleCheckGame(ws, room, env) {
    const game = await env.DB.prepare(
      "SELECT is_active FROM games WHERE room = ?"
    ).bind(room).first();

    ws.send(JSON.stringify(["gameStatus", game?.is_active === 1 ? "true" : "false"]));
    if (game?.is_active === 1) {
      await this.handleGetGameState(ws, room, env);
    }
  }

  // ========== HANDLE GET GAME STATE ==========
  async handleGetGameState(ws, room, env) {
    const game = await env.DB.prepare(
      "SELECT * FROM games WHERE room = ?"
    ).bind(room).first();

    if (!game || game.is_active !== 1) {
      ws.send(JSON.stringify(["gameState", { room, hasGame: false }]));
      return;
    }

    const players = await env.DB.prepare(
      "SELECT username, eliminated, number_drawn FROM game_players WHERE game_room = ?"
    ).bind(room).all();

    const allPlayers = (players.results || []).map(p => p.username);
    const activePlayers = (players.results || []).filter(p => p.eliminated === 0).map(p => p.username);
    const submitted = (players.results || []).filter(p => p.number_drawn > 0).map(p => p.username);

    ws.send(JSON.stringify(["gameState", {
      room,
      hasGame: true,
      gameType: 'lowcard',
      isActive: game.is_active === 1,
      phase: game.phase || 'registration',
      round: game.round || 1,
      bet: game.bet || 0,
      host: game.host || 'Unknown',
      registrationOpen: game.registration_open === 1,
      players: allPlayers,
      activePlayers: activePlayers,
      submitted: submitted,
      playerCount: allPlayers.length,
      activeCount: activePlayers.length,
      isEvaluating: game.phase === 'evaluating',
      evaluationLocked: game.phase === 'evaluating',
      drawTimeExpired: game.phase === 'evaluating'
    }]));
  }

  // ========== HANDLE CLOSE REGISTRATION ==========
  async handleCloseRegistration(room, env) {
    const game = await env.DB.prepare(
      "SELECT is_active FROM games WHERE room = ?"
    ).bind(room).first();

    if (!game || game.is_active !== 1) return;

    await env.DB.prepare(
      "UPDATE games SET registration_open = 0, phase = 'draw' WHERE room = ?"
    ).bind(room).run();

    const players = await env.DB.prepare(
      "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0"
    ).bind(room).all();

    // Tambah bot jika kurang dari 2
    if ((players.results || []).length < 2) {
      const botNames = ["moz1", "moz2", "moz3", "moz4"];
      for (let i = 0; i < Math.min(4, 4 - (players.results || []).length); i++) {
        const botName = botNames[i % botNames.length];
        await env.DB.prepare(
          "INSERT OR REPLACE INTO game_players (game_room, username, eliminated) VALUES (?, ?, 0)"
        ).bind(room, `BOT_${botName}_${Date.now()}`).run();
      }
    }

    const playerList = (players.results || []).map(p => p.username);
    await this.broadcastToRoom(room, ["gameLowCardClosed", playerList], env);

    // ✅ START DRAW PHASE 20 DETIK
    setTimeout(async () => {
      await this.handleCloseDrawPhase(room, env);
    }, 20000);
  }

  // ========== HANDLE CLOSE DRAW PHASE ==========
  async handleCloseDrawPhase(room, env) {
    const game = await env.DB.prepare(
      "SELECT is_active, phase FROM games WHERE room = ?"
    ).bind(room).first();

    if (!game || game.is_active !== 1 || game.phase !== 'draw') return;

    await env.DB.prepare(
      "UPDATE games SET phase = 'evaluating' WHERE room = ?"
    ).bind(room).run();

    await this.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"], env);

    // ✅ EVALUATE GAME
    await this.handleEvaluateGame(room, env);
  }

  // ========== HANDLE EVALUATE GAME ==========
  async handleEvaluateGame(room, env) {
    const game = await env.DB.prepare(
      "SELECT bet, round FROM games WHERE room = ?"
    ).bind(room).first();

    if (!game) return;

    const players = await env.DB.prepare(
      "SELECT username, number_drawn, tanda FROM game_players WHERE game_room = ? AND eliminated = 0 AND number_drawn > 0"
    ).bind(room).all();

    if ((players.results || []).length === 0) {
      await this.broadcastToRoom(room, ["gameLowCardError", "No players submitted"], env);
      await env.DB.prepare("UPDATE games SET is_active = 0 WHERE room = ?").bind(room).run();
      return;
    }

    let lowest = Infinity;
    let losers = [];
    let allSame = true;
    let firstValue = players.results[0]?.number_drawn;

    for (const p of players.results || []) {
      if (p.number_drawn < lowest) {
        lowest = p.number_drawn;
        losers = [p.username];
      } else if (p.number_drawn === lowest) {
        losers.push(p.username);
      }
      if (p.number_drawn !== firstValue) allSame = false;
    }

    if (allSame && (players.results || []).length > 1) {
      const allPlayers = (players.results || []).map(p => p.username);
      await this.broadcastToRoom(room, ["gameLowCardRoundResult", game.round, [], [], allPlayers, true], env);

      await env.DB.prepare(
        "UPDATE games SET round = round + 1, phase = 'draw' WHERE room = ?"
      ).bind(room).run();

      await env.DB.prepare(
        "UPDATE game_players SET number_drawn = 0, tanda = '' WHERE game_room = ?"
      ).bind(room).run();

      setTimeout(async () => {
        const currentGame = await env.DB.prepare(
          "SELECT is_active FROM games WHERE room = ?"
        ).bind(room).first();
        if (currentGame?.is_active === 1) {
          await this.handleCloseDrawPhase(room, env);
        }
      }, 20000);
      return;
    }

    for (const loser of losers) {
      await env.DB.prepare(
        "UPDATE game_players SET eliminated = 1 WHERE game_room = ? AND username = ?"
      ).bind(room, loser).run();
    }

    const remaining = await env.DB.prepare(
      "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0"
    ).bind(room).all();

    if ((remaining.results || []).length === 1) {
      const winner = remaining.results[0].username;
      const totalCoin = (game.bet || 0) * (players.results || []).length;

      const isRecording = await env.QUESTIONS.get(`lowcard_recording_status_${room}`);
      if (isRecording === 'true') {
        const winners = await env.QUESTIONS.get(`lowcard_winner_${room}`, 'json') || {};
        winners[winner] = (winners[winner] || 0) + 1;
        await env.QUESTIONS.put(`lowcard_winner_${room}`, JSON.stringify(winners));
        await this.broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }], env);
      }

      await this.broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin], env);
      await env.DB.prepare("UPDATE games SET is_active = 0 WHERE room = ?").bind(room).run();
      return;
    }

    const currentRound = await env.DB.prepare(
      "SELECT round FROM games WHERE room = ?"
    ).bind(room).first();

    await this.broadcastToRoom(room, [
      "gameLowCardRoundResult",
      currentRound?.round || 1,
      (players.results || []).map(p => `${p.username}:${p.number_drawn}${p.tanda ? `(${p.tanda})` : ''}`),
      losers,
      (remaining.results || []).map(p => p.username)
    ], env);

    await env.DB.prepare(
      "UPDATE games SET round = round + 1, phase = 'draw' WHERE room = ?"
    ).bind(room).run();

    await env.DB.prepare(
      "UPDATE game_players SET number_drawn = 0, tanda = '' WHERE game_room = ?"
    ).bind(room).run();

    setTimeout(async () => {
      const currentGame = await env.DB.prepare(
        "SELECT is_active FROM games WHERE room = ?"
      ).bind(room).first();
      if (currentGame?.is_active === 1) {
        await this.handleCloseDrawPhase(room, env);
      }
    }, 20000);
  }

  // ========== HANDLE DICE ANSWER ==========
  async handleDiceAnswer(ws, username, guess, env) {
    const dice = await env.QUESTIONS.get('dice_current', 'json') || {};

    if (!dice.active) {
      ws.send(JSON.stringify(["diceError", "No active dice"]));
      return;
    }

    const guessValue = parseInt(guess);
    if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
      ws.send(JSON.stringify(["diceError", "Invalid guess 1-6"]));
      return;
    }

    const answered = await env.QUESTIONS.get(`dice_answered_${dice.round}`, 'json') || [];
    if (answered.includes(username)) {
      ws.send(JSON.stringify(["diceError", "Already answered"]));
      return;
    }

    answered.push(username);
    await env.QUESTIONS.put(`dice_answered_${dice.round}`, JSON.stringify(answered));

    const guesses = await env.QUESTIONS.get(`dice_guesses_${dice.round}`, 'json') || {};
    guesses[username] = guessValue;
    await env.QUESTIONS.put(`dice_guesses_${dice.round}`, JSON.stringify(guesses));

    await this.broadcastToRoom(DICE_ROOM, ["diceAnswer", { username, guess: guessValue }], env);

    if (guessValue === dice.value && !dice.hasWinner) {
      dice.hasWinner = true;
      dice.winner = username;
      await env.QUESTIONS.put('dice_current', JSON.stringify(dice));

      const points = await env.QUESTIONS.get('dice_points', 'json') || {};
      points[username] = (points[username] || 0) + 1;
      await env.QUESTIONS.put('dice_points', JSON.stringify(points));

      await this.broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: username,
        totalPoints: points[username],
        diceValue: dice.value,
        round: dice.round
      }], env);
    }
  }

  // ========== BROADCAST TO ROOM ==========
  async broadcastToRoom(room, message, env) {
    const msgStr = JSON.stringify(message);
    const users = await env.DB.prepare(
      "SELECT ws_id FROM users WHERE room = ? AND active = 1"
    ).bind(room).all();

    for (const user of users.results || []) {
      const conn = wsConnections.get(user.ws_id);
      if (conn && conn.ws && conn.ws.readyState === 1) {
        try {
          conn.ws.send(msgStr);
        } catch(e) {}
      }
    }
  }
}
