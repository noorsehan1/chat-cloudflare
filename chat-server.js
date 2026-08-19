// ==================== INDEX.JS ====================
// VERSION: 4.0.0 - WEBSOCKET + D1
// OUTPUT PERSIS SEPERTI CHAT-SERVER.JS + GAME-SERVER.JS

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  BATCH_SIZE: 20,
  LOCK_TIMEOUT: 10000,
  MAX_EVENT_QUEUE: 100,
  MAX_PROCESS_TIME_MS: 500,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const DICE_ROOM = "Quiz";

// ==================== WEBSOCKET STORAGE ====================
const wsConnections = new Map(); // wsId → { ws, username, room }

// ==================== MAIN WORKER ====================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // ============================================================
    // CHAT SERVER - /ws (PERSIS SEPERTI CHAT-SERVER.JS)
    // ============================================================
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server", {
          status: 200,
          headers: { "Cache-Control": "no-cache" }
        });
      }

      const username = url.searchParams.get("username") || "Anonymous";
      const room = url.searchParams.get("room") || "General";

      if (wsConnections.size >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }

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

      // ✅ SIMPAN KE D1
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

      // ✅ BROADCAST USER JOIN
      await broadcastToRoom(room, ["roomUserCount", room, await getRoomCount(room, env)], env);
      await broadcastUserList(room, env);

      // ✅ WEBSOCKET MESSAGE HANDLER
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

          // ✅ VALIDASI ROOM
          if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
            const room = args[0];
            if (room && !ROOMS_SET.has(room)) return;
          }

          // ============================================================
          // CHAT EVENTS (PERSIS SEPERTI CHAT-SERVER.JS)
          // ============================================================
          switch(evt) {
            case "setIdTarget2": {
              const [username, isNewUser] = args;
              await handleSetId(server, username, isNewUser, env);
              break;
            }

            case "joinRoom": {
              const [roomName] = args;
              await handleJoin(server, roomName, env);
              break;
            }

            case "multiJoin": {
              const [multiUsername, multiRoomname] = args;
              await handleMultiJoin(server, multiUsername, multiRoomname, env);
              break;
            }

            case "exitMulti": {
              const [targetUsername] = args;
              await handleExitMulti(server, targetUsername, env);
              break;
            }

            case "setActiveMulti": {
              const [targetUsername] = args;
              await handleSetActiveMulti(server, targetUsername, env);
              break;
            }

            case "updateKursi": {
              const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
              await handleUpdateKursi(kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt, env);
              break;
            }

            case "chat": {
              const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
              if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;

              await env.DB.prepare(
                "INSERT INTO messages (room, username, message, timestamp) VALUES (?, ?, ?, ?)"
              ).bind(chatRoom, chatUser, chatMsg, Date.now()).run();

              await broadcastToRoom(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor], env);
              break;
            }

            case "updatePoint": {
              const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
              if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
                await env.DB.prepare(
                  "INSERT OR REPLACE INTO points (room, seat_number, x, y, fast) VALUES (?, ?, ?, ?, ?)"
                ).bind(pointRoom, pointSeat, pointX || 0, pointY || 0, pointFast || 0).run();

                await broadcastToRoom(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast], env);
              }
              break;
            }

            case "removeKursiAndPoint": {
              const [removeRoom, removeSeat] = args;
              await env.DB.prepare(
                "DELETE FROM seats WHERE room = ? AND seat_number = ?"
              ).bind(removeRoom, removeSeat).run();
              await env.DB.prepare(
                "DELETE FROM points WHERE room = ? AND seat_number = ?"
              ).bind(removeRoom, removeSeat).run();

              await broadcastToRoom(removeRoom, ["removeKursi", removeRoom, removeSeat], env);
              await broadcastToRoom(removeRoom, ["roomUserCount", removeRoom, await getRoomCount(removeRoom, env)], env);
              break;
            }

            case "private": {
              const [privTarget, privNoimg, privMsg, privSender] = args;
              if (privTarget && privMsg) {
                const targetUser = await env.DB.prepare(
                  "SELECT ws_id FROM users WHERE username = ? AND active = 1"
                ).bind(privTarget).first();

                if (targetUser) {
                  await sendToUser(targetUser.ws_id, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender], env);
                }
                server.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
              }
              break;
            }

            case "gift": {
              const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
              if (giftRoom && ROOMS_SET.has(giftRoom)) {
                await broadcastToRoom(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()], env);
              }
              break;
            }

            case "rollangak": {
              const [rollRoom, rollUser, rollAngka] = args;
              if (rollRoom && ROOMS_SET.has(rollRoom)) {
                await broadcastToRoom(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka], env);
              }
              break;
            }

            case "sendnotif": {
              const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
              if (notifTarget && notifMsg) {
                const targetUser = await env.DB.prepare(
                  "SELECT ws_id FROM users WHERE username = ? AND active = 1"
                ).bind(notifTarget).first();

                if (targetUser) {
                  await sendToUser(targetUser.ws_id, ["notif", notifNoimg, notifUser, notifMsg, Date.now()], env);
                }
              }
              break;
            }

            case "getCurrentNumber": {
              const roomSettings = await env.DB.prepare(
                "SELECT current_number FROM room_settings WHERE room = ?"
              ).bind("General").first();
              server.send(JSON.stringify(["currentNumber", roomSettings?.current_number || 1]));
              break;
            }

            case "isUserOnline": {
              const [onlineTarget, onlineCallback] = args;
              const result = await env.DB.prepare(
                "SELECT active FROM users WHERE username = ?"
              ).bind(onlineTarget).first();
              const isOnline = result?.active === 1;
              server.send(JSON.stringify(["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]));
              break;
            }

            case "getOnlineUsers": {
              const users = await env.DB.prepare(
                "SELECT username FROM users WHERE active = 1"
              ).all();
              const userList = (users.results || []).map(u => u.username);
              server.send(JSON.stringify(["allOnlineUsers", userList]));
              break;
            }

            case "getAllRoomsUserCount": {
              const counts = {};
              for (const room of ROOMS) {
                counts[room] = await getRoomCount(room, env);
              }
              server.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)]));
              break;
            }

            case "getRoomUserCount": {
              const [roomName] = args;
              if (roomName && ROOMS_SET.has(roomName)) {
                const count = await getRoomCount(roomName, env);
                server.send(JSON.stringify(["roomUserCount", roomName, count]));
              }
              break;
            }

            case "setMuteType": {
              const [muteVal, muteRoom] = args;
              if (muteRoom && ROOMS_SET.has(muteRoom)) {
                await env.DB.prepare(
                  "INSERT OR REPLACE INTO room_settings (room, muted) VALUES (?, ?)"
                ).bind(muteRoom, muteVal ? 1 : 0).run();

                await broadcastToRoom(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom], env);
                server.send(JSON.stringify(["muteTypeSet", !!muteVal, true, muteRoom]));
              }
              break;
            }

            case "getMuteType": {
              const [muteRoom] = args;
              if (muteRoom && ROOMS_SET.has(muteRoom)) {
                const result = await env.DB.prepare(
                  "SELECT muted FROM room_settings WHERE room = ?"
                ).bind(muteRoom).first();
                server.send(JSON.stringify(["muteTypeResponse", result?.muted === 1, muteRoom]));
              }
              break;
            }

            case "modwarning": {
              const [modRoom] = args;
              if (modRoom && ROOMS_SET.has(modRoom)) {
                await broadcastToRoom(modRoom, ["modwarning", modRoom], env);
              }
              break;
            }

            case "onDestroy": {
              // Cleanup
              const conn = wsConnections.get(wsId);
              if (conn) {
                wsConnections.delete(wsId);
                await env.DB.prepare(
                  "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
                ).bind(Date.now(), wsId).run();
              }
              break;
            }

            // ============================================================
            // GAME EVENTS (PERSIS SEPERTI GAME-SERVER.JS)
            // ============================================================
            case "switchRoom": {
              const [room, username] = args;
              await handleSwitchRoom(server, room, username, env);
              break;
            }

            case "gameLowCardStart": {
              const [bet, username] = args;
              await handleGameStart(server, bet, username, env);
              break;
            }

            case "gameLowCardJoin": {
              const [username] = args;
              await handleGameJoin(server, username, env);
              break;
            }

            case "gameLowCardNumber": {
              const [number, tanda, username] = args;
              await handleGameNumber(server, number, tanda, username, env);
              break;
            }

            case "gameLowCardLeave": {
              const [username] = args;
              await handleGameLeave(server, username, env);
              break;
            }

            case "checkGameRunning": {
              const [room] = args;
              await handleCheckGame(server, room, env);
              break;
            }

            case "getGameState": {
              const [room] = args;
              await handleGetGameState(server, room, env);
              break;
            }

            case "submitDiceAnswer": {
              const [username, guess] = args;
              await handleDiceAnswer(server, username, guess, env);
              break;
            }

            case "getDiceLastWeekWinner": {
              const winner = await env.QUESTIONS.get('dice_last_week_winner', 'json');
              if (winner) {
                server.send(JSON.stringify(["diceLastWeekWinner", winner.username, winner.score || 0, winner.week || ""]));
              } else {
                server.send(JSON.stringify(["diceLastWeekWinner", "", 0, ""]));
              }
              break;
            }

            case "getDiceLeaderboard": {
              const points = await env.QUESTIONS.get('dice_points', 'json') || {};
              const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]).slice(0, 10);
              server.send(JSON.stringify(["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]));
              break;
            }

            case "deleteDiceLastWeekWinner": {
              await env.QUESTIONS.delete('dice_last_week_winner');
              server.send(JSON.stringify(["diceLastWeekWinnerDeleted", true, "Deleted"]));
              await broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner deleted"], env);
              break;
            }

            case "getDiceStatus": {
              const dice = await env.QUESTIONS.get('dice_current', 'json') || {};
              server.send(JSON.stringify(["diceStatus", !!dice.active, dice.round || 1]));
              break;
            }

            case "startRecordingWinners": {
              const [roomName] = args;
              if (roomName) {
                await env.QUESTIONS.put(`lowcard_recording_status_${roomName}`, 'true');
                await broadcastToRoom(roomName, ["recordingStatus", true], env);
                server.send(JSON.stringify(["startRecordingResult", { success: true, message: "Recording enabled" }]));
              }
              break;
            }

            case "stopRecordingWinners": {
              const [roomName] = args;
              if (roomName) {
                await env.QUESTIONS.delete(`lowcard_recording_status_${roomName}`);
                await env.QUESTIONS.delete(`lowcard_winner_${roomName}`);
                await broadcastToRoom(roomName, ["recordingStatus", false], env);
                server.send(JSON.stringify(["stopRecordingResult", { success: true, message: "Recording stopped" }]));
              }
              break;
            }

            case "getRecordingStatus": {
              const [roomName] = args;
              if (roomName) {
                const status = await env.QUESTIONS.get(`lowcard_recording_status_${roomName}`);
                server.send(JSON.stringify(["recordingStatus", status === 'true']));
              }
              break;
            }

            case "sendWinnersToRoom": {
              const [room] = args;
              if (room) {
                const winners = await env.QUESTIONS.get(`lowcard_winner_${room}`, 'json') || {};
                await broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }], env);
                server.send(JSON.stringify(["sendWinnersResult", { success: true, message: "Winners refreshed" }]));
              }
              break;
            }

            case "getRoomWinners": {
              const [room] = args;
              if (room) {
                const winners = await env.QUESTIONS.get(`lowcard_winner_${room}`, 'json') || {};
                await broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }], env);
                server.send(JSON.stringify(["sendWinnersResult", { success: true, message: "Winners updated" }]));
              }
              break;
            }

            case "startGameWithRecording": {
              const [_, room, bet, username] = args;
              await handleGameStart(server, bet, username, env, true);
              break;
            }

            default:
              server.send(JSON.stringify(["error", `Unknown event: ${evt}`]));
              break;
          }
        } catch(e) {
          console.error("Message error:", e);
        }
      });

      // ✅ WEBSOCKET CLOSE
      server.addEventListener("close", async () => {
        const conn = wsConnections.get(wsId);
        if (conn) {
          const { username, room } = conn;

          await env.DB.prepare(
            "DELETE FROM seats WHERE room = ? AND username = ?"
          ).bind(room, username).run();

          await env.DB.prepare(
            "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
          ).bind(Date.now(), wsId).run();

          const seat = await getSeatNumber(username, room, env);
          if (seat) {
            await broadcastToRoom(room, ["removeKursi", room, seat], env);
          }
          await broadcastToRoom(room, ["roomUserCount", room, await getRoomCount(room, env)], env);
          await broadcastUserList(room, env);
        }
        wsConnections.delete(wsId);
      });

      server.addEventListener("error", async () => {
        wsConnections.delete(wsId);
        await env.DB.prepare(
          "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
        ).bind(Date.now(), wsId).run();
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // ============================================================
    // GAME SERVER - /game (PERSIS SEPERTI GAME-SERVER.JS)
    // ============================================================
    if (pathname === "/game/ws" || pathname === "/game") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Game Server", { status: 200 });
      }

      const room = url.searchParams.get("room") || "LowCard";
      const username = url.searchParams.get("username") || "Anonymous";

      const wsUrl = `/ws?username=${encodeURIComponent(username)}&room=${encodeURIComponent(room)}`;
      return new Response(null, {
        status: 307,
        headers: { "Location": wsUrl }
      });
    }

    // ============================================================
    // HEALTH CHECK
    // ============================================================
    if (pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: wsConnections.size,
        rooms: ROOMS,
        timestamp: Date.now()
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // ============================================================
    // FRONTEND
    // ============================================================
    if (pathname === "/" || pathname === "/chat") {
      return new Response(HTML, {
        headers: { 'Content-Type': 'text/html' }
      });
    }

    return new Response("Server running", { status: 200 });
  }
};

// ============================================================
// HANDLER FUNCTIONS (PERSIS SEPERTI CHAT-SERVER.JS & GAME-SERVER.JS)
// ============================================================

// ========== HANDLE SET ID ==========
async function handleSetId(ws, username, isNewUser, env) {
  if (!username) return;

  const existing = await env.DB.prepare(
    "SELECT username FROM users WHERE username = ?"
  ).bind(username).first();

  if (!existing) {
    await env.DB.prepare(
      "INSERT INTO users (username, active, last_active) VALUES (?, 1, ?)"
    ).bind(username, Date.now()).run();
  }

  ws.send(JSON.stringify(isNewUser ? ["joinroomawal"] : ["needJoinRoom"]));
}

// ========== HANDLE JOIN ==========
async function handleJoin(ws, roomName, env) {
  if (!roomName || !ROOMS_SET.has(roomName)) return;

  const username = ws.username || "Anonymous";

  await env.DB.prepare(
    "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
  ).bind(roomName, Date.now(), username).run();

  let seat = await getSeatNumber(username, roomName, env);
  if (!seat) {
    seat = await getAvailableSeat(roomName, env);
    if (seat) {
      await env.DB.prepare(
        "INSERT INTO seats (room, seat_number, username) VALUES (?, ?, ?)"
      ).bind(roomName, seat, username).run();
    }
  }

  if (seat) {
    ws.send(JSON.stringify(["rooMasuk", seat, roomName]));
    ws.send(JSON.stringify(["numberKursiSaya", seat]));

    const muted = await env.DB.prepare(
      "SELECT muted FROM room_settings WHERE room = ?"
    ).bind(roomName).first();
    ws.send(JSON.stringify(["muteTypeResponse", muted?.muted === 1, roomName]));

    await broadcastToRoom(roomName, ["roomUserCount", roomName, await getRoomCount(roomName, env)], env);
    await broadcastAllSeats(roomName, env);

    setTimeout(async () => {
      await sendAllStateTo(ws, roomName, env);
    }, 1000);
  }
}

// ========== HANDLE MULTI JOIN ==========
async function handleMultiJoin(ws, multiUsername, multiRoomname, env) {
  if (!multiUsername || !multiRoomname) return;

  // Hapus dari room lama
  await env.DB.prepare(
    "DELETE FROM seats WHERE username = ?"
  ).bind(multiUsername).run();

  const seat = await getAvailableSeat(multiRoomname, env);
  if (!seat) {
    ws.send(JSON.stringify(["roomFull", multiRoomname]));
    return;
  }

  await env.DB.prepare(
    "INSERT INTO seats (room, seat_number, username) VALUES (?, ?, ?)"
  ).bind(multiRoomname, seat, multiUsername).run();

  await env.DB.prepare(
    "UPDATE users SET room = ?, active = 1, last_active = ? WHERE username = ?"
  ).bind(multiRoomname, Date.now(), multiUsername).run();

  ws.send(JSON.stringify(["rooMasukMulti", seat, multiRoomname]));
  await broadcastToRoom(multiRoomname, ["roomUserCount", multiRoomname, await getRoomCount(multiRoomname, env)], env);
}

// ========== HANDLE EXIT MULTI ==========
async function handleExitMulti(ws, targetUsername, env) {
  if (!targetUsername) return;

  const seat = await getSeatNumber(targetUsername, null, env);
  if (seat) {
    await env.DB.prepare(
      "DELETE FROM seats WHERE username = ?"
    ).bind(targetUsername).run();

    const room = await env.DB.prepare(
      "SELECT room FROM users WHERE username = ?"
    ).bind(targetUsername).first();

    if (room) {
      await broadcastToRoom(room.room, ["removeKursi", room.room, seat], env);
      await broadcastToRoom(room.room, ["roomUserCount", room.room, await getRoomCount(room.room, env)], env);
    }
  }

  await env.DB.prepare(
    "UPDATE users SET active = 0, last_active = ? WHERE username = ?"
  ).bind(Date.now(), targetUsername).run();
}

// ========== HANDLE SET ACTIVE MULTI ==========
async function handleSetActiveMulti(ws, targetUsername, env) {
  if (!targetUsername) return;

  const seat = await getSeatNumber(targetUsername, null, env);
  const room = await env.DB.prepare(
    "SELECT room FROM users WHERE username = ?"
  ).bind(targetUsername).first();

  if (seat && room) {
    ws.send(JSON.stringify(["activeChangedMulti", targetUsername, seat, room.room]));
    await broadcastToRoom(room.room, ["userActiveChanged", targetUsername, seat], env);
  }
}

// ========== HANDLE UPDATE KURSI ==========
async function handleUpdateKursi(kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt, env) {
  if (!kursiRoom || !kursiSeat) return;

  await env.DB.prepare(
    `UPDATE seats SET 
      noimageUrl = ?, 
      color = ?, 
      itembawah = ?, 
      itematas = ?, 
      vip = ?, 
      viptanda = ? 
    WHERE room = ? AND seat_number = ?`
  ).bind(
    kursiNoimg || "",
    kursiColor || "",
    kursiBawah || 0,
    kursiAtas || 0,
    kursiVip || 0,
    kursiVt || 0,
    kursiRoom,
    kursiSeat
  ).run();

  const updated = await env.DB.prepare(
    "SELECT * FROM seats WHERE room = ? AND seat_number = ?"
  ).bind(kursiRoom, kursiSeat).first();

  if (updated) {
    await broadcastToRoom(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updated]]], env);
  }
}

// ========== HANDLE SWITCH ROOM (GAME) ==========
async function handleSwitchRoom(ws, room, username, env) {
  if (!room || !ROOMS_SET.has(room)) {
    ws.send(JSON.stringify(["gameLowCardError", "Invalid room"]));
    return;
  }

  await env.DB.prepare(
    "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
  ).bind(room, Date.now(), username).run();

  ws.send(JSON.stringify(["switchRoomSuccess", room]));
  await handleGetGameState(ws, room, env);
}

// ========== HANDLE GAME START ==========
async function handleGameStart(ws, bet, username, env, withRecording = false) {
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

  // Cek recording
  const isRecording = await env.QUESTIONS.get(`lowcard_recording_status_${room}`);
  if (isRecording === 'true' && !withRecording) {
    ws.send(JSON.stringify(["gameLowCardError", "Recording is ACTIVE. Users cannot start games."]));
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

  await broadcastToRoom(room, ["gameLowCardStart", betAmount], env);
  await broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount], env);

  // ✅ START REGISTRATION (20 detik)
  setTimeout(async () => {
    await handleCloseRegistration(room, env);
  }, 20000);
}

// ========== HANDLE GAME JOIN ==========
async function handleGameJoin(ws, username, env) {
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

  await broadcastToRoom(room, ["gameLowCardJoin", username, game.bet || 0], env);
}

// ========== HANDLE GAME NUMBER ==========
async function handleGameNumber(ws, number, tanda, username, env) {
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

  await broadcastToRoom(room, ["gameLowCardPlayerDraw", username, n, tanda || ""], env);

  // ✅ CEK SEMUA PLAYER SUDAH SUBMIT
  const players = await env.DB.prepare(
    "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0"
  ).bind(room).all();

  const submitted = await env.DB.prepare(
    "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0 AND number_drawn > 0"
  ).bind(room).all();

  if ((players.results || []).length === (submitted.results || []).length) {
    await broadcastToRoom(room, ["gameLowCardWait", "wait results"], env);
    setTimeout(async () => {
      await handleEvaluateGame(room, env);
    }, 2000);
  }
}

// ========== HANDLE GAME LEAVE ==========
async function handleGameLeave(ws, username, env) {
  const room = ws.room || "LowCard";

  await env.DB.prepare(
    "UPDATE game_players SET eliminated = 1 WHERE game_room = ? AND username = ?"
  ).bind(room, username).run();

  await broadcastToRoom(room, ["gameLowCardError", `${username} has been eliminated`], env);
}

// ========== HANDLE CHECK GAME ==========
async function handleCheckGame(ws, room, env) {
  const game = await env.DB.prepare(
    "SELECT is_active FROM games WHERE room = ?"
  ).bind(room).first();

  ws.send(JSON.stringify(["gameStatus", game?.is_active === 1 ? "true" : "false"]));
  if (game?.is_active === 1) {
    await handleGetGameState(ws, room, env);
  }
}

// ========== HANDLE GET GAME STATE ==========
async function handleGetGameState(ws, room, env) {
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
async function handleCloseRegistration(room, env) {
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
  await broadcastToRoom(room, ["gameLowCardClosed", playerList], env);

  // ✅ START DRAW PHASE (20 detik)
  setTimeout(async () => {
    await handleCloseDrawPhase(room, env);
  }, 20000);
}

// ========== HANDLE CLOSE DRAW PHASE ==========
async function handleCloseDrawPhase(room, env) {
  const game = await env.DB.prepare(
    "SELECT is_active, phase FROM games WHERE room = ?"
  ).bind(room).first();

  if (!game || game.is_active !== 1 || game.phase !== 'draw') return;

  await env.DB.prepare(
    "UPDATE games SET phase = 'evaluating' WHERE room = ?"
  ).bind(room).run();

  await broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"], env);

  // ✅ EVALUATE GAME
  await handleEvaluateGame(room, env);
}

// ========== HANDLE EVALUATE GAME ==========
async function handleEvaluateGame(room, env) {
  const game = await env.DB.prepare(
    "SELECT bet, round FROM games WHERE room = ?"
  ).bind(room).first();

  if (!game) return;

  const players = await env.DB.prepare(
    "SELECT username, number_drawn, tanda FROM game_players WHERE game_room = ? AND eliminated = 0 AND number_drawn > 0"
  ).bind(room).all();

  if ((players.results || []).length === 0) {
    await broadcastToRoom(room, ["gameLowCardError", "No players submitted"], env);
    await env.DB.prepare("UPDATE games SET is_active = 0 WHERE room = ?").bind(room).run();
    return;
  }

  // ✅ CARI TERENDAH
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

  // ✅ SEMUA SAMA
  if (allSame && (players.results || []).length > 1) {
    const allPlayers = (players.results || []).map(p => p.username);
    await broadcastToRoom(room, ["gameLowCardRoundResult", game.round, [], [], allPlayers, true], env);

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
        await handleCloseDrawPhase(room, env);
      }
    }, 20000);
    return;
  }

  // ✅ ELIMINASI LOSERS
  for (const loser of losers) {
    await env.DB.prepare(
      "UPDATE game_players SET eliminated = 1 WHERE game_room = ? AND username = ?"
    ).bind(room, loser).run();
  }

  // ✅ CEK REMAINING
  const remaining = await env.DB.prepare(
    "SELECT username FROM game_players WHERE game_room = ? AND eliminated = 0"
  ).bind(room).all();

  if ((remaining.results || []).length === 1) {
    const winner = remaining.results[0].username;
    const totalCoin = (game.bet || 0) * (players.results || []).length;

    // ✅ SAVE WINNER KE KV
    const isRecording = await env.QUESTIONS.get(`lowcard_recording_status_${room}`);
    if (isRecording === 'true') {
      const winners = await env.QUESTIONS.get(`lowcard_winner_${room}`, 'json') || {};
      winners[winner] = (winners[winner] || 0) + 1;
      await env.QUESTIONS.put(`lowcard_winner_${room}`, JSON.stringify(winners));
      await broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }], env);
    }

    await broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin], env);
    await env.DB.prepare("UPDATE games SET is_active = 0 WHERE room = ?").bind(room).run();
    return;
  }

  // ✅ LANJUT ROUND
  const currentRound = await env.DB.prepare(
    "SELECT round FROM games WHERE room = ?"
  ).bind(room).first();

  await broadcastToRoom(room, [
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
      await handleCloseDrawPhase(room, env);
    }
  }, 20000);
}

// ========== HANDLE DICE ANSWER ==========
async function handleDiceAnswer(ws, username, guess, env) {
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

  // CEK SUDAH ANSWER
  const answered = await env.QUESTIONS.get(`dice_answered_${dice.round}`, 'json') || [];
  if (answered.includes(username)) {
    ws.send(JSON.stringify(["diceError", "Already answered"]));
    return;
  }

  // SIMPAN ANSWER
  answered.push(username);
  await env.QUESTIONS.put(`dice_answered_${dice.round}`, JSON.stringify(answered));

  // SIMPAN GUESS
  const guesses = await env.QUESTIONS.get(`dice_guesses_${dice.round}`, 'json') || {};
  guesses[username] = guessValue;
  await env.QUESTIONS.put(`dice_guesses_${dice.round}`, JSON.stringify(guesses));

  await broadcastToRoom(DICE_ROOM, ["diceAnswer", { username, guess: guessValue }], env);

  // CEK WINNER
  if (guessValue === dice.value && !dice.hasWinner) {
    dice.hasWinner = true;
    dice.winner = username;
    await env.QUESTIONS.put('dice_current', JSON.stringify(dice));

    const points = await env.QUESTIONS.get('dice_points', 'json') || {};
    points[username] = (points[username] || 0) + 1;
    await env.QUESTIONS.put('dice_points', JSON.stringify(points));

    await broadcastToRoom(DICE_ROOM, ["diceWinner", {
      username: username,
      totalPoints: points[username],
      diceValue: dice.value,
      round: dice.round
    }], env);
  }
}

// ============================================================
// HELPER FUNCTIONS
// ============================================================

// ========== GET ROOM COUNT ==========
async function getRoomCount(room, env) {
  const result = await env.DB.prepare(
    "SELECT COUNT(*) as count FROM users WHERE room = ? AND active = 1"
  ).bind(room).first();
  return result?.count || 0;
}

// ========== GET SEAT NUMBER ==========
async function getSeatNumber(username, room, env) {
  let query = "SELECT seat_number FROM seats WHERE username = ?";
  let params = [username];
  if (room) {
    query += " AND room = ?";
    params.push(room);
  }
  const result = await env.DB.prepare(query).bind(...params).first();
  return result?.seat_number || null;
}

// ========== GET AVAILABLE SEAT ==========
async function getAvailableSeat(room, env) {
  const seats = await env.DB.prepare(
    "SELECT seat_number FROM seats WHERE room = ? ORDER BY seat_number"
  ).bind(room).all();

  const taken = new Set((seats.results || []).map(s => s.seat_number));
  for (let i = 1; i <= C.MAX_SEATS; i++) {
    if (!taken.has(i)) return i;
  }
  return null;
}

// ========== BROADCAST TO ROOM ==========
async function broadcastToRoom(room, message, env) {
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

// ========== SEND TO USER ==========
async function sendToUser(wsId, message, env) {
  const conn = wsConnections.get(wsId);
  if (conn && conn.ws && conn.ws.readyState === 1) {
    try {
      conn.ws.send(JSON.stringify(message));
      return true;
    } catch(e) {}
  }
  return false;
}

// ========== BROADCAST USER LIST ==========
async function broadcastUserList(room, env) {
  const users = await env.DB.prepare(
    "SELECT username FROM users WHERE room = ? AND active = 1"
  ).bind(room).all();

  await broadcastToRoom(room, ["userList", (users.results || []).map(u => u.username)], env);
}

// ========== BROADCAST ALL SEATS ==========
async function broadcastAllSeats(room, env) {
  const seats = await env.DB.prepare(
    "SELECT seat_number, username, noimageUrl, color, itembawah, itematas, vip, viptanda FROM seats WHERE room = ?"
  ).bind(room).all();

  const seatData = {};
  for (const seat of seats.results || []) {
    seatData[seat.seat_number] = {
      namauser: seat.username,
      noimageUrl: seat.noimageUrl || "",
      color: seat.color || "",
      itembawah: seat.itembawah || 0,
      itematas: seat.itematas || 0,
      vip: seat.vip || 0,
      viptanda: seat.viptanda || 0
    };
  }

  await broadcastToRoom(room, ["allUpdateKursiList", room, seatData], env);
}

// ========== SEND ALL STATE TO CLIENT ==========
async function sendAllStateTo(ws, room, env) {
  await broadcastAllSeats(room, env);

  const points = await env.DB.prepare(
    "SELECT seat_number, x, y, fast FROM points WHERE room = ?"
  ).bind(room).all();

  const pointData = (points.results || []).map(p => ({
    seat: p.seat_number,
    x: p.x || 0,
    y: p.y || 0,
    fast: p.fast || 0
  }));

  if (pointData.length > 0) {
    ws.send(JSON.stringify(["allPointsList", room, pointData]));
  }

  await handleGetGameState(ws, room, env);
}
