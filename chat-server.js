// ==================== CHAT-SERVER.JS ====================
// VERSION: 4.0.0 - WEBSOCKET + D1 (TANPA DO)
// PISAH DARI GAME SERVER

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

// ==================== WEBSOCKET STORAGE ====================
const wsConnections = new Map();

export class ChatServer {
  constructor(env) {
    this.env = env;
    this.wsConnections = wsConnections;
    this.ROOMS = ROOMS;
    this.ROOMS_SET = ROOMS_SET;
    this.C = C;
  }

  // ========== FETCH ==========
  async fetch(request) {
    const url = new URL(request.url);
    const pathname = url.pathname;

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

      // ✅ BROADCAST USER JOIN
      await this.broadcastToRoom(room, ["roomUserCount", room, await this.getRoomCount(room)], env);
      await this.broadcastUserList(room, env);

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

          if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
            const room = args[0];
            if (room && !ROOMS_SET.has(room)) return;
          }

          await this.handleEvent(server, evt, args, env);
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

          const seat = await this.getSeatNumber(username, room, env);
          if (seat) {
            await this.broadcastToRoom(room, ["removeKursi", room, seat], env);
          }
          await this.broadcastToRoom(room, ["roomUserCount", room, await this.getRoomCount(room)], env);
          await this.broadcastUserList(room, env);
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

    return new Response("Chat Server", { status: 200 });
  }

  // ========== HANDLE EVENT ==========
  async handleEvent(ws, evt, args, env) {
    switch(evt) {
      case "setIdTarget2": {
        const [username, isNewUser] = args;
        await this.handleSetId(ws, username, isNewUser, env);
        break;
      }

      case "joinRoom": {
        const [roomName] = args;
        await this.handleJoin(ws, roomName, env);
        break;
      }

      case "multiJoin": {
        const [multiUsername, multiRoomname] = args;
        await this.handleMultiJoin(ws, multiUsername, multiRoomname, env);
        break;
      }

      case "exitMulti": {
        const [targetUsername] = args;
        await this.handleExitMulti(ws, targetUsername, env);
        break;
      }

      case "setActiveMulti": {
        const [targetUsername] = args;
        await this.handleSetActiveMulti(ws, targetUsername, env);
        break;
      }

      case "updateKursi": {
        const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
        await this.handleUpdateKursi(kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt, env);
        break;
      }

      case "chat": {
        const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
        if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;

        await env.DB.prepare(
          "INSERT INTO messages (room, username, message, timestamp) VALUES (?, ?, ?, ?)"
        ).bind(chatRoom, chatUser, chatMsg, Date.now()).run();

        await this.broadcastToRoom(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor], env);
        break;
      }

      case "updatePoint": {
        const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
        if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
          await env.DB.prepare(
            "INSERT OR REPLACE INTO points (room, seat_number, x, y, fast) VALUES (?, ?, ?, ?, ?)"
          ).bind(pointRoom, pointSeat, pointX || 0, pointY || 0, pointFast || 0).run();

          await this.broadcastToRoom(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast], env);
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

        await this.broadcastToRoom(removeRoom, ["removeKursi", removeRoom, removeSeat], env);
        await this.broadcastToRoom(removeRoom, ["roomUserCount", removeRoom, await this.getRoomCount(removeRoom)], env);
        break;
      }

      case "private": {
        const [privTarget, privNoimg, privMsg, privSender] = args;
        if (privTarget && privMsg) {
          const targetUser = await env.DB.prepare(
            "SELECT ws_id FROM users WHERE username = ? AND active = 1"
          ).bind(privTarget).first();

          if (targetUser) {
            await this.sendToUser(targetUser.ws_id, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender], env);
          }
          ws.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
        }
        break;
      }

      case "gift": {
        const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
        if (giftRoom && ROOMS_SET.has(giftRoom)) {
          await this.broadcastToRoom(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()], env);
        }
        break;
      }

      case "rollangak": {
        const [rollRoom, rollUser, rollAngka] = args;
        if (rollRoom && ROOMS_SET.has(rollRoom)) {
          await this.broadcastToRoom(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka], env);
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
            await this.sendToUser(targetUser.ws_id, ["notif", notifNoimg, notifUser, notifMsg, Date.now()], env);
          }
        }
        break;
      }

      case "getCurrentNumber": {
        const roomSettings = await env.DB.prepare(
          "SELECT current_number FROM room_settings WHERE room = ?"
        ).bind("General").first();
        ws.send(JSON.stringify(["currentNumber", roomSettings?.current_number || 1]));
        break;
      }

      case "isUserOnline": {
        const [onlineTarget, onlineCallback] = args;
        const result = await env.DB.prepare(
          "SELECT active FROM users WHERE username = ?"
        ).bind(onlineTarget).first();
        const isOnline = result?.active === 1;
        ws.send(JSON.stringify(["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]));
        break;
      }

      case "getOnlineUsers": {
        const users = await env.DB.prepare(
          "SELECT username FROM users WHERE active = 1"
        ).all();
        const userList = (users.results || []).map(u => u.username);
        ws.send(JSON.stringify(["allOnlineUsers", userList]));
        break;
      }

      case "getAllRoomsUserCount": {
        const counts = {};
        for (const room of ROOMS) {
          counts[room] = await this.getRoomCount(room);
        }
        ws.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)]));
        break;
      }

      case "getRoomUserCount": {
        const [roomName] = args;
        if (roomName && ROOMS_SET.has(roomName)) {
          const count = await this.getRoomCount(roomName);
          ws.send(JSON.stringify(["roomUserCount", roomName, count]));
        }
        break;
      }

      case "setMuteType": {
        const [muteVal, muteRoom] = args;
        if (muteRoom && ROOMS_SET.has(muteRoom)) {
          await env.DB.prepare(
            "INSERT OR REPLACE INTO room_settings (room, muted) VALUES (?, ?)"
          ).bind(muteRoom, muteVal ? 1 : 0).run();

          await this.broadcastToRoom(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom], env);
          ws.send(JSON.stringify(["muteTypeSet", !!muteVal, true, muteRoom]));
        }
        break;
      }

      case "getMuteType": {
        const [muteRoom] = args;
        if (muteRoom && ROOMS_SET.has(muteRoom)) {
          const result = await env.DB.prepare(
            "SELECT muted FROM room_settings WHERE room = ?"
          ).bind(muteRoom).first();
          ws.send(JSON.stringify(["muteTypeResponse", result?.muted === 1, muteRoom]));
        }
        break;
      }

      case "modwarning": {
        const [modRoom] = args;
        if (modRoom && ROOMS_SET.has(modRoom)) {
          await this.broadcastToRoom(modRoom, ["modwarning", modRoom], env);
        }
        break;
      }

      case "onDestroy": {
        const conn = wsConnections.get(ws._wsId);
        if (conn) {
          wsConnections.delete(ws._wsId);
          await env.DB.prepare(
            "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
          ).bind(Date.now(), ws._wsId).run();
        }
        break;
      }

      default:
        ws.send(JSON.stringify(["error", `Unknown event: ${evt}`]));
        break;
    }
  }

  // ========== HANDLE SET ID ==========
  async handleSetId(ws, username, isNewUser, env) {
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
  async handleJoin(ws, roomName, env) {
    if (!roomName || !ROOMS_SET.has(roomName)) return;

    const username = ws.username || "Anonymous";

    await env.DB.prepare(
      "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
    ).bind(roomName, Date.now(), username).run();

    let seat = await this.getSeatNumber(username, roomName, env);
    if (!seat) {
      seat = await this.getAvailableSeat(roomName, env);
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

      await this.broadcastToRoom(roomName, ["roomUserCount", roomName, await this.getRoomCount(roomName)], env);
      await this.broadcastAllSeats(roomName, env);

      setTimeout(async () => {
        await this.sendAllStateTo(ws, roomName, env);
      }, 1000);
    }
  }

  // ========== HANDLE MULTI JOIN ==========
  async handleMultiJoin(ws, multiUsername, multiRoomname, env) {
    if (!multiUsername || !multiRoomname) return;

    await env.DB.prepare(
      "DELETE FROM seats WHERE username = ?"
    ).bind(multiUsername).run();

    const seat = await this.getAvailableSeat(multiRoomname, env);
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
    await this.broadcastToRoom(multiRoomname, ["roomUserCount", multiRoomname, await this.getRoomCount(multiRoomname)], env);
  }

  // ========== HANDLE EXIT MULTI ==========
  async handleExitMulti(ws, targetUsername, env) {
    if (!targetUsername) return;

    const seat = await this.getSeatNumber(targetUsername, null, env);
    if (seat) {
      await env.DB.prepare(
        "DELETE FROM seats WHERE username = ?"
      ).bind(targetUsername).run();

      const room = await env.DB.prepare(
        "SELECT room FROM users WHERE username = ?"
      ).bind(targetUsername).first();

      if (room) {
        await this.broadcastToRoom(room.room, ["removeKursi", room.room, seat], env);
        await this.broadcastToRoom(room.room, ["roomUserCount", room.room, await this.getRoomCount(room.room)], env);
      }
    }

    await env.DB.prepare(
      "UPDATE users SET active = 0, last_active = ? WHERE username = ?"
    ).bind(Date.now(), targetUsername).run();
  }

  // ========== HANDLE SET ACTIVE MULTI ==========
  async handleSetActiveMulti(ws, targetUsername, env) {
    if (!targetUsername) return;

    const seat = await this.getSeatNumber(targetUsername, null, env);
    const room = await env.DB.prepare(
      "SELECT room FROM users WHERE username = ?"
    ).bind(targetUsername).first();

    if (seat && room) {
      ws.send(JSON.stringify(["activeChangedMulti", targetUsername, seat, room.room]));
      await this.broadcastToRoom(room.room, ["userActiveChanged", targetUsername, seat], env);
    }
  }

  // ========== HANDLE UPDATE KURSI ==========
  async handleUpdateKursi(kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt, env) {
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
      await this.broadcastToRoom(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updated]]], env);
    }
  }

  // ========== HELPER FUNCTIONS ==========

  async getRoomCount(room) {
    const env = this.env;
    const result = await env.DB.prepare(
      "SELECT COUNT(*) as count FROM users WHERE room = ? AND active = 1"
    ).bind(room).first();
    return result?.count || 0;
  }

  async getSeatNumber(username, room, env) {
    let query = "SELECT seat_number FROM seats WHERE username = ?";
    let params = [username];
    if (room) {
      query += " AND room = ?";
      params.push(room);
    }
    const result = await env.DB.prepare(query).bind(...params).first();
    return result?.seat_number || null;
  }

  async getAvailableSeat(room, env) {
    const seats = await env.DB.prepare(
      "SELECT seat_number FROM seats WHERE room = ? ORDER BY seat_number"
    ).bind(room).all();

    const taken = new Set((seats.results || []).map(s => s.seat_number));
    for (let i = 1; i <= C.MAX_SEATS; i++) {
      if (!taken.has(i)) return i;
    }
    return null;
  }

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

  async sendToUser(wsId, message, env) {
    const conn = wsConnections.get(wsId);
    if (conn && conn.ws && conn.ws.readyState === 1) {
      try {
        conn.ws.send(JSON.stringify(message));
        return true;
      } catch(e) {}
    }
    return false;
  }

  async broadcastUserList(room, env) {
    const users = await env.DB.prepare(
      "SELECT username FROM users WHERE room = ? AND active = 1"
    ).bind(room).all();

    await this.broadcastToRoom(room, ["userList", (users.results || []).map(u => u.username)], env);
  }

  async broadcastAllSeats(room, env) {
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

    await this.broadcastToRoom(room, ["allUpdateKursiList", room, seatData], env);
  }

  async sendAllStateTo(ws, room, env) {
    await this.broadcastAllSeats(room, env);

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
  }
}
