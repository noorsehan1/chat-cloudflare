// ==================== CHAT-SERVER.JS ====================
// VERSION: 4.0.1 - FIXED CONNECTION

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

    // ✅ CHAT WEBSOCKET
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      const upgrade = request.headers.get("Upgrade");
      
      // ✅ JIKA BUKAN WEBSOCKET, TAMPILKAN HTML TEST
      if (upgrade !== "websocket") {
        return new Response(this.getTestHTML(), {
          status: 200,
          headers: { 
            "Content-Type": "text/html",
            "Cache-Control": "no-cache"
          }
        });
      }

      // ✅ AMBIL PARAMETER
      const username = url.searchParams.get("username") || "User_" + Math.floor(Math.random() * 1000);
      const room = url.searchParams.get("room") || "General";

      // ✅ CEK LIMIT
      if (wsConnections.size >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }

      // ✅ BUAT WEBSOCKET PAIR
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];

      try {
        // ✅ ACCEPT WEBSOCKET
        server.accept();
        console.log(`✅ WebSocket connected: ${username} in ${room}`);
      } catch(e) {
        console.error("❌ Accept failed:", e);
        return new Response("WebSocket acceptance failed", { status: 500 });
      }

      // ✅ SIMPAN CONNECTION
      const wsId = crypto.randomUUID();
      wsConnections.set(wsId, {
        ws: server,
        username: username,
        room: room,
        connectedAt: Date.now()
      });

      // ✅ SET VARIABLE DI WS
      server.wsId = wsId;
      server.username = username;
      server.room = room;

      // ✅ SIMPAN KE D1 (TAPI TIDAK BLOKIR)
      try {
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
      } catch(e) {
        console.error("❌ D1 error:", e);
      }

      // ✅ KIRIM CONFIRMATION KE CLIENT
      try {
        server.send(JSON.stringify(["connection", "success", wsId]));
        server.send(JSON.stringify(["joinroomawal"]));
      } catch(e) {
        console.error("❌ Send error:", e);
      }

      // ============================================================
      // ✅ WEBSOCKET MESSAGE HANDLER
      // ============================================================
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
          console.log(`📩 Event: ${evt} from ${username}`);
          
          await this.handleEvent(server, evt, args, this.env);
        } catch(e) {
          console.error("❌ Message error:", e);
        }
      });

      // ============================================================
      // ✅ WEBSOCKET CLOSE
      // ============================================================
      server.addEventListener("close", async () => {
        console.log(`🔌 WebSocket closed: ${username}`);
        const conn = wsConnections.get(wsId);
        if (conn) {
          wsConnections.delete(wsId);
          try {
            await this.env.DB.prepare(
              "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
            ).bind(Date.now(), wsId).run();
          } catch(e) {}
        }
      });

      // ============================================================
      // ✅ WEBSOCKET ERROR
      // ============================================================
      server.addEventListener("error", async () => {
        console.log(`❌ WebSocket error: ${username}`);
        wsConnections.delete(wsId);
        try {
          await this.env.DB.prepare(
            "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
          ).bind(Date.now(), wsId).run();
        } catch(e) {}
      });

      // ✅ RETURN RESPONSE
      return new Response(null, { 
        status: 101, 
        webSocket: client 
      });
    }

    // ============================================================
    // ✅ TEST PAGE
    // ============================================================
    if (pathname === "/test") {
      return new Response(this.getTestHTML(), {
        status: 200,
        headers: { "Content-Type": "text/html" }
      });
    }

    // ============================================================
    // ✅ STATUS
    // ============================================================
    if (pathname === "/status") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: wsConnections.size,
        maxConnections: C.MAX_GLOBAL_CONNECTIONS,
        rooms: ROOMS,
        timestamp: Date.now()
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    return new Response("Chat Server Running", { status: 200 });
  }

  // ============================================================
  // ✅ GET TEST HTML
  // ============================================================
  getTestHTML() {
    return `
<!DOCTYPE html>
<html>
<head>
  <title>Chat Test</title>
  <meta charset="UTF-8">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: Arial; background: #0d1117; color: #e6edf3; display: flex; justify-content: center; align-items: center; height: 100vh; }
    .container { width: 500px; max-width: 90%; background: #161b22; padding: 20px; border-radius: 12px; }
    h2 { margin-bottom: 15px; color: #58a6ff; }
    input, button { width: 100%; padding: 10px; margin: 5px 0; border-radius: 6px; border: 1px solid #30363d; background: #0d1117; color: #e6edf3; }
    button { background: #238636; border: none; cursor: pointer; font-weight: bold; }
    button:hover { background: #2ea043; }
    #status { padding: 10px; border-radius: 6px; margin: 10px 0; text-align: center; }
    #status.connected { background: #0d4426; color: #3fb950; }
    #status.disconnected { background: #44260d; color: #f0883e; }
    #messages { height: 300px; overflow-y: auto; border: 1px solid #30363d; border-radius: 6px; padding: 10px; margin: 10px 0; }
    .msg { margin: 5px 0; padding: 5px 10px; background: #0d1117; border-radius: 4px; }
    .msg .name { color: #58a6ff; }
    .msg .time { color: #8b949e; font-size: 11px; margin-left: 10px; }
    .input-row { display: flex; gap: 10px; }
    .input-row input { flex: 1; }
    .input-row button { width: auto; padding: 10px 20px; }
  </style>
</head>
<body>
  <div class="container">
    <h2>💬 Chat Test</h2>
    <div id="status" class="disconnected">⏳ Connecting...</div>
    <div class="input-row">
      <input id="username" placeholder="Username" value="User_${Math.floor(Math.random() * 1000)}" />
      <button onclick="connect()">Connect</button>
    </div>
    <div id="messages"></div>
    <div class="input-row">
      <input id="msgInput" placeholder="Type message..." disabled />
      <button id="sendBtn" disabled>Send</button>
    </div>
  </div>

  <script>
    let ws = null;
    let username = '';

    function connect() {
      username = document.getElementById('username').value.trim() || 'User_' + Math.floor(Math.random() * 1000);
      const status = document.getElementById('status');
      
      try {
        const wsUrl = \`wss://\${window.location.host}/ws?username=\${encodeURIComponent(username)}&room=General\`;
        console.log('Connecting to:', wsUrl);
        
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          status.className = 'connected';
          status.textContent = '✅ Connected as ' + username;
          document.getElementById('msgInput').disabled = false;
          document.getElementById('sendBtn').disabled = false;
          console.log('✅ WebSocket connected');
          
          // Kirim setIdTarget2
          ws.send(JSON.stringify(["setIdTarget2", username, true]));
        };

        ws.onmessage = (e) => {
          try {
            const data = JSON.parse(e.data);
            console.log('📩 Received:', data);
            
            if (data[0] === 'chat') {
              addMessage(data[2], data[3]);
            } else if (data[0] === 'joinroomawal') {
              // Join room
              ws.send(JSON.stringify(["joinRoom", "General"]));
              addSystemMessage('📢 Joined General room');
            } else if (data[0] === 'rooMasuk') {
              addSystemMessage('📢 Seat: ' + data[1]);
            } else if (data[0] === 'connection') {
              addSystemMessage('📢 Connection: ' + data[2]);
            } else if (data[0] === 'roomUserCount') {
              addSystemMessage('👥 Users: ' + data[2]);
            } else if (data[0] === 'userList') {
              addSystemMessage('👥 Online: ' + (data[1] || []).join(', '));
            }
          } catch(e) {
            console.error('Parse error:', e);
          }
        };

        ws.onclose = () => {
          status.className = 'disconnected';
          status.textContent = '❌ Disconnected - Reconnecting...';
          document.getElementById('msgInput').disabled = true;
          document.getElementById('sendBtn').disabled = true;
          console.log('❌ WebSocket closed');
          
          setTimeout(connect, 3000);
        };

        ws.onerror = (e) => {
          console.error('❌ WebSocket error:', e);
        };

      } catch(e) {
        console.error('❌ Connection error:', e);
        status.className = 'disconnected';
        status.textContent = '❌ Error: ' + e.message;
      }
    }

    function sendMessage() {
      const input = document.getElementById('msgInput');
      const msg = input.value.trim();
      if (!msg || !ws || ws.readyState !== 1) return;
      
      ws.send(JSON.stringify(["chat", "General", "", username, msg, "", ""]));
      input.value = '';
    }

    function addMessage(name, msg) {
      const container = document.getElementById('messages');
      const div = document.createElement('div');
      div.className = 'msg';
      const time = new Date().toLocaleTimeString();
      div.innerHTML = \`<span class="name">\${name}</span><span class="time">\${time}</span><div>\${msg}</div>\`;
      container.appendChild(div);
      container.scrollTop = container.scrollHeight;
    }

    function addSystemMessage(msg) {
      const container = document.getElementById('messages');
      const div = document.createElement('div');
      div.className = 'msg';
      div.style.textAlign = 'center';
      div.style.color = '#8b949e';
      div.style.background = 'none';
      div.textContent = msg;
      container.appendChild(div);
      container.scrollTop = container.scrollHeight;
    }

    // Event listeners
    document.getElementById('sendBtn').onclick = sendMessage;
    document.getElementById('msgInput').onkeydown = (e) => {
      if (e.key === 'Enter') sendMessage();
    };

    // Auto connect
    connect();
  </script>
</body>
</html>
    `;
  }

  // ============================================================
  // ✅ HANDLE EVENT
  // ============================================================
  async handleEvent(ws, evt, args, env) {
    console.log(`📩 Event: ${evt}, Args:`, args);

    switch(evt) {

      // ============================================================
      // SET ID TARGET 2
      // ============================================================
      case "setIdTarget2": {
        const [username, isNewUser] = args;
        if (!username) return;
        
        ws.username = username;
        ws.send(JSON.stringify(isNewUser ? ["joinroomawal"] : ["needJoinRoom"]));
        break;
      }

      // ============================================================
      // JOIN ROOM
      // ============================================================
      case "joinRoom": {
        const [roomName] = args;
        if (!roomName || !ROOMS_SET.has(roomName)) {
          ws.send(JSON.stringify(["error", "Invalid room"]));
          return;
        }

        const username = ws.username || "Anonymous";
        ws.room = roomName;

        // Update di DB
        try {
          await env.DB.prepare(
            "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
          ).bind(roomName, Date.now(), username).run();
        } catch(e) {}

        // Dapatkan seat
        let seat = await this.getSeatNumber(username, roomName, env);
        if (!seat) {
          seat = await this.getAvailableSeat(roomName, env);
          if (seat) {
            try {
              await env.DB.prepare(
                "INSERT INTO seats (room, seat_number, username) VALUES (?, ?, ?)"
              ).bind(roomName, seat, username).run();
            } catch(e) {}
          }
        }

        if (seat) {
          ws.send(JSON.stringify(["rooMasuk", seat, roomName]));
          ws.send(JSON.stringify(["numberKursiSaya", seat]));
          
          await this.broadcastToRoom(roomName, ["roomUserCount", roomName, await this.getRoomCount(roomName)], env);
          await this.broadcastAllSeats(roomName, env);
        }
        break;
      }

      // ============================================================
      // CHAT
      // ============================================================
      case "chat": {
        const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
        if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;

        try {
          await env.DB.prepare(
            "INSERT INTO messages (room, username, message, timestamp) VALUES (?, ?, ?, ?)"
          ).bind(chatRoom, chatUser, chatMsg, Date.now()).run();
        } catch(e) {}

        await this.broadcastToRoom(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor || "", chatTextColor || ""], env);
        break;
      }

      // ============================================================
      // UPDATE POINT
      // ============================================================
      case "updatePoint": {
        const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
        if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
          try {
            await env.DB.prepare(
              "INSERT OR REPLACE INTO points (room, seat_number, x, y, fast) VALUES (?, ?, ?, ?, ?)"
            ).bind(pointRoom, pointSeat, pointX || 0, pointY || 0, pointFast || 0).run();
          } catch(e) {}

          await this.broadcastToRoom(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast], env);
        }
        break;
      }

      // ============================================================
      // PRIVATE
      // ============================================================
      case "private": {
        const [privTarget, privNoimg, privMsg, privSender] = args;
        if (privTarget && privMsg) {
          try {
            const targetUser = await env.DB.prepare(
              "SELECT ws_id FROM users WHERE username = ? AND active = 1"
            ).bind(privTarget).first();

            if (targetUser) {
              await this.sendToUser(targetUser.ws_id, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender], env);
            }
          } catch(e) {}
          ws.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
        }
        break;
      }

      // ============================================================
      // GET ONLINE USERS
      // ============================================================
      case "getOnlineUsers": {
        try {
          const users = await env.DB.prepare(
            "SELECT username FROM users WHERE active = 1"
          ).all();
          const userList = (users.results || []).map(u => u.username);
          ws.send(JSON.stringify(["allOnlineUsers", userList]));
        } catch(e) {}
        break;
      }

      // ============================================================
      // GET ROOM USER COUNT
      // ============================================================
      case "getRoomUserCount": {
        const [roomName] = args;
        if (roomName && ROOMS_SET.has(roomName)) {
          const count = await this.getRoomCount(roomName);
          ws.send(JSON.stringify(["roomUserCount", roomName, count]));
        }
        break;
      }

      // ============================================================
      // GET ALL ROOMS USER COUNT
      // ============================================================
      case "getAllRoomsUserCount": {
        const counts = {};
        for (const room of ROOMS) {
          counts[room] = await this.getRoomCount(room);
        }
        ws.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)]));
        break;
      }

      // ============================================================
      // SET MUTE TYPE
      // ============================================================
      case "setMuteType": {
        const [muteVal, muteRoom] = args;
        if (muteRoom && ROOMS_SET.has(muteRoom)) {
          try {
            await env.DB.prepare(
              "INSERT OR REPLACE INTO room_settings (room, muted) VALUES (?, ?)"
            ).bind(muteRoom, muteVal ? 1 : 0).run();
          } catch(e) {}

          await this.broadcastToRoom(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom], env);
          ws.send(JSON.stringify(["muteTypeSet", !!muteVal, true, muteRoom]));
        }
        break;
      }

      // ============================================================
      // GET MUTE TYPE
      // ============================================================
      case "getMuteType": {
        const [muteRoom] = args;
        if (muteRoom && ROOMS_SET.has(muteRoom)) {
          try {
            const result = await env.DB.prepare(
              "SELECT muted FROM room_settings WHERE room = ?"
            ).bind(muteRoom).first();
            ws.send(JSON.stringify(["muteTypeResponse", result?.muted === 1, muteRoom]));
          } catch(e) {
            ws.send(JSON.stringify(["muteTypeResponse", false, muteRoom]));
          }
        }
        break;
      }

      default:
        ws.send(JSON.stringify(["error", `Unknown event: ${evt}`]));
        break;
    }
  }

  // ============================================================
  // HELPER FUNCTIONS
  // ============================================================

  async getRoomCount(room) {
    try {
      const env = this.env;
      const result = await env.DB.prepare(
        "SELECT COUNT(*) as count FROM users WHERE room = ? AND active = 1"
      ).bind(room).first();
      return result?.count || 0;
    } catch(e) {
      return 0;
    }
  }

  async getSeatNumber(username, room, env) {
    try {
      let query = "SELECT seat_number FROM seats WHERE username = ?";
      let params = [username];
      if (room) {
        query += " AND room = ?";
        params.push(room);
      }
      const result = await env.DB.prepare(query).bind(...params).first();
      return result?.seat_number || null;
    } catch(e) {
      return null;
    }
  }

  async getAvailableSeat(room, env) {
    try {
      const seats = await env.DB.prepare(
        "SELECT seat_number FROM seats WHERE room = ? ORDER BY seat_number"
      ).bind(room).all();

      const taken = new Set((seats.results || []).map(s => s.seat_number));
      for (let i = 1; i <= C.MAX_SEATS; i++) {
        if (!taken.has(i)) return i;
      }
      return null;
    } catch(e) {
      return 1;
    }
  }

  async broadcastToRoom(room, message, env) {
    try {
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
    } catch(e) {}
  }

  async sendToUser(wsId, message, env) {
    try {
      const conn = wsConnections.get(wsId);
      if (conn && conn.ws && conn.ws.readyState === 1) {
        conn.ws.send(JSON.stringify(message));
        return true;
      }
    } catch(e) {}
    return false;
  }

  async broadcastAllSeats(room, env) {
    try {
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
    } catch(e) {}
  }
}
