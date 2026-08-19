// ==================== INDEX.JS - PURE WORKER + D1 ====================
// VERSION: 4.0.0 - TANPA DURABLE OBJECTS!

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  BATCH_SIZE: 20,
  LOCK_TIMEOUT: 10000,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

// ==================== WEBSOCKET STORAGE (MEMORY) ====================
const wsConnections = new Map(); // wsId → { ws, username, room }
const userConnections = new Map(); // username → Set(ws)
const userSeat = new Map(); // username → { room, seat, isMulti }
const roomClients = new Map(); // room → Set(ws)

// ==================== ROOM MANAGER (IN-MEMORY CACHE) ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();
    this.points = new Map();
    this.muted = false;
    this.number = 1;
    this.loaded = false;
  }

  async load(env) {
    if (this.loaded) return;
    this.loaded = true;

    try {
      // Load seats
      const seats = await env.DB.prepare(
        "SELECT seat_number, username, noimageUrl, color, itembawah, itematas, vip, viptanda FROM seats WHERE room = ?"
      ).bind(this.name).all();

      for (const seat of seats.results || []) {
        this.seats.set(seat.seat_number, {
          namauser: seat.username,
          noimageUrl: seat.noimageUrl || "",
          color: seat.color || "",
          itembawah: seat.itembawah || 0,
          itematas: seat.itematas || 0,
          vip: seat.vip || 0,
          viptanda: seat.viptanda || 0
        });
      }

      // Load points
      const points = await env.DB.prepare(
        "SELECT seat_number, x, y, fast FROM points WHERE room = ?"
      ).bind(this.name).all();

      for (const point of points.results || []) {
        this.points.set(point.seat_number, {
          x: point.x || 0,
          y: point.y || 0,
          fast: point.fast || 0
        });
      }

      // Load settings
      const settings = await env.DB.prepare(
        "SELECT muted, current_number FROM room_settings WHERE room = ?"
      ).bind(this.name).first();

      if (settings) {
        this.muted = settings.muted === 1;
        this.number = settings.current_number || 1;
      }
    } catch(e) {
      console.error(`Failed to load room ${this.name}:`, e);
    }
  }

  async save(env) {
    try {
      // Save seats
      for (const [seat, data] of this.seats) {
        await env.DB.prepare(
          `INSERT OR REPLACE INTO seats 
            (room, seat_number, username, noimageUrl, color, itembawah, itematas, vip, viptanda) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).bind(
          this.name, seat, data.namauser, data.noimageUrl || "",
          data.color || "", data.itembawah || 0, data.itematas || 0,
          data.vip || 0, data.viptanda || 0
        ).run();
      }

      // Save points
      for (const [seat, data] of this.points) {
        await env.DB.prepare(
          `INSERT OR REPLACE INTO points 
            (room, seat_number, x, y, fast) 
            VALUES (?, ?, ?, ?, ?)`
        ).bind(this.name, seat, data.x || 0, data.y || 0, data.fast || 0).run();
      }

      // Save settings
      await env.DB.prepare(
        `INSERT OR REPLACE INTO room_settings 
          (room, muted, current_number) 
          VALUES (?, ?, ?)`
      ).bind(this.name, this.muted ? 1 : 0, this.number).run();

    } catch(e) {
      console.error(`Failed to save room ${this.name}:`, e);
    }
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= C.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, noimageUrl, color, itembawah, itematas, vip, viptanda) {
    if (!userId) return null;
    for (const [seat, data] of this.seats) {
      if (data && data.namauser === userId) return seat;
    }
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: noimageUrl || "",
      namauser: userId,
      color: color || "",
      itembawah: itembawah || 0,
      itematas: itematas || 0,
      vip: vip || 0,
      viptanda: viptanda || 0,
    });
    return seat;
  }

  updateSeat(seat, data) {
    if (!this.seats.has(seat) || !data) return false;
    this.seats.set(seat, {
      noimageUrl: data.noimageUrl || "",
      namauser: data.namauser || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0
    });
    return true;
  }

  removeSeat(seat) {
    this.points.delete(seat);
    return this.seats.delete(seat);
  }

  getSeat(seat) {
    const data = this.seats.get(seat);
    return data ? { ...data } : null;
  }

  getCount() { return this.seats.size; }

  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      if (data) result[seat] = { ...data };
    }
    return result;
  }

  setMuted(val) { this.muted = !!val; return this.muted; }
  getMuted() { return this.muted; }
  setNumber(n) { this.number = n || 1; }
  getNumber() { return this.number; }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, { x: x || 0, y: y || 0, fast: !!fast });
    return true;
  }

  getPoint(seat) {
    const point = this.points.get(seat);
    return point ? { ...point } : null;
  }

  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      if (this.seats.has(seat) && point) {
        result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    return result;
  }
}

// ==================== ROOMS CACHE ====================
const roomsCache = new Map();

async function getRoomManager(roomName, env) {
  if (!roomsCache.has(roomName)) {
    const rm = new RoomManager(roomName);
    await rm.load(env);
    roomsCache.set(roomName, rm);
  }
  return roomsCache.get(roomName);
}

// ==================== MAIN WORKER ====================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // ============================================================
    // WEBSOCKET
    // ============================================================
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      const upgrade = request.headers.get("Upgrade");

      if (upgrade !== "websocket") {
        return new Response(getTestHTML(), {
          status: 200,
          headers: { "Content-Type": "text/html", "Cache-Control": "no-cache" }
        });
      }

      const username = url.searchParams.get("username") || "User_" + Math.floor(Math.random() * 1000);
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

      server.wsId = wsId;
      server.username = username;
      server.room = room;

      // ✅ SIMPAN KE D1
      try {
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
        console.error("D1 error:", e);
      }

      // ✅ KIRIM AWAL
      try {
        server.send(JSON.stringify(["connection", "success", wsId]));
        server.send(JSON.stringify(["joinroomawal"]));
      } catch(e) {}

      // ✅ WEBSOCKET MESSAGE
      server.addEventListener("message", async (event) => {
        try {
          let data;
          try { data = JSON.parse(event.data); } catch(e) { return; }
          if (!Array.isArray(data) || !data.length) return;

          const [evt, ...args] = data;
          await handleEvent(server, evt, args, env);
        } catch(e) {
          console.error("Message error:", e);
        }
      });

      // ✅ WEBSOCKET CLOSE
      server.addEventListener("close", async () => {
        const conn = wsConnections.get(wsId);
        if (conn) {
          const { username, room } = conn;

          try {
            await env.DB.prepare(
              "DELETE FROM seats WHERE room = ? AND username = ?"
            ).bind(room, username).run();

            await env.DB.prepare(
              "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
            ).bind(Date.now(), wsId).run();
          } catch(e) {}

          const rm = await getRoomManager(room, env);
          const seat = getSeatNumberFromMap(username, rm);
          if (seat) {
            rm.removeSeat(seat);
            await broadcastToRoom(room, ["removeKursi", room, seat], env);
            await broadcastToRoom(room, ["roomUserCount", room, rm.getCount()], env);
          }
          await rm.save(env);
        }
        wsConnections.delete(wsId);
      });

      server.addEventListener("error", async () => {
        wsConnections.delete(wsId);
        try {
          await env.DB.prepare(
            "UPDATE users SET active = 0, last_active = ? WHERE ws_id = ?"
          ).bind(Date.now(), wsId).run();
        } catch(e) {}
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // ============================================================
    // STATUS
    // ============================================================
    if (pathname === "/status") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: wsConnections.size,
        rooms: ROOMS,
        timestamp: Date.now()
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    return new Response("Chat Server Running", { status: 200 });
  }
};

// ============================================================
// HANDLE EVENT
// ============================================================
async function handleEvent(ws, evt, args, env) {
  switch(evt) {

    case "setIdTarget2": {
      const [username, isNewUser] = args;
      if (!username) return;
      ws.username = username;
      ws.send(JSON.stringify(isNewUser ? ["joinroomawal"] : ["needJoinRoom"]));
      break;
    }

    case "joinRoom": {
      const [roomName] = args;
      if (!roomName || !ROOMS_SET.has(roomName)) {
        ws.send(JSON.stringify(["error", "Invalid room"]));
        return;
      }

      const username = ws.username || "Anonymous";
      ws.room = roomName;

      try {
        await env.DB.prepare(
          "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
        ).bind(roomName, Date.now(), username).run();
      } catch(e) {}

      const rm = await getRoomManager(roomName, env);

      let seat = getSeatNumberFromMap(username, rm);
      if (!seat) {
        seat = rm.getAvailableSeat();
        if (seat) {
          rm.addSeat(username, "", "", 0, 0, 0, 0);
        }
      }

      if (seat) {
        ws.send(JSON.stringify(["rooMasuk", seat, roomName]));
        ws.send(JSON.stringify(["numberKursiSaya", seat]));

        // Add to room clients
        if (!roomClients.has(roomName)) {
          roomClients.set(roomName, new Set());
        }
        roomClients.get(roomName).add(ws);

        await broadcastToRoom(roomName, ["roomUserCount", roomName, rm.getCount()], env);
        await broadcastAllSeats(roomName, env);
        await broadcastAllPoints(roomName, env);
        await rm.save(env);
      }
      break;
    }

    case "chat": {
      const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
      if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;

      try {
        await env.DB.prepare(
          "INSERT INTO messages (room, username, message, timestamp) VALUES (?, ?, ?, ?)"
        ).bind(chatRoom, chatUser, chatMsg, Date.now()).run();
      } catch(e) {}

      await broadcastToRoom(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor || "", chatTextColor || ""], env);
      break;
    }

    case "updatePoint": {
      const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
      if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
        const rm = await getRoomManager(pointRoom, env);
        if (rm && rm.seats.has(pointSeat)) {
          rm.updatePoint(pointSeat, pointX, pointY, pointFast === 1);
          await broadcastToRoom(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast], env);
          await rm.save(env);
        }
      }
      break;
    }

    case "updateKursi": {
      const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
      if (!kursiRoom || !kursiSeat) break;

      const rm = await getRoomManager(kursiRoom, env);
      if (!rm) break;

      rm.updateSeat(kursiSeat, {
        noimageUrl: kursiNoimg || "",
        namauser: kursiName || "",
        color: kursiColor || "",
        itembawah: kursiBawah || 0,
        itematas: kursiAtas || 0,
        vip: kursiVip || 0,
        viptanda: kursiVt || 0
      });

      const updated = rm.getSeat(kursiSeat);
      if (updated) {
        await broadcastToRoom(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updated]]], env);
        await rm.save(env);
      }
      break;
    }

    case "removeKursiAndPoint": {
      const [removeRoom, removeSeat] = args;
      const rm = await getRoomManager(removeRoom, env);
      if (rm && rm.seats.has(removeSeat)) {
        rm.removeSeat(removeSeat);
        await broadcastToRoom(removeRoom, ["removeKursi", removeRoom, removeSeat], env);
        await broadcastToRoom(removeRoom, ["roomUserCount", removeRoom, rm.getCount()], env);
        await rm.save(env);
      }
      break;
    }

    case "private": {
      const [privTarget, privNoimg, privMsg, privSender] = args;
      if (privTarget && privMsg) {
        try {
          const targetUser = await env.DB.prepare(
            "SELECT ws_id FROM users WHERE username = ? AND active = 1"
          ).bind(privTarget).first();

          if (targetUser) {
            const conn = wsConnections.get(targetUser.ws_id);
            if (conn && conn.ws && conn.ws.readyState === 1) {
              conn.ws.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
            }
          }
        } catch(e) {}
        ws.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
      }
      break;
    }

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

    case "getAllRoomsUserCount": {
      const counts = {};
      for (const room of ROOMS) {
        const rm = await getRoomManager(room, env);
        counts[room] = rm?.getCount() || 0;
      }
      ws.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)]));
      break;
    }

    case "getRoomUserCount": {
      const [roomName] = args;
      if (roomName && ROOMS_SET.has(roomName)) {
        const rm = await getRoomManager(roomName, env);
        ws.send(JSON.stringify(["roomUserCount", roomName, rm?.getCount() || 0]));
      }
      break;
    }

    case "setMuteType": {
      const [muteVal, muteRoom] = args;
      if (muteRoom && ROOMS_SET.has(muteRoom)) {
        const rm = await getRoomManager(muteRoom, env);
        rm.setMuted(muteVal);
        await broadcastToRoom(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom], env);
        ws.send(JSON.stringify(["muteTypeSet", !!muteVal, true, muteRoom]));
        await rm.save(env);
      }
      break;
    }

    case "getMuteType": {
      const [muteRoom] = args;
      if (muteRoom && ROOMS_SET.has(muteRoom)) {
        const rm = await getRoomManager(muteRoom, env);
        ws.send(JSON.stringify(["muteTypeResponse", rm?.getMuted() || false, muteRoom]));
      }
      break;
    }

    case "getCurrentNumber": {
      const rm = await getRoomManager("General", env);
      ws.send(JSON.stringify(["currentNumber", rm?.getNumber() || 1]));
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

function getSeatNumberFromMap(username, rm) {
  for (const [seat, data] of rm.seats) {
    if (data && data.namauser === username) return seat;
  }
  return null;
}

async function broadcastToRoom(room, message, env) {
  const msgStr = JSON.stringify(message);
  const clients = roomClients.get(room);
  if (!clients || clients.size === 0) return;

  for (const ws of clients) {
    if (ws && ws.readyState === 1) {
      try { ws.send(msgStr); } catch(e) {}
    }
  }
}

async function broadcastAllSeats(room, env) {
  const rm = await getRoomManager(room, env);
  const allSeats = rm.getAllSeats();
  if (Object.keys(allSeats).length > 0) {
    await broadcastToRoom(room, ["allUpdateKursiList", room, allSeats], env);
  }
}

async function broadcastAllPoints(room, env) {
  const rm = await getRoomManager(room, env);
  const allPoints = rm.getAllPoints();
  if (allPoints.length > 0) {
    await broadcastToRoom(room, ["allPointsList", room, allPoints], env);
  }
}

// ============================================================
// HTML TEST PAGE
// ============================================================
function getTestHTML() {
  return `
<!DOCTYPE html>
<html>
<head>
  <title>Chat D1</title>
  <meta charset="UTF-8">
  <style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family:Arial; background:#0d1117; color:#e6edf3; display:flex; justify-content:center; align-items:center; height:100vh; }
    .container { width:500px; max-width:90%; background:#161b22; padding:20px; border-radius:12px; }
    h2 { color:#58a6ff; margin-bottom:15px; }
    input, button { width:100%; padding:10px; margin:5px 0; border-radius:6px; border:1px solid #30363d; background:#0d1117; color:#e6edf3; }
    button { background:#238636; border:none; cursor:pointer; font-weight:bold; }
    button:hover { background:#2ea043; }
    #status { padding:10px; border-radius:6px; margin:10px 0; text-align:center; }
    #status.connected { background:#0d4426; color:#3fb950; }
    #status.disconnected { background:#44260d; color:#f0883e; }
    #messages { height:300px; overflow-y:auto; border:1px solid #30363d; border-radius:6px; padding:10px; margin:10px 0; }
    .msg { margin:5px 0; padding:5px 10px; background:#0d1117; border-radius:4px; }
    .msg .name { color:#58a6ff; }
    .msg .time { color:#8b949e; font-size:11px; margin-left:10px; }
    .input-row { display:flex; gap:10px; }
    .input-row input { flex:1; }
    .input-row button { width:auto; padding:10px 20px; }
  </style>
</head>
<body>
  <div class="container">
    <h2>💬 Chat D1</h2>
    <div id="status" class="disconnected">⏳ Connecting...</div>
    <input id="username" placeholder="Username" value="User_${Math.floor(Math.random() * 1000)}" />
    <button onclick="connect()">Connect</button>
    <div id="messages"></div>
    <div class="input-row">
      <input id="msgInput" placeholder="Type message..." disabled />
      <button id="sendBtn" disabled>Send</button>
    </div>
  </div>

  <script>
    let ws = null, username = '';
    function connect() {
      username = document.getElementById('username').value.trim() || 'User_' + Math.floor(Math.random() * 1000);
      const status = document.getElementById('status');
      try {
        ws = new WebSocket(\`wss://\${window.location.host}/ws?username=\${encodeURIComponent(username)}\`);
        ws.onopen = () => {
          status.className = 'connected';
          status.textContent = '✅ Connected as ' + username;
          document.getElementById('msgInput').disabled = false;
          document.getElementById('sendBtn').disabled = false;
          ws.send(JSON.stringify(["setIdTarget2", username, true]));
        };
        ws.onmessage = (e) => {
          try {
            const data = JSON.parse(e.data);
            console.log('📩', data);
            if (data[0] === 'chat') addMessage(data[2], data[3]);
            else if (data[0] === 'joinroomawal') {
              ws.send(JSON.stringify(["joinRoom", "General"]));
            } else if (data[0] === 'rooMasuk') {
              addSystemMessage('📢 Seat: ' + data[1]);
            } else if (data[0] === 'roomUserCount') {
              addSystemMessage('👥 Users: ' + data[2]);
            } else if (data[0] === 'allUpdateKursiList') {
              addSystemMessage('🪑 Seats updated');
            } else if (data[0] === 'allPointsList') {
              addSystemMessage('📍 Points updated');
            }
          } catch(e) { console.error(e); }
        };
        ws.onclose = () => {
          status.className = 'disconnected';
          status.textContent = '❌ Disconnected - Reconnecting...';
          document.getElementById('msgInput').disabled = true;
          document.getElementById('sendBtn').disabled = true;
          setTimeout(connect, 3000);
        };
        ws.onerror = () => {
          status.className = 'disconnected';
          status.textContent = '❌ Error';
        };
      } catch(e) { console.error(e); }
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
      div.innerHTML = \`<span class="name">\${name}</span><span class="time">\${new Date().toLocaleTimeString()}</span><div>\${msg}</div>\`;
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

    document.getElementById('sendBtn').onclick = sendMessage;
    document.getElementById('msgInput').onkeydown = (e) => { if (e.key === 'Enter') sendMessage(); };
    connect();
  </script>
</body>
</html>
  `;
}
