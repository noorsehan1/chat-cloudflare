// ==================== CHAT-WORKER.JS ====================
// VERSION: 5.0.0 - PURE WORKER (NO CRON, NO KV)
// SEMUA STATE DI MEMORY, TIMER DI DALAM WORKER

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_UPDATE_TIK: 6,         // 6 × 15 menit = 90 menit
  MAX_NUMBER: 6,
  BATCH_SIZE: 20,
  LOCK_TIMEOUT: 10000,
  CLEANUP_INTERVAL: 600000,     // 10 MENIT
  MAX_EVENT_QUEUE: 100,
  TIK_INTERVAL_MS: 900000,      // 15 MENIT
  WS_PING_INTERVAL: 30000,      // 30 DETIK
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

// ==================== ROOM MANAGER (DI MEMORY) ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();      // seat -> { namauser, noimageUrl, color, ... }
    this.points = new Map();     // seat -> { x, y, fast }
    this.muted = false;
    this.number = 1;
    this.clients = new Set();    // WebSocket connections
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

  setMuted(val) { 
    this.muted = !!val; 
    return this.muted; 
  }
  
  getMuted() { return this.muted; }
  
  setNumber(n) { 
    this.number = n || 1; 
  }
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

  addClient(ws) {
    if (ws && !this.clients.has(ws)) {
      this.clients.add(ws);
    }
  }

  removeClient(ws) {
    if (ws) {
      this.clients.delete(ws);
    }
  }

  broadcast(msg) {
    if (!msg) return;
    const toRemove = new Set();
    for (const ws of this.clients) {
      try {
        if (ws && ws.readyState === 1 && !ws._closing) {
          ws.send(msg);
        } else {
          toRemove.add(ws);
        }
      } catch(e) {
        toRemove.add(ws);
      }
    }
    for (const ws of toRemove) {
      this.clients.delete(ws);
    }
  }
}

// ==================== GLOBAL STATE ====================
let rooms = new Map();
let currentNumber = 1;
let tikCounter = 0;
let globalWSSet = new Set();          // Semua WebSocket
let userConnections = new Map();      // username -> Set(WebSocket)
let userSeat = new Map();             // username -> { room, seat, isMulti }
let userRoom = new Map();             // username -> room
let wsActiveMulti = new Map();        // WebSocket -> { username, room }

// Timer references
let globalTimer = null;
let cleanupTimer = null;
let isInitialized = false;

// ==================== INITIALIZE ROOMS ====================
function initRooms() {
  if (isInitialized) return;
  isInitialized = true;
  
  for (const room of ROOMS) {
    rooms.set(room, new RoomManager(room));
  }
  
  // Start timers
  startGlobalTimers();
}

function startGlobalTimers() {
  // Timer 15 MENIT - UPDATE NUMBER
  if (globalTimer) clearInterval(globalTimer);
  globalTimer = setInterval(() => {
    processTik();
  }, C.TIK_INTERVAL_MS);

  // Timer 10 MENIT - CLEANUP
  if (cleanupTimer) clearInterval(cleanupTimer);
  cleanupTimer = setInterval(() => {
    performCleanup();
  }, C.CLEANUP_INTERVAL);
}

function processTik() {
  tikCounter++;
  
  // Update number setiap 6 tik (90 menit)
  if (tikCounter >= C.NUMBER_UPDATE_TIK) {
    currentNumber = currentNumber < C.MAX_NUMBER ? currentNumber + 1 : 1;
    tikCounter = 0;
    
    // Update semua room
    const numberMsg = JSON.stringify(["currentNumber", currentNumber]);
    for (const [roomName, roomMan] of rooms) {
      roomMan.setNumber(currentNumber);
      roomMan.broadcast(numberMsg);
    }
    
    console.log(`🔄 Number updated to: ${currentNumber}`);
  }
  
  console.log(`⏱️ Tik ${tikCounter}/${C.NUMBER_UPDATE_TIK} - Number: ${currentNumber}`);
}

function performCleanup() {
  // Cleanup dead connections
  const toRemove = [];
  for (const ws of globalWSSet) {
    if (!ws || ws.readyState !== 1 || ws._closing) {
      toRemove.push(ws);
    }
  }
  for (const ws of toRemove) {
    cleanup(ws);
  }
  
  // Cleanup points for seats that don't exist
  for (const [roomName, roomMan] of rooms) {
    const pointsToRemove = [];
    for (const [seat] of roomMan.points) {
      if (!roomMan.seats.has(seat)) {
        pointsToRemove.push(seat);
      }
    }
    for (const seat of pointsToRemove) {
      roomMan.points.delete(seat);
    }
  }
}

// ==================== CLEANUP ====================
function cleanup(ws) {
  if (!ws || ws._cleaning) return;
  ws._cleaning = true;
  
  try {
    const username = ws.username;
    const room = ws.room;
    const isMulti = ws.isMulti || false;
    
    // Remove dari room clients
    if (room) {
      const roomMan = rooms.get(room);
      if (roomMan) {
        roomMan.removeClient(ws);
        if (!isMulti && ws.seat) {
          roomMan.removeSeat(ws.seat);
          roomMan.broadcast(JSON.stringify(["removeKursi", room, ws.seat]));
          roomMan.broadcast(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
        }
      }
    }
    
    // Remove dari global set
    globalWSSet.delete(ws);
    
    // Remove dari user connections
    if (username) {
      const conns = userConnections.get(username);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          userConnections.delete(username);
          if (!isMulti) {
            const seatInfo = userSeat.get(username);
            if (seatInfo) {
              const roomMan = rooms.get(seatInfo.room);
              if (roomMan && seatInfo.seat) {
                roomMan.removeSeat(seatInfo.seat);
                roomMan.broadcast(JSON.stringify(["removeKursi", seatInfo.room, seatInfo.seat]));
                roomMan.broadcast(JSON.stringify(["roomUserCount", seatInfo.room, roomMan.getCount()]));
              }
            }
            userSeat.delete(username);
            userRoom.delete(username);
          }
        }
      }
    }
    
    // Remove multi data
    wsActiveMulti.delete(ws);
    
  } catch(e) {
    console.error('Cleanup error:', e);
  }
  
  try {
    if (ws.readyState === 1) {
      ws.close(1000, "Cleanup");
    }
  } catch(e) {}
}

// ==================== MAIN WORKER ====================
export default {
  async fetch(request, env, ctx) {
    // Init jika belum
    if (!isInitialized) {
      initRooms();
    }
    
    const url = new URL(request.url);
    const path = url.pathname;

    // ========== HTTP ENDPOINTS ==========
    if (path === '/api/rooms') {
      const counts = {};
      for (const [room, roomMan] of rooms) {
        counts[room] = roomMan.getCount();
      }
      return new Response(JSON.stringify({ 
        rooms: counts, 
        number: currentNumber,
        connections: globalWSSet.size,
        tikCounter: tikCounter
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (path === '/api/stats') {
      return new Response(JSON.stringify({
        connections: globalWSSet.size,
        rooms: rooms.size,
        maxSeats: C.MAX_SEATS,
        globalNumber: currentNumber,
        tikCounter: tikCounter,
        userConnections: userConnections.size
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // ========== WEBSOCKET ==========
    const upgrade = request.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Chat Server - Pure Worker (No KV)", { 
        status: 200,
        headers: { "Cache-Control": "no-cache" }
      });
    }

    // Check global connections limit
    if (globalWSSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }

    // Create WebSocket pair
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];

    // Accept WebSocket
    ctx.acceptWebSocket(server);

    // Initialize connection
    server.wsId = crypto.randomUUID();
    server.username = null;
    server.room = null;
    server.seat = null;
    server.isMulti = false;
    server._closing = false;
    server._lastPing = Date.now();

    // Add to global set
    globalWSSet.add(server);

    // ========== WEB SOCKET HANDLERS ==========
    server.addEventListener('message', async (event) => {
      if (server._closing) return;
      try {
        await handleMessage(server, event.data);
      } catch (e) {
        console.error('Message handler error:', e);
      }
    });

    server.addEventListener('close', () => {
      cleanup(server);
    });

    server.addEventListener('error', () => {
      cleanup(server);
    });

    // ========== PING TIMER (30 DETIK) ==========
    const pingInterval = setInterval(() => {
      if (server._closing || server.readyState !== 1) {
        clearInterval(pingInterval);
        return;
      }
      try {
        server.send(JSON.stringify(["ping", Date.now()]));
        server._lastPing = Date.now();
      } catch (e) {
        clearInterval(pingInterval);
        cleanup(server);
      }
    }, C.WS_PING_INTERVAL);

    server._pingInterval = pingInterval;

    return new Response(null, { status: 101, webSocket: client });
  },

  // ========== CLEANUP ON SHUTDOWN ==========
  async cleanup() {
    if (globalTimer) {
      clearInterval(globalTimer);
      globalTimer = null;
    }
    if (cleanupTimer) {
      clearInterval(cleanupTimer);
      cleanupTimer = null;
    }
    
    // Tutup semua koneksi
    for (const ws of globalWSSet) {
      try {
        if (ws.readyState === 1) {
          ws.close(1000, "Worker shutting down");
        }
      } catch(e) {}
    }
    
    globalWSSet.clear();
    userConnections.clear();
    userSeat.clear();
    userRoom.clear();
    wsActiveMulti.clear();
    rooms.clear();
  }
};

// ==================== MESSAGE HANDLER ====================
async function handleMessage(ws, raw) {
  if (!ws || ws._closing) return;

  try {
    let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
    if (str.length > C.MAX_MESSAGE_SIZE) return;

    let data;
    try { data = JSON.parse(str); } catch(e) { return; }
    if (!Array.isArray(data) || !data.length) return;

    const [evt, ...args] = data;

    // ========== ROUTE EVENTS ==========
    switch(evt) {
      case "setIdTarget2":
        handleSetId(ws, args[0], args[1]);
        break;

      case "joinRoom":
        handleJoin(ws, args[0]);
        break;

      case "multiJoin":
        handleMultiJoin(ws, args[0], args[1]);
        break;

      case "exitMulti":
        handleExitMulti(ws, args[0]);
        break;

      case "setActiveMulti":
        handleSetActiveMulti(ws, args[0]);
        break;

      case "updateKursi":
        handleUpdateKursi(ws, args);
        break;

      case "chat":
        handleChat(ws, args);
        break;

      case "updatePoint":
        handleUpdatePoint(ws, args);
        break;

      case "removeKursiAndPoint":
        handleRemoveKursi(ws, args);
        break;

      case "private":
        handlePrivate(ws, args);
        break;

      case "gift":
        handleGift(ws, args);
        break;

      case "rollangak":
        handleRoll(ws, args);
        break;

      case "sendnotif":
        handleNotif(ws, args);
        break;

      case "getCurrentNumber":
        safeSend(ws, ["currentNumber", currentNumber]);
        break;

      case "isUserOnline":
        handleIsUserOnline(ws, args);
        break;

      case "getOnlineUsers":
        handleGetOnlineUsers(ws);
        break;

      case "getAllRoomsUserCount":
        handleAllRoomsCount(ws);
        break;

      case "getRoomUserCount":
        handleRoomCount(ws, args[0]);
        break;

      case "setMuteType":
        handleSetMute(ws, args);
        break;

      case "modwarning":
        if (args[0] && ROOMS_SET.has(args[0])) {
          const roomMan = rooms.get(args[0]);
          if (roomMan) {
            roomMan.broadcast(JSON.stringify(["modwarning", args[0]]));
          }
        }
        break;

      case "getMuteType":
        handleGetMute(ws, args[0]);
        break;

      case "onDestroy":
        cleanup(ws);
        break;

      case "pong":
        ws._lastPing = Date.now();
        break;

      default:
        safeSend(ws, ["error", `Unknown event: ${evt}`]);
        break;
    }
  } catch (e) {
    console.error('Handle message error:', e);
  }
}

// ==================== HANDLE SET ID ====================
function handleSetId(ws, username, isNewUser) {
  if (!ws || !username) return;
  
  // Cek multi user
  const seatInfo = userSeat.get(username);
  if (seatInfo && seatInfo.isMulti && isNewUser === false) {
    ws.username = username;
    ws.isMulti = true;
    ws.room = seatInfo.room;
    ws.seat = seatInfo.seat;
    safeSend(ws, ["multiUserActive", username]);
    return;
  }

  // Hapus koneksi lama
  const oldConns = userConnections.get(username);
  if (oldConns) {
    const toRemove = [];
    for (const conn of oldConns) {
      if (!conn || conn.readyState !== 1) {
        toRemove.push(conn);
      }
    }
    for (const conn of toRemove) {
      oldConns.delete(conn);
      globalWSSet.delete(conn);
      wsActiveMulti.delete(conn);
    }
  }

  // Hapus user dari room sebelumnya
  if (!seatInfo || !seatInfo.isMulti) {
    if (seatInfo) {
      const roomMan = rooms.get(seatInfo.room);
      if (roomMan && seatInfo.seat) {
        roomMan.removeSeat(seatInfo.seat);
        roomMan.broadcast(JSON.stringify(["removeKursi", seatInfo.room, seatInfo.seat]));
        roomMan.broadcast(JSON.stringify(["roomUserCount", seatInfo.room, roomMan.getCount()]));
      }
    }
    userSeat.delete(username);
    userRoom.delete(username);
  }

  // Set username
  ws.username = username;
  ws.isMulti = false;

  // Simpan koneksi
  let conns = userConnections.get(username);
  if (!conns) {
    conns = new Set();
    userConnections.set(username, conns);
  }
  conns.add(ws);

  if (isNewUser) {
    safeSend(ws, ["joinroomawal"]);
  } else {
    safeSend(ws, ["needJoinRoom"]);
  }
}

// ==================== HANDLE JOIN ====================
function handleJoin(ws, roomName) {
  if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName)) {
    return false;
  }

  const username = ws.username;
  const roomMan = rooms.get(roomName);
  if (!roomMan) return false;

  // Cek apakah user sudah di room ini
  let seat = null;
  for (const [s, data] of roomMan.seats) {
    if (data.namauser === username) {
      seat = s;
      break;
    }
  }

  // Cari kursi kosong
  if (!seat) {
    if (roomMan.getCount() >= C.MAX_SEATS) {
      safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    seat = roomMan.getAvailableSeat();
    if (!seat) {
      safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    roomMan.addSeat(username, "", "", 0, 0, 0, 0);
  }

  // Hapus dari room sebelumnya
  const oldRoom = ws.room;
  if (oldRoom && oldRoom !== roomName) {
    const oldRoomMan = rooms.get(oldRoom);
    if (oldRoomMan) {
      const oldSeat = userSeat.get(username)?.seat;
      if (oldSeat) {
        oldRoomMan.removeSeat(oldSeat);
        oldRoomMan.broadcast(JSON.stringify(["removeKursi", oldRoom, oldSeat]));
        oldRoomMan.broadcast(JSON.stringify(["roomUserCount", oldRoom, oldRoomMan.getCount()]));
      }
      oldRoomMan.removeClient(ws);
    }
    userSeat.delete(username);
    userRoom.delete(username);
  }

  // Update state
  ws.room = roomName;
  ws.seat = seat;
  ws.isMulti = false;

  userSeat.set(username, { room: roomName, seat, isMulti: false });
  userRoom.set(username, roomName);
  roomMan.addClient(ws);

  // Kirim response
  safeSend(ws, ["rooMasuk", seat, roomName]);
  safeSend(ws, ["numberKursiSaya", seat]);
  safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
  safeSend(ws, ["currentNumber", currentNumber]);
  safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);

  // Broadcast ke room
  roomMan.broadcast(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));

  // Kirim state setelah 1 detik
  setTimeout(() => {
    if (ws && ws.readyState === 1 && !ws._closing) {
      sendAllStateTo(ws, roomName, true);
    }
  }, 1000);

  return true;
}

// ==================== HANDLE MULTI JOIN ====================
function handleMultiJoin(ws, username, roomName) {
  if (!username || !roomName || !ROOMS_SET.has(roomName)) return;

  const roomMan = rooms.get(roomName);
  if (!roomMan) return;

  // Hapus dari room lain
  for (const [room, rm] of rooms) {
    for (const [seat, data] of rm.seats) {
      if (data.namauser === username) {
        rm.removeSeat(seat);
        rm.broadcast(JSON.stringify(["removeKursi", room, seat]));
        rm.broadcast(JSON.stringify(["roomUserCount", room, rm.getCount()]));
        break;
      }
    }
  }

  // Cari kursi
  let seat = null;
  for (let s = 1; s <= C.MAX_SEATS; s++) {
    if (!roomMan.seats.has(s)) {
      seat = s;
      break;
    }
  }
  if (!seat) {
    safeSend(ws, ["roomFull", roomName]);
    return;
  }

  // Tambah kursi
  roomMan.addSeat(username, "", "", 0, 0, 0, 0);

  // Update state
  ws.username = username;
  ws.room = roomName;
  ws.seat = seat;
  ws.isMulti = true;

  userSeat.set(username, { room: roomName, seat, isMulti: true });
  userRoom.set(username, roomName);
  wsActiveMulti.set(ws, { username, room: roomName });
  roomMan.addClient(ws);

  safeSend(ws, ["rooMasukMulti", seat, roomName]);
  roomMan.broadcast(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
}

// ==================== HANDLE EXIT MULTI ====================
function handleExitMulti(ws, username) {
  if (!username) return;

  const seatInfo = userSeat.get(username);
  if (!seatInfo) return;

  const roomName = seatInfo.room;
  const roomMan = rooms.get(roomName);
  if (roomMan && seatInfo.seat) {
    roomMan.removeSeat(seatInfo.seat);
    roomMan.broadcast(JSON.stringify(["removeKursi", roomName, seatInfo.seat]));
    roomMan.broadcast(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
    roomMan.removeClient(ws);
  }

  userSeat.delete(username);
  userRoom.delete(username);
  wsActiveMulti.delete(ws);
  ws.username = null;
  ws.room = null;
  ws.seat = null;
  ws.isMulti = false;
}

// ==================== HANDLE SET ACTIVE MULTI ====================
function handleSetActiveMulti(ws, username) {
  const seatInfo = userSeat.get(username);
  if (!seatInfo) return;

  const roomName = seatInfo.room;
  const roomMan = rooms.get(roomName);
  
  // Hapus dari room sebelumnya
  if (ws.room && ws.room !== roomName) {
    const oldRoomMan = rooms.get(ws.room);
    if (oldRoomMan) {
      oldRoomMan.removeClient(ws);
    }
  }

  ws.username = username;
  ws.room = roomName;
  ws.seat = seatInfo.seat;
  ws.isMulti = true;

  wsActiveMulti.set(ws, { username, room: roomName });
  roomMan.addClient(ws);

  safeSend(ws, ["activeChangedMulti", username, seatInfo.seat, roomName]);
  roomMan.broadcast(JSON.stringify(["userActiveChanged", username, seatInfo.seat]));
}

// ==================== HANDLE UPDATE KURSI ====================
function handleUpdateKursi(ws, args) {
  try {
    const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
    const roomMan = rooms.get(kursiRoom);
    if (!roomMan) return;

    const updated = roomMan.updateSeat(kursiSeat, {
      noimageUrl: kursiNoimg || "",
      namauser: kursiName || "",
      color: kursiColor || "",
      itembawah: kursiBawah || 0,
      itematas: kursiAtas || 0,
      vip: kursiVip || 0,
      viptanda: kursiVt || 0
    });

    if (updated) {
      const updatedSeat = roomMan.getSeat(kursiSeat);
      roomMan.broadcast(JSON.stringify(["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]));
    }
  } catch(e) {
    console.error('Update kursi error:', e);
  }
}

// ==================== HANDLE CHAT ====================
function handleChat(ws, args) {
  try {
    const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
    
    if (!chatMsg || !ROOMS_SET.has(chatRoom)) return;
    
    const roomMan = rooms.get(chatRoom);
    if (!roomMan) return;
    
    if (roomMan.getMuted()) {
      safeSend(ws, ["chatError", "Room is muted"]);
      return;
    }
    
    roomMan.broadcast(JSON.stringify(["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]));
  } catch(e) {
    console.error('Chat error:', e);
  }
}

// ==================== HANDLE UPDATE POINT ====================
function handleUpdatePoint(ws, args) {
  try {
    const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
    if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
      const roomMan = rooms.get(pointRoom);
      if (roomMan && roomMan.seats.has(pointSeat)) {
        if (roomMan.updatePoint(pointSeat, pointX, pointY, pointFast === 1)) {
          roomMan.broadcast(JSON.stringify(["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]));
        }
      }
    }
  } catch(e) {
    console.error('Update point error:', e);
  }
}

// ==================== HANDLE REMOVE KURSI ====================
function handleRemoveKursi(ws, args) {
  try {
    const [removeRoom, removeSeat] = args;
    const roomMan = rooms.get(removeRoom);
    if (roomMan && roomMan.seats.has(removeSeat)) {
      // Hapus dari userSeat
      for (const [username, info] of userSeat) {
        if (info.seat === removeSeat && info.room === removeRoom) {
          userSeat.delete(username);
          userRoom.delete(username);
          break;
        }
      }
      roomMan.removeSeat(removeSeat);
      roomMan.broadcast(JSON.stringify(["removeKursi", removeRoom, removeSeat]));
      roomMan.broadcast(JSON.stringify(["roomUserCount", removeRoom, roomMan.getCount()]));
    }
  } catch(e) {
    console.error('Remove kursi error:', e);
  }
}

// ==================== HANDLE PRIVATE ====================
function handlePrivate(ws, args) {
  try {
    const [privTarget, privNoimg, privMsg, privSender] = args;
    if (privTarget && privMsg) {
      const targetConns = userConnections.get(privTarget);
      if (targetConns) {
        for (const targetWs of targetConns) {
          if (targetWs?.readyState === 1) {
            safeSend(targetWs, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
            break;
          }
        }
      }
      safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
    }
  } catch(e) {
    console.error('Private error:', e);
  }
}

// ==================== HANDLE GIFT ====================
function handleGift(ws, args) {
  try {
    const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
    if (giftRoom && ROOMS_SET.has(giftRoom)) {
      const roomMan = rooms.get(giftRoom);
      if (roomMan) {
        roomMan.broadcast(JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
      }
    }
  } catch(e) {
    console.error('Gift error:', e);
  }
}

// ==================== HANDLE ROLL ====================
function handleRoll(ws, args) {
  try {
    const [rollRoom, rollUser, rollAngka] = args;
    if (rollRoom && ROOMS_SET.has(rollRoom)) {
      const roomMan = rooms.get(rollRoom);
      if (roomMan) {
        roomMan.broadcast(JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
      }
    }
  } catch(e) {
    console.error('Roll error:', e);
  }
}

// ==================== HANDLE NOTIF ====================
function handleNotif(ws, args) {
  try {
    const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
    if (notifTarget && notifMsg) {
      const targetConns = userConnections.get(notifTarget);
      if (targetConns) {
        for (const c of targetConns) {
          if (c?.readyState === 1) {
            safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
            break;
          }
        }
      }
    }
  } catch(e) {
    console.error('Notif error:', e);
  }
}

// ==================== HANDLE USER ONLINE ====================
function handleIsUserOnline(ws, args) {
  try {
    const [onlineTarget, onlineCallback] = args;
    let isOnline = false;
    const seatInfo = userSeat.get(onlineTarget);
    if (seatInfo?.seat) {
      if (seatInfo.isMulti) {
        isOnline = true;
      } else {
        const connections = userConnections.get(onlineTarget);
        if (connections) {
          for (const conn of connections) {
            if (conn?.readyState === 1) { isOnline = true; break; }
          }
        }
      }
    }
    safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
  } catch(e) {
    console.error('User online error:', e);
  }
}

// ==================== HANDLE GET ONLINE USERS ====================
function handleGetOnlineUsers(ws) {
  try {
    const users = [];
    for (const [username, seatInfo] of userSeat) {
      if (seatInfo?.seat) {
        if (seatInfo.isMulti) {
          users.push(username);
        } else {
          const connections = userConnections.get(username);
          if (connections) {
            for (const conn of connections) {
              if (conn?.readyState === 1) { users.push(username); break; }
            }
          }
        }
      }
    }
    safeSend(ws, ["allOnlineUsers", users]);
  } catch(e) {
    console.error('Get online users error:', e);
  }
}

// ==================== HANDLE ALL ROOMS COUNT ====================
function handleAllRoomsCount(ws) {
  try {
    const counts = {};
    for (const [room, roomMan] of rooms) {
      counts[room] = roomMan.getCount();
    }
    safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
  } catch(e) {
    console.error('All rooms count error:', e);
  }
}

// ==================== HANDLE ROOM COUNT ====================
function handleRoomCount(ws, roomName) {
  try {
    if (roomName && ROOMS_SET.has(roomName)) {
      const roomMan = rooms.get(roomName);
      safeSend(ws, ["roomUserCount", roomName, roomMan?.getCount() || 0]);
    }
  } catch(e) {
    console.error('Room count error:', e);
  }
}

// ==================== HANDLE SET MUTE ====================
function handleSetMute(ws, args) {
  try {
    const [muteVal, muteRoom] = args;
    if (!muteRoom || !ROOMS_SET.has(muteRoom)) return;
    
    const roomMan = rooms.get(muteRoom);
    if (!roomMan) return;
    
    roomMan.setMuted(muteVal);
    roomMan.broadcast(JSON.stringify(["muteStatusChanged", !!muteVal, muteRoom]));
    safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
  } catch(e) {
    console.error('Set mute error:', e);
  }
}

// ==================== HANDLE GET MUTE ====================
function handleGetMute(ws, muteRoom) {
  try {
    if (muteRoom && ROOMS_SET.has(muteRoom)) {
      const roomMan = rooms.get(muteRoom);
      safeSend(ws, ["muteTypeResponse", roomMan?.getMuted() || false, muteRoom]);
    }
  } catch(e) {
    console.error('Get mute error:', e);
  }
}

// ==================== SEND ALL STATE ====================
function sendAllStateTo(ws, room, excludeSelf = false) {
  if (!ws || !ws.username || ws.readyState !== 1) return;

  const roomMan = rooms.get(room);
  if (!roomMan) return;

  try {
    const allSeats = roomMan.getAllSeats();
    const allPoints = roomMan.getAllPoints();
    const selfSeat = ws.seat;

    safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
    safeSend(ws, ["currentNumber", currentNumber]);

    if (allSeats && Object.keys(allSeats).length > 0) {
      if (excludeSelf && selfSeat && allSeats[selfSeat]) {
        const filtered = { ...allSeats };
        delete filtered[selfSeat];
        if (Object.keys(filtered).length > 0) {
          safeSend(ws, ["allUpdateKursiList", room, filtered]);
        }
      } else {
        safeSend(ws, ["allUpdateKursiList", room, allSeats]);
      }
    }

    if (allPoints?.length > 0) {
      let filteredPoints = allPoints;
      if (excludeSelf && selfSeat) {
        filteredPoints = allPoints.filter(p => p.seat !== selfSeat);
      }
      if (filteredPoints.length > 0) {
        safeSend(ws, ["allPointsList", room, filteredPoints]);
      }
    }
  } catch(e) {
    console.error('Send all state error:', e);
  }
}

// ==================== SAFE SEND ====================
function safeSend(ws, msg) {
  if (!ws) return false;
  try {
    if (ws.readyState === 1 && !ws._closing) {
      ws.send(JSON.stringify(msg));
      return true;
    }
  } catch(e) {
    console.error('Send error:', e);
  }
  return false;
}
