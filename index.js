import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

function createEmptySeat() {
  return {
    noimageUrl: "",
    namauser: "",
    color: "",
    itembawah: 0,
    itematas: 0,
    vip: 0,
    viptanda: 0,
    points: [],
    lockTime: undefined
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Map();
    this.userToSeat = new Map();
    this.userSeatMap = new Map(); // Cache untuk seat user

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    
    // Tetap 100ms tapi dengan optimasi priority-based
    this._flushTimer = setInterval(() => this.priorityFlush(), 100);
    this.cycleCount = 0;

    this.lowcard = new LowCardGameManager(this);

    this.offlineUsers = new Map();
    this.offlineTimers = new Map();
    this.lastActivity = new Map();
    this.pingTimeouts = new Map();
    
    this.OFFLINE_TIMEOUT_MS = 30 * 1000;
    this.PING_TIMEOUT_MS = 30 * 1000;
    this.HEARTBEAT_INTERVAL = 15 * 1000;
    
    this.userConnections = new Map();
    
    // Optimasi heartbeat
    this.heartbeatIndex = 0;
    this.heartbeatUserIds = [];
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
        }
        return true;
      }
    } catch (e) {
      this.cleanupClient(ws);
    }
    return false;
  }

  sendPingToClient(ws) {
    if (ws.idtarget && ws.readyState === 1) {
      const pingSent = this.safeSend(ws, ["ping", ws.idtarget]);
      if (pingSent) {
        this.setPingTimeout(ws.idtarget, ws);
      }
    }
  }

  setPingTimeout(userId, ws) {
    if (this.pingTimeouts.has(userId)) {
      clearTimeout(this.pingTimeouts.get(userId));
    }

    const timeoutId = setTimeout(() => {
      const currentWs = this.getUserPrimaryConnection(userId);
      if (currentWs === ws) {
        this.handlePingTimeout(ws);
      }
      this.pingTimeouts.delete(userId);
    }, this.PING_TIMEOUT_MS);

    this.pingTimeouts.set(userId, timeoutId);
  }

  handlePingTimeout(ws) {
    const userId = ws.idtarget;
    if (!userId) return;
    
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(["needReconnect", "Ping timeout - please reconnect"]));
      }
    } catch (e) {}

    this.cleanupClient(ws);
  }

  heartbeat() {
    const now = Date.now();
    
    // Refresh user list setiap 10 detik
    if (this.heartbeatIndex === 0 || now % 10000 < 100) {
      this.heartbeatUserIds = Array.from(this.userConnections.keys());
    }
    
    const batchSize = 8; // Process 8 users per cycle
    let processed = 0;
    
    while (processed < batchSize && this.heartbeatIndex < this.heartbeatUserIds.length) {
      const userId = this.heartbeatUserIds[this.heartbeatIndex];
      const primaryWs = this.getUserPrimaryConnection(userId);
      
      if (primaryWs && primaryWs.readyState === 1) {
        const lastActive = this.lastActivity.get(userId) || 0;
        if (now - lastActive >= this.HEARTBEAT_INTERVAL) {
          this.sendPingToClient(primaryWs);
        }
      }
      
      this.heartbeatIndex++;
      processed++;
    }
    
    // Reset index jika sudah sampai akhir
    if (this.heartbeatIndex >= this.heartbeatUserIds.length) {
      this.heartbeatIndex = 0;
    }
  }

  broadcastToRoom(room, msg) {
    for (const [wsId, ws] of this.clients) {
      if (ws.roomname === room) this.safeSend(ws, msg);
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length === 0) continue;
      
      // Batch messages - max 8 pesan per flush
      const batchSize = 8;
      const toSend = messages.splice(0, batchSize);
      
      for (const msg of toSend) {
        this.broadcastToRoom(room, msg);
      }
      
      // Jika masih banyak message, hapus yang lama
      if (messages.length > 50) {
        messages.splice(0, messages.length - 30);
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      if (seatMapUpdates.size === 0) continue;
      
      // Batch updates - max 12 kursi per flush
      const updates = [];
      let count = 0;
      for (let seat = 1; seat <= this.MAX_SEATS && count < 12; seat++) {
        if (!seatMapUpdates.has(seat)) continue;
        const info = seatMapUpdates.get(seat);
        const { points, ...rest } = info;
        updates.push([seat, rest]);
        seatMapUpdates.delete(seat);
        count++;
      }
      
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
    }
  }

  priorityFlush() {
    const startTime = Date.now();
    const TIME_BUDGET = 8; // ms - jangan gunakan lebih dari 8ms per cycle
    
    // 1. FLUSH CHAT (Highest Priority - selalu proses)
    if (this.chatMessageBuffer.size > 0) {
      this.flushChatBuffer();
    }
    
    // 2. FLUSH KURSI (High Priority - proses jika ada waktu)
    if (Date.now() - startTime < TIME_BUDGET && this.updateKursiBuffer.size > 0) {
      this.flushKursiUpdates();
    }
    
    // 3. PRIVATE MESSAGES (High Priority - proses batch kecil)
    if (Date.now() - startTime < TIME_BUDGET) {
      this.flushPrivateMessages();
    }
    
    // 4. CLEANUP (Medium Priority - proses setiap 3 cycle)
    if (this.cycleCount % 3 === 0 && Date.now() - startTime < TIME_BUDGET) {
      this.cleanExpiredLocks();
    }
    
    // 5. HEARTBEAT (Low Priority - proses setiap 5 cycle)  
    if (this.cycleCount % 5 === 0 && Date.now() - startTime < TIME_BUDGET) {
      this.heartbeat();
    }
    
    // 6. INACTIVE USERS (Low Priority - proses setiap 10 cycle)
    if (this.cycleCount % 10 === 0 && Date.now() - startTime < TIME_BUDGET) {
      this.checkInactiveUsers();
    }
    
    // 7. OFFLINE USERS (Low Priority - proses setiap 10 cycle)
    if (this.cycleCount % 10 === 0 && Date.now() - startTime < TIME_BUDGET) {
      this.checkOfflineUsers();
    }
    
    this.cycleCount = (this.cycleCount + 1) % 30;
  }

  flushPrivateMessages() {
    let processed = 0;
    const maxProcessPerCycle = 15;
    
    for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
      if (processed >= maxProcessPerCycle) break;
      
      const primaryWs = this.getUserPrimaryConnection(id);
      if (primaryWs) {
        // Limit messages sent per cycle
        const messagesToSend = msgs.splice(0, 6);
        for (const m of messagesToSend) {
          this.safeSend(primaryWs, m);
        }
        if (msgs.length === 0) {
          this.privateMessageBuffer.delete(id);
        }
        processed++;
      }
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const [wsId, ws] of this.clients) this.safeSend(ws, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    let cleaned = 0;
    const maxCleanPerCycle = 10;
    
    for (const room of roomList) {
      if (cleaned >= maxCleanPerCycle) break;
      
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (cleaned >= maxCleanPerCycle) break;
        
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          cleaned++;
        }
      }
    }
  }

  checkInactiveUsers() {
    const now = Date.now();
    const toRemove = [];
    let checked = 0;
    const maxCheckPerCycle = 20;

    for (const [id, lastActive] of this.lastActivity.entries()) {
      if (checked >= maxCheckPerCycle) break;
      
      if (now - lastActive >= this.PING_TIMEOUT_MS + 5000) {
        toRemove.push(id);
      }
      checked++;
    }

    for (const id of toRemove.slice(0, 5)) { // Max 5 removal per cycle
      this.forceUserReconnect(id);
    }
  }

  forceUserReconnect(userId) {
    const connections = this.userConnections.get(userId);
    if (connections) {
      for (const ws of connections) {
        try {
          if (ws.readyState === 1) {
            ws.send(JSON.stringify(["needReconnect", "Inactive - please reconnect"]));
          }
        } catch (e) {}
        this.cleanupClient(ws);
      }
    }

    this.offlineUsers.delete(userId);
    this.offlineTimers.delete(userId);
    this.lastActivity.delete(userId);
    this.pingTimeouts.delete(userId);
    this.removeAllSeatsById(userId);
  }

  checkOfflineUsers() {
    const now = Date.now();
    const toRemove = [];
    let checked = 0;
    const maxCheckPerCycle = 15;
    
    for (const [id, saved] of this.offlineUsers.entries()) {
      if (checked >= maxCheckPerCycle) break;
      
      if (now - saved.timestamp >= this.OFFLINE_TIMEOUT_MS) {
        toRemove.push(id);
      }
      checked++;
    }

    for (const id of toRemove.slice(0, 5)) { // Max 5 removal per cycle
      this.offlineUsers.delete(id);
      this.offlineTimers.delete(id);
      this.lastActivity.delete(id);
      this.pingTimeouts.delete(id);
      this.removeAllSeatsById(id);
    }
  }

  getUserPrimaryConnection(userId) {
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return null;
    return Array.from(connections).pop();
  }

  addUserConnection(userId, ws) {
    if (!this.userConnections.has(userId)) {
      this.userConnections.set(userId, new Set());
    }
    this.userConnections.get(userId).add(ws);
  }

  removeUserConnection(userId, ws) {
    const connections = this.userConnections.get(userId);
    if (connections) {
      connections.delete(ws);
      if (connections.size === 0) {
        this.userConnections.delete(userId);
      }
    }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    // Clean expired locks dengan limit
    let cleaned = 0;
    const maxCleanPerCall = 8;
    for (const [seat, info] of seatMap) {
      if (cleaned >= maxCleanPerCall) break;
      
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
        Object.assign(info, createEmptySeat());
        cleaned++;
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (!k) continue;
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = now;
        
        // Update cache
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        if (!this.userSeatMap.has(ws.idtarget)) {
          this.userSeatMap.set(ws.idtarget, []);
        }
        this.userSeatMap.get(ws.idtarget).push({ room, seat: i });
        
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};
    
    // Batasi jumlah points yang dikirim
    let pointCount = 0;
    const maxPointsPerSeat = 50;
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      
      // Batasi points yang dikirim
      const pointsToSend = info.points.slice(-maxPointsPerSeat);
      for (const p of pointsToSend) {
        allPoints.push({ seat, ...p });
        pointCount++;
        if (pointCount > 500) break; // Max total points
      }
      
      if (pointCount > 500) break;
      
      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
        meta[seat] = {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: info.vip,
          viptanda: info.viptanda
        };
      }
    }
    
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, meta]);
  }

  cleanupClientById(idtarget) {
    const connections = this.userConnections.get(idtarget);
    if (connections) {
      for (const ws of Array.from(connections)) {
        try {
          if (ws.readyState === 1) {
            ws.close(4000, "Duplicate connection cleanup");
          }
        } catch (e) {}
        this.cleanupClient(ws);
      }
    }
  }

  removeAllSeatsById(idtarget) {
    const userSeats = this.userSeatMap.get(idtarget);
    if (!userSeats) return false;
    
    let removedAny = false;
    const affectedRooms = new Set();
    
    for (const {room, seat} of userSeats) {
      const seatMap = this.roomSeats.get(room);
      if (seatMap && seatMap.has(seat)) {
        const info = seatMap.get(seat);
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          affectedRooms.add(room);
          removedAny = true;
        }
      }
    }
    
    this.userSeatMap.delete(idtarget);
    this.userToSeat.delete(idtarget);
    
    // Broadcast counts hanya untuk room yang berubah
    for (const room of affectedRooms) {
      this.broadcastRoomUserCount(room);
    }
    
    return removedAny;
  }

  getAllOnlineUsers() {
    return Array.from(this.userConnections.keys());
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    let count = 0;
    const maxUsersPerCall = 100;
    
    for (const [userId, connections] of this.userConnections) {
      if (count >= maxUsersPerCall) break;
      
      const primaryWs = this.getUserPrimaryConnection(userId);
      if (primaryWs && primaryWs.roomname === roomName) {
        users.push(userId);
        count++;
      }
    }
    return users;
  }

  scheduleOfflineRemoval(idtarget) {
    if (this.offlineTimers.has(idtarget)) {
      clearTimeout(this.offlineTimers.get(idtarget));
    }
    
    const timeoutId = setTimeout(() => {
      if (this.offlineUsers.has(idtarget)) {
        this.offlineUsers.delete(idtarget);
        this.removeAllSeatsById(idtarget);
        this.lastActivity.delete(idtarget);
        this.pingTimeouts.delete(idtarget);
      }
      this.offlineTimers.delete(idtarget);
    }, this.OFFLINE_TIMEOUT_MS);
    
    this.offlineTimers.set(idtarget, timeoutId);
  }

  cancelOfflineRemoval(idtarget) {
    if (this.offlineTimers.has(idtarget)) {
      clearTimeout(this.offlineTimers.get(idtarget));
      this.offlineTimers.delete(idtarget);
    }
    if (this.offlineUsers.has(idtarget)) {
      this.offlineUsers.delete(idtarget);
    }
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
  }

  cleanupClient(ws) {
    if (!ws) return;
    
    const id = ws.idtarget;
    const wsId = ws._id;
    
    if (id) {
      this.removeUserConnection(id, ws);
    }
    
    this.clients.delete(wsId);
    
    if (id) {
      const remainingConnections = this.userConnections.get(id);
      if (!remainingConnections || remainingConnections.size === 0) {
        if (ws.roomname) {
          this.offlineUsers.set(id, {
            roomname: ws.roomname,
            seats: ws.numkursi ? Array.from(ws.numkursi) : [],
            timestamp: Date.now()
          });
          this.scheduleOfflineRemoval(id);
        }
        
        this.lastActivity.delete(id);
        this.pingTimeouts.delete(id);
      }
    }
    
    if (ws.numkursi) {
      ws.numkursi.clear();
    }
    
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  cleanupondestroy(ws) {
    if (ws.idtarget) {
      const userId = ws.idtarget;
      
      // Hentikan ping timeout
      if (this.pingTimeouts.has(userId)) {
        clearTimeout(this.pingTimeouts.get(userId));
        this.pingTimeouts.delete(userId);
      }
      
      // Hentikan offline timer
      if (this.offlineTimers.has(userId)) {
        clearTimeout(this.offlineTimers.get(userId));
        this.offlineTimers.delete(userId);
      }
      
      // Hapus dari semua tracking
      this.offlineUsers.delete(userId);
      this.lastActivity.delete(userId);
      this.userConnections.delete(userId);
      this.userToSeat.delete(userId);
      this.userSeatMap.delete(userId);
      
      // Hapus semua seat yang dimiliki user ini
      this.removeAllSeatsById(userId);
    }
    
    // Hapus dari clients map
    if (ws._id) {
      this.clients.delete(ws._id);
    }
    
    // Clear semua data WebSocket
    if (ws.numkursi) {
      ws.numkursi.clear();
    }
    
    // Reset properties WebSocket
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws._id = undefined;
    
    // Close WebSocket connection jika masih terbuka
    try {
      if (ws.readyState === 1) {
        ws.close(1000, "Cleanup on destroy");
      }
    } catch (e) {}
  }

  handleMessage(ws, raw) {
    if (!ws._id) {
      ws._id = Math.random().toString(36).substring(2, 15);
      this.clients.set(ws._id, ws);
    }
    
    if (ws.idtarget) {
      this.lastActivity.set(ws.idtarget, Date.now());
    }

    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
      case "onDestroy": {
        this.cleanupondestroy(ws);
        break;
      }
      
     case "setIdTarget": {
  const newId = data[1];
  
  const existingConnections = this.userConnections.get(newId);
  if (existingConnections) {
    for (const oldWs of Array.from(existingConnections)) {
      if (oldWs !== ws && oldWs.readyState === 1) {
        try {
          oldWs.close(4000, "New connection established");
        } catch (e) {}
        this.cleanupClient(oldWs);
      }
    }
  }
  
  ws.idtarget = newId;
  this.addUserConnection(newId, ws);
  this.lastActivity.set(newId, Date.now());

  // Kirim private messages yang tertahan
  if (this.privateMessageBuffer.has(newId)) {
    for (const msg of this.privateMessageBuffer.get(newId)) this.safeSend(ws, msg);
    this.privateMessageBuffer.delete(newId);
  }

  const offline = this.offlineUsers.get(newId);
  if (offline) {
    // User yang ada di offlineUsers dapat needJoinRoom
    const { roomname, seats } = offline;
    
    // Bersihkan data offline
    this.offlineUsers.delete(newId);
    this.cancelOfflineRemoval(newId);
    
    // Hapus seat yang sebelumnya
    this.removeAllSeatsById(newId);
    
    // Kirim needJoinRoom
    this.safeSend(ws, ["needJoinRoom", "Please join a room"]);
  } else {
    // User baru juga dapat needJoinRoom
   
  }
  break;
}

      case "pong": {
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
          if (this.pingTimeouts.has(ws.idtarget)) {
            clearTimeout(this.pingTimeouts.get(ws.idtarget));
            this.pingTimeouts.delete(ws.idtarget);
          }
        }
        break;
      }

      case "ping": {
        const idtarget = data[1];
        if (idtarget) {
          this.lastActivity.set(idtarget, Date.now());
        }
        this.safeSend(ws, ["pong"]);
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        const primaryWs = this.getUserPrimaryConnection(idtarget);
        if (primaryWs) {
          this.safeSend(primaryWs, notif);
        } else {
          if (!this.privateMessageBuffer.has(idtarget)) this.privateMessageBuffer.set(idtarget, []);
          this.privateMessageBuffer.get(idtarget).push(notif);
        }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        
        const targetPrimaryWs = this.getUserPrimaryConnection(idt);
        if (targetPrimaryWs) {
          this.safeSend(targetPrimaryWs, out);
        } else {
          if (!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt, []);
          this.privateMessageBuffer.get(idt).push(out);
          this.safeSend(ws, ["privateFailed", idt, "User offline"]);
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";

        const connections = this.userConnections.get(username);
        const online = !!(connections && connections.size > 0);
        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

        if (connections && connections.size > 1) {
          const primaryWs = this.getUserPrimaryConnection(username);
          const connectionsToClose = Array.from(connections).filter(conn => conn !== primaryWs);
          
          for (const oldWs of connectionsToClose) {
            try {
              if (oldWs.readyState === 1) {
                oldWs.close(4000, "Duplicate connection closed");
              }
            } catch (e) {}
            this.cleanupClient(oldWs);
          }
        }
        break;
      }

      case "getAllRoomsUserCount":
        this.handleGetAllRoomsUserCount(ws);
        break;

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;

      case "getAllOnlineUsers":
        this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
        break;

      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return this.safeSend(ws, ["error", "Unknown room"]);
        this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
        break;
      }

      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
        
        if (ws.idtarget) {
          this.removeAllSeatsById(ws.idtarget);
        }
        
        ws.roomname = newRoom;
        const seatMap = this.roomSeats.get(newRoom);
        const foundSeat = this.lockSeat(newRoom, ws);
        
        if (foundSeat === null) {
          this.safeSend(ws, ["roomFull", newRoom]);
          return;
        }
        
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        
        if (ws.idtarget) {
          this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        }
        
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);
        
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
          this.cancelOfflineRemoval(ws.idtarget);
        }
        break;
      }

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        
        const buffer = this.chatMessageBuffer.get(roomname);
        if (buffer.length < 80) { // Limit buffer size
          buffer.push([
            "chat", roomname, noImageURL, username, message, usernameColor, chatTextColor
          ]);
        }
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.points.push({ x, y, fast });
        if (si.points.length > 150) si.points.shift(); // Kurangi dari 200 ke 150
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());
        for (const [wsId, c] of this.clients) c.numkursi?.delete(seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
        seatMap.set(seat, currentInfo);
        if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, points: [] });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for gift"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        
        const buffer = this.chatMessageBuffer.get(roomname);
        if (buffer.length < 80) { // Limit buffer size
          buffer.push([
            "gift", roomname, sender, receiver, giftName, Date.now()
          ]);
        }
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws._id = Math.random().toString(36).substring(2, 15);
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    
    this.clients.set(ws._id, ws);

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
        this.cleanupClient(ws);
      }
    });
    
    ws.addEventListener("close", (ev) => {
      this.cleanupClient(ws);
    });
    
    ws.addEventListener("error", (error) => {
      this.cleanupClient(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
