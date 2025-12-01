import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
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
    lastPoint: null,
    locked: false,
    lockedBy: null
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // Core data structures
    this.clients = new Set();
    this.userToSeat = new Map();        // userId -> {room, seat}
    this.userLastSeen = new Map();      // userId -> last active timestamp
    this.seatProcessingLocks = new Map(); // "room:seat" -> timestamp
    
    // Grace period configuration
    this.GRACE_PERIOD = 30 * 1000;      // 30 detik grace period
    this.SEAT_PROCESSING_TIMEOUT = 3000; // 3 detik untuk proses seat
    
    // Room and seat management
    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    // Managers
    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);

    // Buffers
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.messageCounts = new Map();

    // Connection tracking
    this._nextConnId = 1;
    this.hasEverSetId = false;
    
    // Background number rotation
    this.currentNumber = 1;
    this.maxNumber = 6;

    // ===== TIMERS =====
    // Background number rotation (15 menit)
    this._tickTimer = setInterval(() => this.tick(), 15 * 60 * 1000);
    
    // Flush buffers (100ms - optimal)
    this._flushTimer = setInterval(() => { 
      if (this.clients.size > 0) this.periodicFlush(); 
    }, 100);
    
    // Grace period cleanup (30 detik)
    this._graceCleanupTimer = setInterval(() => { 
      this.cleanupExpiredUsers(); 
    }, 30 * 1000);
    
    // Cleanup processing locks (10 detik)
    this._lockCleanupTimer = setInterval(() => {
      this.cleanupExpiredLocks();
    }, 10 * 1000);
  }

  // ===== GRACE PERIOD MANAGEMENT =====
  updateUserLastSeen(userId) {
    if (userId) {
      this.userLastSeen.set(userId, Date.now());
    }
  }

  isUserInGracePeriod(userId) {
    if (!userId) return false;
    
    const lastSeen = this.userLastSeen.get(userId);
    if (!lastSeen) return false;
    
    return (Date.now() - lastSeen <= this.GRACE_PERIOD);
  }

  cleanupExpiredUsers() {
    const now = Date.now();
    const usersToCleanup = [];
    
    // Cari user yang sudah lewat grace period
    for (const [userId, lastSeen] of this.userLastSeen.entries()) {
      if (now - lastSeen > this.GRACE_PERIOD) {
        usersToCleanup.push(userId);
      }
    }
    
    // Cleanup user yang expired
    for (const userId of usersToCleanup) {
      this.fullRemoveById(userId);
      this.userLastSeen.delete(userId);
    }
  }

  // ===== SEAT LOCKING SYSTEM =====
  acquireSeatLock(room, seat, userId) {
    const lockKey = `${room}:${seat}`;
    const now = Date.now();
    
    // Cek jika kursi sedang diproses
    if (this.seatProcessingLocks.has(lockKey)) {
      const lockTime = this.seatProcessingLocks.get(lockKey);
      if (now - lockTime < this.SEAT_PROCESSING_TIMEOUT) {
        return false; // Masih dalam proses
      }
    }
    
    this.seatProcessingLocks.set(lockKey, now);
    return true;
  }
  
  releaseSeatLock(room, seat) {
    const lockKey = `${room}:${seat}`;
    this.seatProcessingLocks.delete(lockKey);
  }
  
  cleanupExpiredLocks() {
    const now = Date.now();
    const expired = [];
    
    for (const [lockKey, timestamp] of this.seatProcessingLocks.entries()) {
      if (now - timestamp > this.SEAT_PROCESSING_TIMEOUT) {
        expired.push(lockKey);
      }
    }
    
    for (const lockKey of expired) {
      this.seatProcessingLocks.delete(lockKey);
    }
  }

  // ===== SEAT MANAGEMENT =====
  lockSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seatNumber)) return false;
    
    const seatInfo = seatMap.get(seatNumber);
    
    // Hanya lock jika kosong atau sudah milik user ini
    if (seatInfo.namauser && seatInfo.namauser !== userId) {
      return false;
    }
    
    seatInfo.locked = true;
    seatInfo.lockedBy = userId;
    return true;
  }

  unlockSeat(room, seatNumber) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seatNumber)) return false;
    
    const seatInfo = seatMap.get(seatNumber);
    seatInfo.locked = false;
    seatInfo.lockedBy = null;
    return true;
  }

  findEmptySeat(room, ws) {
    if (!ws.idtarget) return null;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    // 1. Cek apakah user sudah memiliki kursi di room ini
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && seatInfo.namauser === ws.idtarget) {
        return i;
      }
    }

    // 2. Cari kursi kosong yang aman
    const availableSeats = [];
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (!seatInfo) continue;
      
      const isEmpty = !seatInfo.namauser || seatInfo.namauser === "";
      const notLocked = !seatInfo.locked || seatInfo.lockedBy === ws.idtarget;
      const lockKey = `${room}:${i}`;
      const notProcessing = !this.seatProcessingLocks.has(lockKey) || 
                           (Date.now() - this.seatProcessingLocks.get(lockKey) > this.SEAT_PROCESSING_TIMEOUT);
      
      if (isEmpty && notLocked && notProcessing) {
        availableSeats.push(i);
      }
    }
    
    if (availableSeats.length === 0) return null;
    
    // Pilih kursi pertama yang tersedia
    const selectedSeat = availableSeats[0];
    
    // Lock kursi ini untuk proses join
    if (this.acquireSeatLock(room, selectedSeat, ws.idtarget)) {
      return selectedSeat;
    }
    
    return null;
  }

  // ===== USER CLEANUP =====
  cleanupUserFromSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    
    const seatInfo = seatMap.get(seatNumber);
    if (seatInfo && seatInfo.namauser === userId) {
      // Hapus VIP badge jika ada
      if (seatInfo.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seatNumber);
      }
      
      // Buka kunci kursi
      this.unlockSeat(room, seatNumber);
      
      // Reset seat data
      Object.assign(seatInfo, createEmptySeat());
      
      // Clear buffer dan broadcast
      this.clearSeatBuffer(room, seatNumber);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.broadcastRoomUserCount(room);
    }
    
    // Hapus dari mapping
    this.userToSeat.delete(userId);
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;
    
    // Hapus dari mapping tapi pertahankan lastSeen untuk grace period
    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    
    // Cleanup VIP badges
    this.vipManager.cleanupUserVipBadges(idtarget);
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;
    
    // Hapus dari grace period tracking
    this.userLastSeen.delete(idtarget);
    
    // Cleanup dari semua kursi
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      for (const [seatNumber, info] of seatMap) {
        if (info.namauser === idtarget) {
          this.unlockSeat(room, seatNumber);
          Object.assign(info, createEmptySeat());
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }
    
    // Cleanup VIP badges
    this.vipManager.cleanupUserVipBadges(idtarget);
    
    // Hapus dari mapping
    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    
    // Close semua connections dengan ID ini
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget) {
        if (c.readyState === 1) {
          c.close(1000, "Session removed");
        }
        this.clients.delete(c);
      }
    }
  }

  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  // ===== MESSAGE HANDLING =====
  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._connId || 'anonymous';
    const windowStart = Math.floor(now / 1000);

    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    const stats = this.messageCounts.get(key);
    if (stats.window !== windowStart) {
      stats.count = 0;
      stats.window = windowStart;
    }

    let limit = 20; // Default limit
    if (messageType === "chat") limit = 50;
    if (messageType === "updatePoint") limit = 100;

    stats.count += 1;
    if (stats.count > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }
    return true;
  }

  safeSend(ws, arr) {
    if (!ws || ws.readyState !== 1) return false;
    
    // Cek bufferedAmount
    if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1_000_000) {
      return false;
    }
    
    try { 
      ws.send(JSON.stringify(arr)); 
      return true; 
    } catch (e) { 
      return false; 
    }
  }

  // ===== BROADCAST =====
  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    
    let sentCount = 0;
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const info of seatMap.values()) {
        if (info.namauser) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    if (!roomList.includes(room)) return;
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  // ===== BUFFER FLUSHING =====
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer.entries()) {
      if (!roomList.includes(room) || !messages || messages.length === 0) continue;
      
      for (const msg of messages) {
        this.broadcastToRoom(room, msg);
      }
      this.chatMessageBuffer.set(room, []);
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer.entries()) {
      if (!roomList.includes(room) || !seatMapUpdates || seatMapUpdates.size === 0) continue;
      
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
  }

  // ===== BACKGROUND NUMBER =====
  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        this.safeSend(c, ["currentNumber", this.currentNumber]);
      }
    }
  }

  // ===== CORE HANDLERS =====
  handleSetIdTarget2(ws, id, baru) {
    if (!id) return;
    
    // Update last seen
    this.updateUserLastSeen(id);
    
    if (baru === true) {
      // User baru
      this.forceUserCleanup(id);
      ws.idtarget = id;
      ws.roomname = undefined;
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      // User existing - coba auto-reconnect
      ws.idtarget = id;
      ws.roomname = undefined;
      
      // Cek grace period
      if (this.isUserInGracePeriod(id)) {
        const existingSeatInfo = this.userToSeat.get(id);
        
        if (existingSeatInfo) {
          const { room, seat } = existingSeatInfo;
          const seatMap = this.roomSeats.get(room);
          const seatInfo = seatMap?.get(seat);
          
          // Validasi seat masih valid
          if (seatInfo && seatInfo.namauser === id) {
            // AUTO-RECONNECT SUKSES
            ws.roomname = room;
          
            this.safeSend(ws, ["currentNumber", this.currentNumber]);
            
            // Kirim state room dengan delay
            setTimeout(() => {
              if (ws.readyState === 1 && ws.roomname === room) {
                this.sendAllStateTo(ws, room);
              }
            }, 100);
            
            // Kirim VIP badges
            this.vipManager.getAllVipBadges(ws, room);
            
            return; // JANGAN kirim needJoinRoom
          }
        }
      }
      
      // Grace period habis atau tidak ada seat yang valid
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  handleJoinRoom(ws, newRoom) {
  if (!roomList.includes(newRoom) || !ws.idtarget || ws.readyState !== 1) {
    return false;
  }

  // Cari kursi kosong
  const foundSeat = this.findEmptySeat(newRoom, ws);
  if (!foundSeat) {
    this.safeSend(ws, ["roomFull", newRoom]);
    return false;
  }

  // Set room dan seat
  ws.roomname = newRoom;
  
  // Update seat info
  const seatMap = this.roomSeats.get(newRoom);
  const seatInfo = seatMap.get(foundSeat);
  if (seatInfo) {
    seatInfo.namauser = ws.idtarget;
    seatInfo.locked = true;
    seatInfo.lockedBy = ws.idtarget;
  }

  // Simpan mapping user -> seat
  this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

  // Kirim konfirmasi
  this.safeSend(ws, ["numberKursiSaya", foundSeat]);
  this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
  this.safeSend(ws, ["currentNumber", this.currentNumber]);

  // Kirim state room
  setTimeout(() => {
    if (ws.readyState === 1 && ws.roomname === newRoom) {
      this.sendAllStateTo(ws, newRoom);
    }
  }, 300);

  // Kirim VIP badges
  this.vipManager.getAllVipBadges(ws, newRoom);
  
  // Broadcast update count
  this.broadcastRoomUserCount(newRoom);

  return true;
}

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1 || ws.roomname !== room) return;
    
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const allKursiMeta = {};
    const lastPointsData = [];

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      
      if (info.namauser) {
        const { lastPoint, ...rest } = info;
        allKursiMeta[seat] = rest;
        
        if (info.lastPoint) {
          lastPointsData.push({
            seat,
            x: info.lastPoint.x,
            y: info.lastPoint.y,
            fast: info.lastPoint.fast
          });
        }
      }
    }

    if (Object.keys(allKursiMeta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }

    if (lastPointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }

    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  // ===== WEB SOCKET HANDLER =====
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    await server.accept();

    const ws = server;
    ws._connId = `conn#${this._nextConnId++}`;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.isManualDestroy = false;
    
    this.clients.add(ws);

    // Message handler dengan update last seen
    ws.addEventListener("message", ev => {
      try {
        if (ws.idtarget) {
          this.updateUserLastSeen(ws.idtarget);
        }
        this.handleMessage(ws, ev.data);
      } catch (e) {
        // Ignore errors
      }
    });

    // Error handler - pertahankan grace period
    ws.addEventListener("error", () => {
      if (ws.idtarget && !ws.isManualDestroy) {
        ws.isManualDestroy = true;
      }
    });

    // Close handler - pertahankan grace period
    ws.addEventListener("close", () => {
      if (ws.idtarget && !ws.isManualDestroy) {
        ws.isManualDestroy = true;
      }
      this.clients.delete(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      return; 
    }
    
    if (!Array.isArray(data) || data.length === 0) return;
    
    const evt = data[0];
    if (!this.checkRateLimit(ws, evt)) return;

    // Update last seen untuk setiap pesan
    this.updateUserLastSeen(ws.idtarget);

    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data);
        break;

      case "isInRoom": {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        this.safeSend(ws, ["inRoomStatus", !!seatInfo]);
        break;
      }

      case "onDestroy":
        break;
        
      case "setIdTarget2":
        this.handleSetIdTarget2(ws, data[1], data[2]);
        break;

      case "setIdTarget": {
        const newId = data[1];
        
        if (ws.idtarget && ws.idtarget !== newId) {
          this.forceUserCleanup(ws.idtarget);
        }
        
        ws.idtarget = newId;
        this.updateUserLastSeen(newId);
        
        const prevSeat = this.userToSeat.get(newId);
        if (prevSeat && this.isUserInGracePeriod(newId)) {
          // Auto-reconnect dalam grace period
          ws.roomname = prevSeat.room;
         
          
          setTimeout(() => {
            if (ws.readyState === 1) {
              this.sendAllStateTo(ws, prevSeat.room);
            }
          }, 100);
        } else if (this.hasEverSetId) {
          this.safeSend(ws, ["needJoinRoom"]);
        }
        
        this.hasEverSetId = true;
        if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        
        for (const c of this.clients) {
          if (c.idtarget === idtarget && c.readyState === 1) {
            this.safeSend(c, notif);
            break;
          }
        }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        
        this.safeSend(ws, out);
        for (const c of this.clients) {
          if (c.idtarget === idt && c.readyState === 1) {
            this.safeSend(c, out);
            break;
          }
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";
        let online = false;
        
        for (const c of this.clients) {
          if (c.idtarget === username && c.readyState === 1) {
            online = true;
            break;
          }
        }
        
        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);
        break;
      }

      case "getAllRoomsUserCount": {
        const result = roomList.map(r => [r, this.getJumlahRoom()[r]]);
        this.safeSend(ws, ["allRoomsUserCount", result]);
        break;
      }

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;

      case "getOnlineUsers": {
        const users = Array.from(this.clients)
          .filter(c => c.idtarget && c.readyState === 1)
          .map(c => c.idtarget);
        this.safeSend(ws, ["allOnlineUsers", users]);
        break;
      }

      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return;
        
        const users = Array.from(this.clients)
          .filter(c => c.roomname === roomName && c.idtarget && c.readyState === 1)
          .map(c => c.idtarget);
        this.safeSend(ws, ["roomOnlineUsers", roomName, users]);
        break;
      }

      case "joinRoom":
        this.handleJoinRoom(ws, data[1]);
        break;

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (ws.roomname !== roomname || !roomList.includes(roomname)) return;
        
        if (!this.chatMessageBuffer.has(roomname)) {
          this.chatMessageBuffer.set(roomname, []);
        }
        
        this.chatMessageBuffer.get(roomname).push([
          "chat", roomname, noImageURL, username, message, usernameColor, chatTextColor
        ]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (ws.roomname !== room || !roomList.includes(room)) return;
        
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        
        si.lastPoint = { x, y, fast, timestamp: Date.now() };
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (ws.roomname !== room || !roomList.includes(room)) return;
        
        Object.assign(this.roomSeats.get(room).get(seat), createEmptySeat());
        this.clearSeatBuffer(room, seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (ws.roomname !== room || !roomList.includes(room)) return;
        
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        
        // Validasi ownership
        if (currentInfo.namauser && currentInfo.namauser !== namauser) {
          this.safeSend(ws, ['error', 'Kursi sudah ditempati user lain']);
          return;
        }
        
        // Update data
        Object.assign(currentInfo, {
          noimageUrl, namauser, color, itembawah, itematas,
          vip: vip || 0,
          viptanda: viptanda || 0
        });
        
        // Auto-lock jika ada user
        if (namauser && namauser !== "") {
          this.lockSeat(room, seat, namauser);
        }
        
        seatMap.set(seat, currentInfo);
        
        // Update buffer
        if (!this.updateKursiBuffer.has(room)) {
          this.updateKursiBuffer.set(room, new Map());
        }
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
        
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (ws.roomname !== roomname || !roomList.includes(roomname)) return;
        
        if (!this.chatMessageBuffer.has(roomname)) {
          this.chatMessageBuffer.set(roomname, []);
        }
        
        this.chatMessageBuffer.get(roomname).push([
          "gift", roomname, sender, receiver, giftName, Date.now()
        ]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (ws.roomname === "LowCard") {
          setTimeout(() => this.lowcard.handleEvent(ws, data), 0);
        }
        break;

      default:
        break;
    }
  }
}

export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    
    return new Response("WebSocket endpoint", { status: 200 });
  }
};


