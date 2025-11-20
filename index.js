// ChatServer Durable Object - FIX KURSI TAMPIL DI ROOM
import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// Constants untuk optimisasi
const MAX_SEATS = 35;
const MAX_MESSAGES_PER_SECOND = 50;
const RECONNECT_TIMEOUT = 20000;
const CLEANUP_INTERVAL = 30000;
const TICK_INTERVAL = 15 * 60 * 1000;
const MAX_POINTS_HISTORY = 5;

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
    lockTime: undefined,
    lastActivity: Date.now()
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // Collections
    this.clients = new Set();
    this.userToSeat = new Map();
    this.hasEverSetId = false;

    // Initialize rooms dan seats
    this.roomSeats = new Map();
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    // Locks dan rate limiting
    this.seatLocks = new Map();
    this.messageCounts = new Map();

    // Game state
    this.currentNumber = 1;
    this.maxNumber = 6;

    // Cleanup collections
    this.pingTimeouts = new Map();
    this.cleanupInProgress = new Set();
    this.usersToRemove = new Map();

    // Game manager
    this.lowcard = new LowCardGameManager(this);

    // Timers
    this._setupTimers();
  }

  _setupTimers() {
    this._tickTimer = setInterval(() => {
      this._safeTick().catch(() => {});
    }, TICK_INTERVAL);

    this._autoRemoveTimer = setInterval(() => {
      if (this.usersToRemove.size > 0 || this.userToSeat.size > 0) {
        this._safeBatchAutoRemove().catch(() => {});
      }
    }, CLEANUP_INTERVAL);
  }

  async destroy() {
    console.log('ChatServer Durable Object destroying...');
    
    const timers = [this._tickTimer, this._autoRemoveTimer];
    for (const timer of timers) {
      if (timer) clearInterval(timer);
    }

    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();

    this._closeAllConnections();

    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      this.lowcard.destroy();
    }
    
    console.log('ChatServer Durable Object destroyed successfully');
  }

  _closeAllConnections() {
    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch (e) {}
    }
    this.clients.clear();
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

    this.usersToRemove.delete(idtarget);
    this._clearPingTimeout(idtarget);
    this._removeFromAllRooms(idtarget);
    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);
    this._removeClientById(idtarget);
  }

  _clearPingTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
  }

  _removeFromAllRooms(idtarget) {
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      let roomUpdated = false;
      for (const [seatNumber, info] of seatMap) {
        const n = info.namauser;
        if (!n) continue;

        if (n === idtarget || n === `__LOCK__${idtarget}`) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          roomUpdated = true;
        }
      }
      if (roomUpdated) {
        this.broadcastRoomUserCount(room);
      }
    }
  }

  _removeClientById(idtarget) {
    for (const client of this.clients) {
      try {
        if (client && client.idtarget === idtarget) {
          try {
            if (client.readyState === 1) {
              client.close(1000, "Session removed");
            }
          } catch (e) {}
          this.clients.delete(client);
        }
      } catch (e) {}
    }
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._id;
    const windowStart = Math.floor(now / 1000);

    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    const stats = this.messageCounts.get(key);
    if (stats.window !== windowStart) {
      stats.count = 0;
      stats.window = windowStart;
    }

    let limit = MAX_MESSAGES_PER_SECOND;
    if (messageType === "chat") limit = 50;
    else if (messageType === "updatePoint") limit = 100;

    if (stats.count++ > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }

    return true;
  }

  async _safeBatchAutoRemove() {
    try {
      await this.batchAutoRemove();
    } catch (error) {}
  }

  async _safeTick() {
    try {
      await this.tick();
    } catch (error) {}
  }

  async batchAutoRemove() {
    const now = Date.now();
    const removalThreshold = 25000;

    this.cleanExpiredLocks();

    const usersToRemoveNow = [];
    let processed = 0;
    const maxBatchSize = 30;

    for (const [idtarget, removalTime] of this.usersToRemove) {
      if (processed >= maxBatchSize) break;

      if (now - removalTime >= removalThreshold && !this.cleanupInProgress.has(idtarget)) {
        usersToRemoveNow.push(idtarget);
        processed++;
      }
    }

    for (const idtarget of usersToRemoveNow) {
      this.cleanupInProgress.add(idtarget);

      try {
        const stillActive = Array.from(this.clients).some(
          c => c.idtarget === idtarget && c.readyState === 1
        );

        if (!stillActive) {
          this.fullRemoveById(idtarget);
        }

        this.usersToRemove.delete(idtarget);
      } catch (error) {
      } finally {
        this.cleanupInProgress.delete(idtarget);
      }
    }

    this._runConsistencyCheck();
  }

  _runConsistencyCheck() {
    let checks = 0;
    const maxChecks = 50;
    const now = Date.now();

    for (const [idtarget, seatInfo] of this.userToSeat) {
      if (checks >= maxChecks) break;

      if (this.usersToRemove.has(idtarget)) continue;

      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      const seatData = seatMap.get(seat);
      if (!seatData || seatData.namauser !== idtarget) continue;

      const hasActiveConnection = Array.from(this.clients).some(
        c => c.idtarget === idtarget && c.readyState === 1
      );

      if (!hasActiveConnection) {
        this.usersToRemove.set(idtarget, now);
      }

      checks++;
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {}
    return false;
  }

  // ✅ FIX: Broadcast ke room dengan benar
  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) {
          sentCount++;
        }
      }
    }
    return sentCount;
  }

  // ✅ FIX: Private message langsung
  sendPrivateMessage(idtarget, msg) {
    let delivered = false;
    for (const c of this.clients) {
      if (c.idtarget === idtarget && c.readyState === 1) {
        this.safeSend(c, msg);
        delivered = true;
      }
    }
    return delivered;
  }

  // ✅ FIX: Hitung user per room dengan benar
  getJumlahRoom() {
    const cnt = Object.create(null);
    for (const room of roomList) {
      cnt[room] = 0;
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          // ✅ PERBAIKAN: Hitung hanya user yang bukan lock dan tidak empty
          if (info.namauser && 
              !String(info.namauser).startsWith("__LOCK__") && 
              info.namauser !== "") {
            cnt[room]++;
          }
        }
      }
    }
    return cnt;
  }

  // ✅ FIX: Broadcast room count ke semua client di room
  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {}
  }

  async tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;

    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        this.safeSend(c, ["currentNumber", this.currentNumber]);
      }
    }
  }

  cleanExpiredLocks() {
    const now = Date.now();
    let cleanedLocks = 0;
    const maxLocksToClean = 20;

    for (const room of roomList) {
      if (cleanedLocks >= maxLocksToClean) break;

      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const [seat, info] of seatMap) {
          if (cleanedLocks >= maxLocksToClean) break;

          if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
            Object.assign(info, createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastRoomUserCount(room);
            cleanedLocks++;
          }
        }
      }
    }
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {}
  }

  // ✅ FIX: Lock seat dengan benar
  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;

      const now = Date.now();

      // Clean expired locks
      let locksCleaned = 0;
      for (const [seat, info] of seatMap) {
        if (locksCleaned >= 5) break;

        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          locksCleaned++;
        }
      }

      // Find available seat
      for (let i = 1; i <= MAX_SEATS; i++) {
        const k = seatMap.get(i);
        if (k && k.namauser === "") {
          k.namauser = "__LOCK__" + ws.idtarget;
          k.lockTime = now;
          k.lastActivity = now;
          this.userToSeat.set(ws.idtarget, { room, seat: i });
          return i;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  senderrorstate(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);

      const kursiUpdates = [];
      for (let seat = 1; seat <= MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          kursiUpdates.push([
            seat,
            {
              noimageUrl: info.noimageUrl,
              namauser: info.namauser,
              color: info.color,
              itembawah: info.itembawah,
              itematas: info.itematas,
              vip: info.vip,
              viptanda: info.viptanda
            }
          ]);
        }
      }

      if (kursiUpdates.length > 0) {
        this.safeSend(ws, ["kursiBatchUpdate", room, kursiUpdates]);
      }

    } catch (error) {}
  }

  // ✅ FIX: Kirim semua state ke client baru
  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      // Kirim current number
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      // Kirim room count
      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);

      const allPoints = [];
      const kursiUpdates = [];

      for (let seat = 1; seat <= MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        // Points
        if (info.points.length > 0) {
          const recentPoints = info.points.slice(-MAX_POINTS_HISTORY);
          for (let i = 0; i < recentPoints.length; i++) {
            const point = recentPoints[i];
            allPoints.push({ seat, ...point });
          }
        }

        // Kursi data - ✅ PERBAIKAN: Kirim semua kursi termasuk yang kosong
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          kursiUpdates.push([
            seat,
            {
              noimageUrl: info.noimageUrl,
              namauser: info.namauser,
              color: info.color,
              itembawah: info.itembawah,
              itematas: info.itematas,
              vip: info.vip,
              viptanda: info.viptanda
            }
          ]);
        }
      }

      // Kirim points
      if (allPoints.length > 0) {
        this.safeSend(ws, ["allPointsList", room, allPoints]);
      }

      // ✅ PERBAIKAN: Kirim batch update kursi
      if (kursiUpdates.length > 0) {
        this.safeSend(ws, ["kursiBatchUpdate", room, kursiUpdates]);
      }

    } catch (error) {}
  }

  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    if (this.cleanupInProgress.has(id)) return;

    this.cleanupInProgress.add(id);

    try {
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      const activeConnections = Array.from(this.clients).filter(
        c => c.idtarget === id && c !== ws && c.readyState === 1
      );

      if (activeConnections.length === 0) {
        this.usersToRemove.set(id, Date.now());
      }

      this.clients.delete(ws);

      if (activeConnections.length === 0) {
        this.fullRemoveById(id);
      }

    } catch (error) {
    } finally {
      this.cleanupInProgress.delete(id);
    }
  }

  async removeAllSeatsById(idtarget) {
    try {
      const seatInfo = this.userToSeat.get(idtarget);
      if (!seatInfo) return;

      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap || !seatMap.has(seat)) {
        this.userToSeat.delete(idtarget);
        return;
      }

      const currentSeat = seatMap.get(seat);
      if (currentSeat.namauser === idtarget || currentSeat.namauser === `__LOCK__${idtarget}`) {
        Object.assign(currentSeat, createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(idtarget);
      this.usersToRemove.delete(idtarget);
    } catch (error) {}
  }

  getAllOnlineUsers() {
    const users = [];
    let count = 0;
    for (const ws of this.clients) {
      if (count >= 1000) break;
      if (ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
        count++;
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    let count = 0;
    for (const ws of this.clients) {
      if (count >= 500) break;
      if (ws.roomname === roomName && ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
        count++;
      }
    }
    return users;
  }

  // ✅ FIX: Handle setIdTarget2 dengan benar
  handleSetIdTarget2(ws, id, baru) {
    ws.idtarget = id;

    if (baru === true) {
      // Cleanup untuk user baru
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;

        for (const [seatNumber, seatInfo] of seatMap) {
          if (seatInfo.namauser === id) {
            Object.assign(seatInfo, createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          }
        }
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(id);
      this.usersToRemove.delete(id);
      
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

    } else if (baru === false) {
      // User lama - restore state
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      this.usersToRemove.delete(id);

      const seatInfo = this.userToSeat.get(id);

      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);

        if (seatMap?.has(seat)) {
          const seatData = seatMap.get(seat);

          if (seatData.namauser === id) {
            // ✅ User masih di seat yang sama - RESTORE BERHASIL
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            
            // ✅ KIRIM STATE LENGKAP ke client
            this.sendAllStateTo(ws, room);
            this.broadcastRoomUserCount(room);
          } else {
            this.safeSend(ws, ["needJoinRoom"]);
          }
        } else {
          this.safeSend(ws, ["needJoinRoom"]);
        }
      } else {
        this.safeSend(ws, ["needJoinRoom"]);
      }
    }
  }

  scheduleCleanupTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
    }

    const timeout = setTimeout(() => {
      if (this.cleanupInProgress.has(idtarget)) return;
      this.cleanupInProgress.add(idtarget);

      try {
        const stillActive = Array.from(this.clients).some(
          c => c.idtarget === idtarget && c.readyState === 1
        );

        if (!stillActive) {
          this.usersToRemove.set(idtarget, Date.now());
        }

      } catch (error) {
      } finally {
        this.pingTimeouts.delete(idtarget);
        this.cleanupInProgress.delete(idtarget);
      }
    }, RECONNECT_TIMEOUT);

    this.pingTimeouts.set(idtarget, timeout);
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;

    try {
      this.fullRemoveById(idtarget);
      this.clients.delete(ws);
    } catch (error) {}
  }

  // ✅ FIX: Join room dengan broadcast yang benar
  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) return false;

    if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);

    ws.roomname = newRoom;
    const foundSeat = this.lockSeat(newRoom, ws);

    if (foundSeat === null) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    ws.numkursi = new Set([foundSeat]);
    
    // ✅ KIRIM INFORMASI KE CLIENT
    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

    if (ws.idtarget) {
      this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
      
      // ✅ UPDATE SEAT DATA dengan user yang sebenarnya (bukan lock)
      const seatMap = this.roomSeats.get(newRoom);
      if (seatMap && seatMap.has(foundSeat)) {
        const seatData = seatMap.get(foundSeat);
        seatData.namauser = ws.idtarget; // Ganti dari __LOCK__ ke id asli
        seatData.lastActivity = Date.now();
        
        // ✅ BROADCAST KE SEMUA ORANG DI ROOM
        this.broadcastToRoom(newRoom, ["updateKursi", newRoom, foundSeat, 
          seatData.noimageUrl, seatData.namauser, seatData.color, 
          seatData.itembawah, seatData.itematas, seatData.vip, seatData.viptanda
        ]);
      }
    }
    
    // ✅ KIRIM SEMUA STATE KE CLIENT YANG BARU JOIN
    this.sendAllStateTo(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);
    
    return true;
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

    try {
      switch (evt) {
        case "isInRoom": {
          const idtarget = ws.idtarget;
          
          if (!idtarget) {
            this.safeSend(ws, ["inRoomStatus", false]);
            return;
          }
          
          const seatInfo = this.userToSeat.get(idtarget);
          if (!seatInfo) {
            this.safeSend(ws, ["inRoomStatus", false]);
            return;
          }
          
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);
          const seatData = seatMap?.get(seat);
          
          const isInRoom = seatData?.namauser === idtarget;
          
          this.safeSend(ws, ["inRoomStatus", isInRoom]);
          break;
        }

        case "onDestroy": {
          const idtarget = ws.idtarget;
          this.handleOnDestroy(ws, idtarget);
          break;
        }

        case "setIdTarget2": {
          const id = data[1];
          const baru = data[2];
          this.handleSetIdTarget2(ws, id, baru);
          break;
        }

        case "setIdTarget": {
          const newId = data[1];
          ws.idtarget = newId;

          if (this.pingTimeouts.has(newId)) {
            clearTimeout(this.pingTimeouts.get(newId));
            this.pingTimeouts.delete(newId);
          }

          if (this.usersToRemove.has(newId)) {
            this.usersToRemove.delete(newId);
          }

          const prevSeat = this.userToSeat.get(newId);

          if (prevSeat) {
            ws.roomname = prevSeat.room;
            ws.numkursi = new Set([prevSeat.seat]);
            this.senderrorstate(ws, prevSeat.room);

            const seatMap = this.roomSeats.get(prevSeat.room);
            if (seatMap) {
              const seatInfo = seatMap.get(prevSeat.seat);
              if (seatInfo.namauser === `__LOCK__${newId}`) {
                // ✅ KONVERSI LOCK KE USER ASLI
                seatInfo.namauser = newId;
                seatInfo.lastActivity = Date.now();
                
                // ✅ BROADCAST UPDATE KURSI
                this.broadcastToRoom(prevSeat.room, ["updateKursi", prevSeat.room, prevSeat.seat, 
                  seatInfo.noimageUrl, seatInfo.namauser, seatInfo.color, 
                  seatInfo.itembawah, seatInfo.itematas, seatInfo.vip, seatInfo.viptanda
                ]);
              }
            }
          } else {
            if (this.hasEverSetId) {
              this.safeSend(ws, ["needJoinRoom"]);
            }
          }

          this.hasEverSetId = true;

          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          this.sendPrivateMessage(idtarget, notif);
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, msg, ts, sender];
          this.safeSend(ws, out);
          this.sendPrivateMessage(idt, out);
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

        case "getAllRoomsUserCount":
          this.handleGetAllRoomsUserCount(ws);
          break;

        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;

        case "getOnlineUsers":
          this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
          break;

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
          break;
        }

        case "joinRoom": {
          const newRoom = data[1];
          this.handleJoinRoom(ws, newRoom);
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;
          
          const chatMsg = ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor];
          this.broadcastToRoom(roomname, chatMsg);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;

          si.points.push({ x, y, fast, timestamp: Date.now() });
          si.points = si.points.slice(-MAX_POINTS_HISTORY);
          si.lastActivity = Date.now();
          
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;

          const lockKey = `${room}-${seat}`;
          if (this.seatLocks.has(lockKey)) return;
          this.seatLocks.set(lockKey, true);

          try {
            const seatMap = this.roomSeats.get(room);
            const currentInfo = seatMap.get(seat) || createEmptySeat();

            Object.assign(currentInfo, {
              noimageUrl, namauser, color, itembawah, itematas, vip, viptanda,
              lastActivity: Date.now()
            });

            seatMap.set(seat, currentInfo);
            
            // ✅ BROADCAST REAL-TIME
            this.broadcastToRoom(room, ["updateKursi", room, seat, 
              noimageUrl, namauser, color, itembawah, itematas, vip, viptanda
            ]);
            
            this.broadcastRoomUserCount(room);
          } finally {
            this.seatLocks.delete(lockKey);
          }
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          const giftMsg = ["gift", roomname, sender, receiver, giftName, Date.now()];
          this.broadcastToRoom(roomname, giftMsg);
          break;
        }

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          const room = ws.roomname;
          if (room !== "LowCard") return;
          this.lowcard.handleEvent(ws, data);
          break;
        }
      }
    } catch (error) {}
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      await server.accept();

      const ws = server;

      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          this.cleanupClientSafely(ws);
        }
      });

      ws.addEventListener("close", (event) => {
        const id = ws.idtarget;
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        this.cleanupClientSafely(ws);
      });

      ws.addEventListener("error", (event) => {
        const id = ws.idtarget;
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        this.cleanupClientSafely(ws);
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      if (new URL(req.url).pathname === "/health")
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
