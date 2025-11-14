// ChatServer Durable Object - WITH BATCH AUTO REMOVE
import { LowCardGameManager } from "./lowcard.js";

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
    points: [],
    lockTime: undefined,
    lastActivity: Date.now()
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    
    this.clients = new Set();
    this.userToSeat = new Map();
    this.hasEverSetId = false;

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    
    // Initialize rooms and seats
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    
    // Initialize buffers for each room
    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }
    
    // Main timers dengan error handling yang lebih baik
    this._tickTimer = setInterval(() => {
      this.tick().catch(() => {});
    }, this.intervalMillis);
    
    this._flushTimer = setInterval(() => {
      this.periodicFlush().catch(() => {});
    }, 100);

    // Auto remove timer - 20 seconds
    this._autoRemoveTimer = setInterval(() => {
      this.batchAutoRemove().catch(() => {});
    }, 20000);

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 20000;
    this.cleanupInProgress = new Set();
    
    // Track users marked for removal
    this.usersToRemove = new Map(); // idtarget -> removal time
  }

  async destroy() {
    // Cleanup semua timer
    const timers = [this._tickTimer, this._flushTimer, this._autoRemoveTimer];
    for (const timer of timers) {
      if (timer) clearInterval(timer);
    }
    
    // Cleanup semua timeout
    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();
  }

  // Batch Auto Remove Function - Remove inactive users every 20 seconds
  async batchAutoRemove() {
    try {
      const now = Date.now();
      const removalThreshold = 20000; // 20 seconds
      
      // Check for expired locks first
      this.cleanExpiredLocks();
      
      // Find inactive users
      const usersToRemoveNow = [];
      
      for (const [idtarget, removalTime] of this.usersToRemove) {
        if (now - removalTime >= removalThreshold) {
          usersToRemoveNow.push(idtarget);
        }
      }
      
      // Remove inactive users
      for (const idtarget of usersToRemoveNow) {
        if (this.cleanupInProgress.has(idtarget)) continue;
        
        this.cleanupInProgress.add(idtarget);
        
        try {
          // Check if user reconnected
          const stillActive = Array.from(this.clients).some(
            c => c.idtarget === idtarget && c.readyState === 1
          );
          
          if (!stillActive) {
            await this.removeAllSeatsById(idtarget);
            
            // Send needJoinRoom notification to any remaining connections
            for (const client of this.clients) {
              if (client.idtarget === idtarget && client.readyState === 1) {
                this.safeSend(client, ["needJoinRoom"]);
              }
            }
          }
          
          this.usersToRemove.delete(idtarget);
        } catch (error) {
          console.error('Error in batchAutoRemove:', error);
        } finally {
          this.cleanupInProgress.delete(idtarget);
        }
      }
      
      // Also check for users with no activity but not in usersToRemove
      for (const [idtarget, seatInfo] of this.userToSeat) {
        if (this.usersToRemove.has(idtarget)) continue;
        
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;
        
        const seatData = seatMap.get(seat);
        if (!seatData || seatData.namauser !== idtarget) continue;
        
        // Check if user has active connection
        const hasActiveConnection = Array.from(this.clients).some(
          c => c.idtarget === idtarget && c.readyState === 1
        );
        
        if (!hasActiveConnection) {
          this.usersToRemove.set(idtarget, now);
        }
      }
      
    } catch (error) {
      console.error('Error in batchAutoRemove:', error);
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      // Ignore send errors
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    const clientsToRemove = [];
    
    for (const c of this.clients) {
      if (c.roomname === room) {
        if (c.readyState === 3) {
          clientsToRemove.push(c);
        } else if (c.readyState === 1) {
          try {
            this.safeSend(c, msg);
          } catch (error) {
            clientsToRemove.push(c);
          }
        }
      }
    }
    
    for (const closedClient of clientsToRemove) {
      this.cleanupClientSafely(closedClient);
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
            cnt[room]++;
          }
        }
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      console.error('Error broadcasting room count:', error);
    }
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          for (const msg of messages) {
            this.broadcastToRoom(room, msg);
          }
          this.chatMessageBuffer.set(room, []);
        } catch (error) {
          this.chatMessageBuffer.set(room, []);
        }
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      try {
        const updates = [];
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          if (!seatMapUpdates.has(seat)) continue;
          const info = seatMapUpdates.get(seat);
          const { points, ...rest } = info;
          updates.push([seat, rest]);
        }
        if (updates.length > 0) {
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
        }
        this.updateKursiBuffer.set(room, new Map());
      } catch (error) {
        this.updateKursiBuffer.set(room, new Map());
      }
    }
  }

  async tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const clientsToRemove = [];
      for (const c of this.clients) {
        if (c.readyState === 3) {
          clientsToRemove.push(c);
        } else if (c.readyState === 1) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      }
      
      for (const closedClient of clientsToRemove) {
        this.cleanupClientSafely(closedClient);
      }
    } catch (error) {
      console.error('Error in tick:', error);
    }
  }

  cleanExpiredLocks() {
    try {
      const now = Date.now();
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          for (const [seat, info] of seatMap) {
            if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
              Object.assign(info, createEmptySeat());
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.broadcastRoomUserCount(room);
            }
          }
        }
      }
    } catch (error) {
      console.error('Error cleaning expired locks:', error);
    }
  }

  async periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();
      
      // Update last activity for connected users
      const now = Date.now();
      for (const client of this.clients) {
        if (client.idtarget && client.readyState === 1) {
          const seatInfo = this.userToSeat.get(client.idtarget);
          if (seatInfo) {
            const { room, seat } = seatInfo;
            const seatMap = this.roomSeats.get(room);
            if (seatMap && seatMap.has(seat)) {
              const seatData = seatMap.get(seat);
              if (seatData.namauser === client.idtarget) {
                seatData.lastActivity = now;
                
                // Cancel removal if user is active
                if (this.usersToRemove.has(client.idtarget)) {
                  this.usersToRemove.delete(client.idtarget);
                }
              }
            }
          }
        }
      }

      const deliveredIds = [];
      for (const [id, msgs] of this.privateMessageBuffer) {
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === id && c.readyState === 1) {
            try {
              for (const m of msgs) {
                this.safeSend(c, m);
              }
              delivered = true;
              break;
            } catch (error) {
              // Continue to next client
            }
          }
        }
        if (delivered) {
          deliveredIds.push(id);
        }
      }
      
      for (const id of deliveredIds) {
        this.privateMessageBuffer.delete(id);
      }
    } catch (error) {
      console.error('Error in periodicFlush:', error);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      console.error('Error handling room count:', error);
    }
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;
    
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;
      
      const now = Date.now();

      // Clean expired locks first
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
        }
      }

      // Find empty seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
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
      console.error('Error locking seat:', error);
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
      
      const activeSeats = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;
        
        const hasUser = info.namauser && !String(info.namauser).startsWith("__LOCK__");
        const hasPoints = info.points.length > 0;
        
        if (hasUser || hasPoints) {
          activeSeats.push({ seat, info });
        }
      }
      
      const kursiUpdates = [];
      for (const { seat, info } of activeSeats) {
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
      
      // Send points with delay to avoid overwhelming the client
      activeSeats.forEach(({ seat, info }, activeIndex) => {
        if (info.points.length > 0) {
          setTimeout(() => {
            if (ws.readyState !== 1) return;
            
            for (const point of info.points) {
              this.safeSend(ws, ["updatePoint", room, seat, point.x, point.y, point.fast]);
            }
            
          }, activeIndex * 100);
        }
      });
      
    } catch (error) {
      console.error('Error sending error state:', error);
    }
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;
    
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      const allPoints = [];
      const meta = {};
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;
        
        for (const p of info.points) {
          allPoints.push({ seat, ...p });
        }
        
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
    } catch (error) {
      console.error('Error sending all state:', error);
    }
  }

  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    if (this.cleanupInProgress.has(id)) {
      return;
    }
    
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
        // Schedule for batch removal instead of immediate removal
        this.usersToRemove.set(id, Date.now());
      }

      this.clients.delete(ws);
      
      if (this.privateMessageBuffer.has(id)) {
        this.privateMessageBuffer.delete(id);
      }
      
    } catch (error) {
      console.error('Error in cleanupClientSafely:', error);
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
      if (!seatMap || !seatMap.has(seat)) return;

      const currentSeat = seatMap.get(seat);
      if (currentSeat.namauser === idtarget) {
        Object.assign(currentSeat, createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(idtarget);
      this.usersToRemove.delete(idtarget); // Remove from pending removal
    } catch (error) {
      console.error('Error removing seats by ID:', error);
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const ws of this.clients) {
      if (ws.idtarget && ws.readyState === 1) users.push(ws.idtarget);
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const ws of this.clients) {
      if (ws.roomname === roomName && ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
      }
    }
    return users;
  }


 handleSetIdTarget2(ws, id, baru) {
  ws.idtarget = id;

  // USER BARU
  if (baru === true) {
    // Cancel any pending removal when new user connects
    if (this.usersToRemove.has(id)) {
      this.usersToRemove.delete(id);
    }

    // Clear any existing ping timeout
    if (this.pingTimeouts.has(id)) {
      clearTimeout(this.pingTimeouts.get(id));
      this.pingTimeouts.delete(id);
    }

    ws.isNewUser = true;
    
    // For new users, send needJoinRoom to prompt room selection
    this.safeSend(ws, ["needJoinRoom"]);
    return;
  }

  // RECONNECT - Existing user
  ws.isNewUser = false;

  // Cancel any pending removal when user reconnects
  if (this.usersToRemove.has(id)) {
    this.usersToRemove.delete(id);
  }

  // Clear any existing ping timeout
  if (this.pingTimeouts.has(id)) {
    clearTimeout(this.pingTimeouts.get(id));
    this.pingTimeouts.delete(id);
  }

  const seatInfo = this.userToSeat.get(id);

  if (seatInfo) {
    const { room, seat } = seatInfo;
    ws.roomname = room;
    ws.numkursi = new Set([seat]);

    // Update seat activity and ensure user is properly seated
    const seatMap = this.roomSeats.get(room);
    if (seatMap && seatMap.has(seat)) {
      const seatData = seatMap.get(seat);
      
      // If seat is locked or empty, reclaim it
      if (String(seatData.namauser).startsWith("__LOCK__") || !seatData.namauser) {
        seatData.namauser = id;
        seatData.lastActivity = Date.now();
      }
      
      // If seat belongs to someone else, remove user from seat mapping
      if (seatData.namauser !== id) {
        this.userToSeat.delete(id);
        this.safeSend(ws, ["needJoinRoom"]);
      } else {
        // Send current room state to reconnected user
        this.senderrorstate(ws, room);
        this.broadcastRoomUserCount(room);
      }
    } else {
      // Seat doesn't exist, remove mapping
      this.userToSeat.delete(id);
      this.safeSend(ws, ["needJoinRoom"]);
    }
  } else {
    // No seat info found, prompt for room join
    this.safeSend(ws, ["needJoinRoom"]);
  }

  // Deliver any pending private messages
  if (this.privateMessageBuffer.has(id)) {
    for (const msg of this.privateMessageBuffer.get(id)) {
      this.safeSend(ws, msg);
    }
    this.privateMessageBuffer.delete(id);
  }

  console.log("SET ID 2:", id, "baru:", baru, "room:", ws.roomname);
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
        
        const stuckClients = [];
        for (const client of this.clients) {
          if (client.idtarget === idtarget && (client.readyState === 2 || client.readyState === 3)) {
            stuckClients.push(client);
          }
        }
        
        for (const client of stuckClients) {
          this.clients.delete(client);
        }
        
      } catch (error) {
        console.error('Error in cleanup timeout:', error);
      } finally {
        this.pingTimeouts.delete(idtarget);
        this.cleanupInProgress.delete(idtarget);
      }
    }, this.RECONNECT_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) 
      return this.safeSend(ws, ["error", "Invalid message format"]);
    
    const evt = data[0];

    try {
      switch (evt) {
        case "onDestroy": {
          const idtarget = ws.idtarget;
          this.handleOnDestroy(ws, idtarget);
          break;
        }

          
     case "setIdTarget2": {
  const id = data[1];
  const baru = data[2]; // This should be the boolean parameter
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

          // Cancel any pending removal when user reconnects
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
              if (seatInfo.namauser === `__LOCK__${newId}` || !seatInfo.namauser) {
                seatInfo.namauser = newId;
                seatInfo.lastActivity = Date.now();
              }
            }
          } else {
            if (!this.hasEverSetId) {
              // First connection - do nothing special
            } else {
              this.safeSend(ws, ["needJoinRoom"]);
            }
          }

          this.hasEverSetId = true;

          if (this.privateMessageBuffer.has(newId)) {
            for (const msg of this.privateMessageBuffer.get(newId)) 
              this.safeSend(ws, msg);
            this.privateMessageBuffer.delete(newId);
          }

          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) { 
              this.safeSend(c, notif); 
              delivered = true; 
            }
          }
          if (!delivered) {
            if (!this.privateMessageBuffer.has(idtarget)) 
              this.privateMessageBuffer.set(idtarget, []);
            this.privateMessageBuffer.get(idtarget).push(notif);
          }
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, msg, ts, sender];
          this.safeSend(ws, out);
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idt && c.readyState === 1) { 
              this.safeSend(c, out); 
              delivered = true; 
            }
          }
          if (!delivered) {
            if (!this.privateMessageBuffer.has(idt)) 
              this.privateMessageBuffer.set(idt, []);
            this.privateMessageBuffer.get(idt).push(out);
            this.safeSend(ws, ["privateFailed", idt, "User offline"]);
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";

          const activeSockets = Array.from(this.clients)
            .filter(c => c.idtarget === username && c.readyState === 1);
          const online = activeSockets.length > 0;

          this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

          // Handle duplicate connections
          if (activeSockets.length > 1) {
            const newest = activeSockets[activeSockets.length - 1];
            const oldSockets = activeSockets.slice(0, -1);

            const userSeatInfo = this.userToSeat.get(username);
            if (userSeatInfo) {
              const { room, seat } = userSeatInfo;
              const seatMap = this.roomSeats.get(room);
              if (seatMap && seatMap.has(seat)) {
                Object.assign(seatMap.get(seat), createEmptySeat());
                this.broadcastToRoom(room, ["removeKursi", room, seat]);
                this.broadcastRoomUserCount(room);
              }
              this.userToSeat.delete(username);
            }

            for (const old of oldSockets) {
              try { 
                if (old.readyState === 1) {
                  old.close(4000, "Duplicate login"); 
                }
                this.clients.delete(old); 
              } catch (e) {
                // Ignore errors during cleanup
              }
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

        case "getOnlineUsers": 
          this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]); 
          break;

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) 
            return this.safeSend(ws, ["error", "Unknown room"]);
          this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
          break;
        }

        case "joinRoom": {
          const newRoom = data[1];
          if (!roomList.includes(newRoom)) 
            return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
          
          if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);
          
          ws.roomname = newRoom;
          const foundSeat = this.lockSeat(newRoom, ws);
          
          if (foundSeat === null) 
            return this.safeSend(ws, ["roomFull", newRoom]);
          
          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          
          if (ws.idtarget) 
            this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
          
          this.sendAllStateTo(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) 
            return this.safeSend(ws, ["error", "Invalid room for chat"]);
          if (!this.chatMessageBuffer.has(roomname)) 
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) 
            return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;
          
          // Update points and activity
          si.points = [{ x, y, fast }];
          si.lastActivity = Date.now();
          
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) 
            return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          for (const c of this.clients) c.numkursi?.delete(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) 
            return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat) || createEmptySeat();
          
          Object.assign(currentInfo, { 
            noimageUrl, 
            namauser, 
            color, 
            itembawah, 
            itematas, 
            vip, 
            viptanda,
            lastActivity: Date.now()
          });
          
          seatMap.set(seat, currentInfo);
          if (!this.updateKursiBuffer.has(room)) 
            this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, points: [] });
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) 
            return this.safeSend(ws, ["error", "Invalid room for gift"]);
          if (!this.chatMessageBuffer.has(roomname)) 
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          break;
        }

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          const room = ws.roomname;
          if (room !== "LowCard") {
            this.safeSend(ws, ["error", "Game LowCard hanya bisa dimainkan di room 'Lowcard'"]);
            break;
          }
          this.lowcard.handleEvent(ws, data);
          break;
        }

        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch (error) {
      console.error('Error handling message:', error);
      try {
        this.safeSend(ws, ["error", "Internal server error"]);
      } catch (sendError) {
        // Ignore send errors during error handling
      }
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      const ws = server;

      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();
      ws.isDestroyed = false;

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          console.error('Error in message event listener:', error);
          try {
            if (ws.readyState === 1) {
              ws.close(1011, "Internal server error");
            }
          } catch (closeError) {
            // Ignore close errors
          } finally {
            this.cleanupClientSafely(ws);
          }
        }
      });
      
      ws.addEventListener("close", (event) => {
        if (!ws.isDestroyed) {
          const id = ws.idtarget;
          if (id) {
            this.scheduleCleanupTimeout(id);
          }
          this.cleanupClientSafely(ws);
        }
      });

      ws.addEventListener("error", (error) => {
        console.error('WebSocket error:', error);
        const id = ws.idtarget;
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        
        if (!ws.isDestroyed) {
          this.cleanupClientSafely(ws);
        }
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error('Error in fetch:', error);
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
      console.error('Error in default fetch:', error);
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};

