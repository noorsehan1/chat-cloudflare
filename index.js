// ChatServer Durable Object - FIXED isDestroyed ISSUE (keep original handleOnDestroy)
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
    lockTime: undefined
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
    
    this._tickTimer = setInterval(() => {
      try {
        this.tick();
      } catch (error) {}
    }, this.intervalMillis);
    
    this._flushTimer = setInterval(() => {
      try {
        this.periodicFlush();
      } catch (error) {}
    }, 100);

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 40000; // 40 detik
    this.cleanupInProgress = new Set();
  }

  async destroy() {
    if (this._tickTimer) clearInterval(this._tickTimer);
    if (this._flushTimer) clearInterval(this._flushTimer);
    
    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();
  }

  // ⚡ FIXED: Cleanup timeout yang BENAR-BENAR bekerja
  scheduleCleanupTimeout(idtarget) {
    console.log(`⏰ Scheduling 40s cleanup timeout for: ${idtarget}`);
    
    // Clear existing timeout jika ada
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    
    const timeout = setTimeout(() => {
      console.log(`⏰⏰⏰ TIMEOUT 40s TRIGGERED for: ${idtarget}`);
      
      if (this.cleanupInProgress.has(idtarget)) {
        console.log(`⏰ Cleanup already in progress for ${idtarget}, skipping`);
        return;
      }
      
      this.cleanupInProgress.add(idtarget);
      
      try {
        // ⚡ FIXED: Cek yang LEBIH AKURAT - apakah masih ada koneksi aktif
        const stillActive = Array.from(this.clients).some(
          c => c.idtarget === idtarget && c.readyState === 1 // HANYA yang readyState 1 (OPEN)
        );
        
        console.log(`⏰ User ${idtarget} still active after 40s?: ${stillActive}`);
        
        if (!stillActive) {
          console.log(`⏰🚨 REMOVING USER: ${idtarget} - No active connections after 40s`);
          
          // ⚡ FIXED: HAPUS SEMUA DATA USER
          const seatInfo = this.userToSeat.get(idtarget);
          if (seatInfo) {
            const { room, seat } = seatInfo;
            const seatMap = this.roomSeats.get(room);
            if (seatMap && seatMap.has(seat)) {
              const currentSeat = seatMap.get(seat);
              if (currentSeat.namauser === idtarget) {
                // ⚡ BENAR-BENAR RESET KURSI
                Object.assign(currentSeat, createEmptySeat());
                this.broadcastToRoom(room, ["removeKursi", room, seat]);
                this.broadcastRoomUserCount(room);
                console.log(`⏰✅ Removed ${idtarget} from seat ${seat} in room ${room}`);
              }
            }
            // ⚡ HAPUS DARI userToSeat
            this.userToSeat.delete(idtarget);
            console.log(`⏰✅ Removed ${idtarget} from userToSeat mapping`);
          }
          
          // ⚡ HAPUS BUFFER MESSAGE
          if (this.privateMessageBuffer.has(idtarget)) {
            this.privateMessageBuffer.delete(idtarget);
            console.log(`⏰✅ Cleared private message buffer for ${idtarget}`);
          }
        } else {
          console.log(`⏰✅ ${idtarget} reconnected within 40s, skipping cleanup`);
        }
        
        // ⚡ CLEANUP WEBSOCKET YANG STUCK (readyState 2/3)
        const stuckClients = Array.from(this.clients).filter(
          client => client.idtarget === idtarget && client.readyState !== 1
        );
        
        for (const client of stuckClients) {
          this.clients.delete(client);
          console.log(`⏰✅ Removed stuck WebSocket (state: ${client.readyState}) for ${idtarget}`);
        }
        
      } catch (error) {
        console.error(`⏰❌ Error during cleanup for ${idtarget}:`, error);
      } finally {
        // ⚡ PASTIKAN SELALU CLEANUP
        this.pingTimeouts.delete(idtarget);
        this.cleanupInProgress.delete(idtarget);
        console.log(`⏰🏁 Cleanup completed for ${idtarget}`);
      }
    }, this.RECONNECT_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
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
    } catch (error) {}
  }

  // ⚡ FIXED: Cleanup client dengan timeout
  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    if (this.cleanupInProgress.has(id)) {
      return;
    }
    
    console.log(`🔌 cleanupClientSafely called for: ${id}`);
    
    // ⚡ LANGSUNG SCHEDULE TIMEOUT - jangan langsung hapus!
    this.scheduleCleanupTimeout(id);
    
    // Tapi tetap hapus WebSocket yang closed dari clients set
    this.clients.delete(ws);
  }

  removeAllSeatsById(idtarget) {
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
    } catch (error) {}
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

  // ✅ BIARKAN SEPERTI KODE AWAL: handleOnDestroy
  handleOnDestroy(ws, idtarget) {
    if (ws.isDestroyed) return;
    
    ws.isDestroyed = true;
    
    if (idtarget) {
      this.cleanupInProgress.add(idtarget);
      
      const seatInfo = this.userToSeat.get(idtarget);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        if (seatMap && seatMap.has(seat)) {
          const currentSeat = seatMap.get(seat);
          if (currentSeat.namauser === idtarget) {
            Object.assign(currentSeat, createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastRoomUserCount(room);
          }
        }
        this.userToSeat.delete(idtarget);
      }
      
      this.clients.delete(ws);
      
      if (this.pingTimeouts.has(idtarget)) {
        clearTimeout(this.pingTimeouts.get(idtarget));
        this.pingTimeouts.delete(idtarget);
      }
      
      this.cleanupInProgress.delete(idtarget);
    } else {
      this.clients.delete(ws);
    }
  }

  // ⚡ FIXED: Handler setIdTarget yang CLEAR timeout
  handleSetIdTarget(ws, newId) {
    ws.idtarget = newId;

    // ⚡ CLEAR TIMEOUT JIKA USER RECONNECT SEBELUM 40 DETIK
    if (this.pingTimeouts.has(newId)) {
      clearTimeout(this.pingTimeouts.get(newId));
      this.pingTimeouts.delete(newId);
      console.log(`✅✅✅ CLEARED TIMEOUT - User reconnected: ${newId}`);
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
        }
      }
    } else {
      if (!this.hasEverSetId) {
        // First time setup
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
  }

  // ... (methods lainnya: senderrorstate, sendAllStateTo, lockSeat, dll)

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
          this.handleOnDestroy(ws, idtarget); // ✅ Tetap pakai yang original
          break;
        }

        case "setIdTarget": {
          const newId = data[1];
          this.handleSetIdTarget(ws, newId);
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

        // ... (case lainnya tetap sama)

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

        // ... (case lainnya)

      }
    } catch (error) {
      try {
        this.safeSend(ws, ["error", "Internal server error"]);
      } catch (sendError) {}
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
      ws.isDestroyed = false; // ✅ Tetap ada isDestroyed untuk handleOnDestroy

      this.clients.add(ws);

      // ⚡ FIXED: Gunakan arrow function untuk mempertahankan 'this' context
      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          try {
            if (ws.readyState === 1) {
              ws.close(1011, "Internal server error");
            }
          } catch (closeError) {
          } finally {
            this.cleanupClientSafely(ws);
          }
        }
      });
      
      // ⚡ FIXED: Langsung jalankan timeout saat WebSocket close - TANPA CEK isDestroyed
      ws.addEventListener("close", (event) => {
        console.log(`🔌 WebSocket closed for: ${ws.idtarget}, code: ${event.code}`);
        // ⚡ LANGSUNG JALANKAN tanpa cek isDestroyed
        if (ws.idtarget) {
          console.log(`⏰ Immediately scheduling cleanup timeout for: ${ws.idtarget}`);
          this.scheduleCleanupTimeout(ws.idtarget);
        }
      });

      // ⚡ FIXED: Langsung jalankan timeout saat WebSocket error - TANPA CEK isDestroyed
      ws.addEventListener("error", (error) => {
        console.log(`❌ WebSocket error for: ${ws.idtarget}`, error);
        // ⚡ LANGSUNG JALANKAN tanpa cek isDestroyed
        if (ws.idtarget) {
          console.log(`⏰ Immediately scheduling cleanup timeout for: ${ws.idtarget}`);
          this.scheduleCleanupTimeout(ws.idtarget);
        }
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
