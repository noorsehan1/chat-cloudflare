// ChatServer Durable Object (Bahasa Indonesia)
// Versi lengkap dengan grace period 30 detik untuk banyak user
// Game Lowcard hanya boleh di room "LowCard"

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
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);

    this.lowcard = new LowCardGameManager(this);

    // Grace period 30 detik untuk reconnect
    this.pendingRemove = new Map();
    
    // Load pending removals dari storage saat startup
    this._initializePendingRemovals();
  }

  async _initializePendingRemovals() {
    try {
      const stored = await this.state.storage.get("pendingRemovals");
      if (stored) {
        const now = Date.now();
        console.log(`Found ${Object.keys(stored).length} pending removals in storage`);
        
        for (const [id, data] of Object.entries(stored)) {
          const timePassed = now - data.disconnectedAt;
          
          // Jika masih dalam grace period, set timer baru
          if (timePassed < 30000) {
            const timeLeft = 30000 - timePassed;
            console.log(`Restoring timer for user ${id}, ${timeLeft}ms left`);
            
            const timeout = setTimeout(() => {
              console.log(`Restored timer expired - removing seats for user: ${id}`);
              this.removeAllSeatsById(id);
              this.pendingRemove.delete(id);
              this._savePendingRemovals();
            }, timeLeft);

            this.pendingRemove.set(id, {
              timeout: timeout,
              room: data.room,
              disconnectedAt: data.disconnectedAt,
              wsId: id
            });
          } else {
            // Jika sudah lewat grace period, hapus langsung
            console.log(`Removing expired user ${id} from storage (${timePassed}ms passed)`);
            this.removeAllSeatsById(id);
          }
        }
        
        // Hapus data yang sudah expired dari storage
        await this._cleanupExpiredRemovals(stored, now);
      }
    } catch (error) {
      console.error('Error initializing pending removals:', error);
    }
  }

  async _cleanupExpiredRemovals(stored, now) {
    const updated = {};
    for (const [id, data] of Object.entries(stored)) {
      if (now - data.disconnectedAt < 30000) {
        updated[id] = data;
      }
    }
    await this.state.storage.put("pendingRemovals", updated);
  }

  async _savePendingRemovals() {
    try {
      const toSave = {};
      for (const [id, data] of this.pendingRemove) {
        toSave[id] = {
          room: data.room,
          disconnectedAt: data.disconnectedAt,
          wsId: data.wsId
        };
      }
      await this.state.storage.put("pendingRemovals", toSave);
      console.log(`Saved ${Object.keys(toSave).length} pending removals to storage`);
    } catch (error) {
      console.error('Error saving pending removals:', error);
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
      }
    } catch (e) {
      console.error('Error sending message:', e);
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room) this.safeSend(c, msg);
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
      for (const msg of messages) this.broadcastToRoom(room, msg);
      messages.length = 0;
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMapUpdates.has(seat)) continue;
        const info = seatMapUpdates.get(seat);
        const { points, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0)
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMapUpdates.clear();
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) this.safeSend(c, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();

    for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
      for (const c of this.clients) {
        if (c.idtarget === id) {
          for (const m of msgs) this.safeSend(c, m);
          this.privateMessageBuffer.delete(id);
          if (c.roomname) this.broadcastRoomUserCount(c.roomname);
        }
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

    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000)
        Object.assign(info, createEmptySeat());
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (!k) continue;
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = now;
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      for (const p of info.points) allPoints.push({ seat, ...p });
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

  removeAllSeatsById(idtarget) {
    console.log(`🚀 REMOVING ALL SEATS FOR USER: ${idtarget}`);
    let removedCount = 0;
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        if (info.namauser === idtarget) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          removedCount++;
          console.log(`✅ Removed seat ${seat} in room ${room} for user ${idtarget}`);
        }
      }
    }

    this.userToSeat.delete(idtarget);
    console.log(`📊 Total seats removed for user ${idtarget}: ${removedCount}`);
  }

  getAllOnlineUsers() {
    const users = [];
    for (const ws of this.clients) if (ws.idtarget) users.push(ws.idtarget);
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const ws of this.clients) if (ws.roomname === roomName && ws.idtarget) users.push(ws.idtarget);
    return users;
  }

  // Fungsi untuk memeriksa apakah user berada di room LowCard
  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  handleMessage(ws, raw) {
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      console.error('Error parsing JSON:', e);
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) {
      console.error('Invalid message format:', data);
      return this.safeSend(ws, ["error", "Invalid message format"]);
    }
    
    const evt = data[0];

    try {
      switch (evt) {
        case "setIdTarget": {
          const newId = data[1];
          console.log(`🎯 Setting ID target: ${newId}`);

          // Cleanup client lama dengan ID yang sama
          for (const c of Array.from(this.clients)) {
            if (c.idtarget === newId && c !== ws) {
              console.log(`🔄 Removing duplicate connection for user: ${newId}`);
              this.clients.delete(c);
              if (c.numkursi) c.numkursi.clear();
              c.roomname = undefined;
              c.idtarget = undefined;
            }
          }

          // Batalkan timer disconnect jika ada untuk user ID ini
          if (this.pendingRemove.has(newId)) {
            const pendingData = this.pendingRemove.get(newId);
            console.log(`✅ User ${newId} reconnected, canceling timer (disconnected at: ${new Date(pendingData.disconnectedAt).toISOString()})`);
            clearTimeout(pendingData.timeout);
            this.pendingRemove.delete(newId);
            this._savePendingRemovals();
          }

          ws.idtarget = newId;

          const seatInfo = this.userToSeat.get(newId);
          let lastRoom;

          if (seatInfo) {
            lastRoom = seatInfo.room;
            ws.roomname = lastRoom;
            this.sendAllStateTo(ws, lastRoom);
          } else {
            ws.roomname = undefined;
          }

          if (this.privateMessageBuffer.has(ws.idtarget)) {
            for (const msg of this.privateMessageBuffer.get(ws.idtarget))
              this.safeSend(ws, msg);
            this.privateMessageBuffer.delete(ws.idtarget);
          }

          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);

          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idtarget) { this.safeSend(c, notif); delivered = true; }
          }
          if (!delivered) {
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
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idt) { this.safeSend(c, out); delivered = true; }
          }
          if (!delivered) {
            if (!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt, []);
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
                if (old.numkursi) old.numkursi.clear();
                old.roomname = undefined;
                old.idtarget = undefined;
              } catch (e) {
                console.error('Error cleaning up duplicate connection:', e);
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
          
          if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);
          
          ws.roomname = newRoom;
          const seatMap = this.roomSeats.get(newRoom);
          const foundSeat = this.lockSeat(newRoom, ws);
          
          if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);
          
          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          
          if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
          
          this.sendAllStateTo(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);

          if (ws.idtarget && this.pendingRemove.has(ws.idtarget)) {
            clearTimeout(this.pendingRemove.get(ws.idtarget).timeout);
            this.pendingRemove.delete(ws.idtarget);
            this._savePendingRemovals();
          }
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
          if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname).push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;
          
          si.points = [{ x, y, fast }];
          
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          for (const c of this.clients) c.numkursi?.delete(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
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
            points: currentInfo.points
          });
          
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
          this.chatMessageBuffer.get(roomname).push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          break;
        }

        // Game Lowcard events - hanya boleh di room "LowCard"
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (!this.isInLowcardRoom(ws)) {
            return this.safeSend(ws, ["error", "Game Lowcard hanya tersedia di room LowCard"]);
          }
          this.lowcard.handleEvent(ws, data);
          break;

        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch (error) {
      console.error('Error handling message event:', evt, error);
      this.safeSend(ws, ["error", "Internal server error"]);
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
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    this.clients.add(ws);

    // Function untuk handle disconnect dengan timer
    const handleDisconnect = () => {
      const id = ws.idtarget;
      
      // Hapus dari clients set segera
      this.clients.delete(ws);
      
      if (id) {
        console.log(`⏰ User ${id} disconnected, starting 30s timer`);

        // Batalkan timer lama jika ada untuk user ID ini
        if (this.pendingRemove.has(id)) {
          console.log(`🔄 Canceling existing timer for user: ${id}`);
          clearTimeout(this.pendingRemove.get(id).timeout);
        }

        // Set timeout 30 detik untuk hapus kursi
        const timeout = setTimeout(() => {
          console.log(`⏰ Timer expired - removing seats for user: ${id}`);
          this.removeAllSeatsById(id);
          this.pendingRemove.delete(id);
          this._savePendingRemovals();
        }, 30000); // 30 detik

        // Simpan timer berdasarkan user ID
        this.pendingRemove.set(id, {
          timeout: timeout,
          room: ws.roomname,
          disconnectedAt: Date.now(),
          wsId: ws.idtarget
        });
        
        // Simpan ke storage
        this._savePendingRemovals();
      }

      // Clear data websocket segera
      if (ws.numkursi) ws.numkursi.clear();
      ws.roomname = undefined;
      ws.idtarget = undefined;
    };

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
        console.error('Error in message event listener:', error);
      }
    });

    // Pastikan timer berjalan baik untuk close maupun error
    ws.addEventListener("close", (event) => {
      console.log(`🔌 WebSocket closed for user: ${ws.idtarget}, code: ${event.code}, reason: ${event.reason}`);
      handleDisconnect();
    });

    ws.addEventListener("error", (error) => {
      console.error(`❌ WebSocket error for user: ${ws.idtarget}`, error);
      handleDisconnect();
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
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
