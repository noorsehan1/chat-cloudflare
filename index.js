// ChatServer Durable Object (Bahasa Indonesia)
// Versi debug dengan log detail dan interval

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
    
    // ✅ INTERVAL UTAMA
    this._tickTimer = setInterval(() => {
      console.log("⏰ TICK TIMER executed");
      this.tick();
    }, this.intervalMillis);

    // ✅ INTERVAL PERIODIC FLUSH (5 detik)
    this._flushTimer = setInterval(() => {
      console.log("🔄 PERIODIC FLUSH TIMER executed");
      this.periodicFlush();
    }, 5000);

    this.lowcard = new LowCardGameManager(this);

    this.gracePeriod = 20000;
    this.pendingRemove = new Map();
    this.missedChatsBuffer = new Map();
    this.disconnectTime = new Map();

    console.log("🚀 ChatServer INITIALIZED dengan interval");
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
      }
    } catch (e) {
      console.log("❌ safeSend ERROR:", e.message);
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room && c.readyState === 1) {
        this.safeSend(c, msg);
      }
    }
  }

  broadcastToAll(msg) {
    for (const c of Array.from(this.clients)) {
      if (c.readyState === 1) {
        this.safeSend(c, msg);
      }
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          cnt[room]++;
        }
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
      if (messages.length > 0) {
        for (const msg of messages) {
          this.broadcastToRoom(room, msg);
        }
        messages.length = 0;
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (seatMapUpdates.has(seat)) {
          const info = seatMapUpdates.get(seat);
          const { points, ...rest } = info;
          updates.push([seat, rest]);
        }
      }
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      seatMapUpdates.clear();
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) {
      if (c.readyState === 1) {
        this.safeSend(c, ["currentNumber", this.currentNumber]);
      }
    }
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
    console.log("🔄 periodicFlush DIMULAI");
    
    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();

    // ✅ PERBAIKAN: Hanya hapus disconnectTime, JANGAN hapus missedChatsBuffer
    const now = Date.now();
    console.log(`🕒 periodicFlush - DisconnectTime entries: ${this.disconnectTime.size}`);
    
    for (const [userId, disconnectTimestamp] of Array.from(this.disconnectTime)) {
      const timeSinceDisconnect = now - disconnectTimestamp;
      console.log(`🕒 User ${userId} - DC selama: ${Math.round(timeSinceDisconnect/1000)}s`);
      
      if (timeSinceDisconnect > this.gracePeriod) {
        console.log(`🧹 Hapus disconnectTime untuk ${userId} (lewat ${this.gracePeriod/1000}s)`);
        this.disconnectTime.delete(userId);
      }
    }

    for (const client of Array.from(this.clients)) {
      if (client.readyState === 2 || client.readyState === 3) {
        this.cleanupClient(client);
      }
    }

    for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
      let delivered = false;
      for (const c of this.clients) {
        if (c.idtarget === id && c.readyState === 1) {
          for (const m of msgs) this.safeSend(c, m);
          delivered = true;
        }
      }
      if (delivered) {
        this.privateMessageBuffer.delete(id);
      }
    }

    console.log("🔄 periodicFlush SELESAI");
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
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
        Object.assign(info, createEmptySeat());
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && k.namauser === "") {
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
  }

  sendPointKursi(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const seatData = [];
    this.safeSend(ws, ["currentNumber", this.currentNumber]); 

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
            seatData.push({
                seat,
                noimageUrl: info.noimageUrl,
                namauser: info.namauser,
                color: info.color,
                itembawah: info.itembawah,
                itematas: info.itematas,
                vip: info.vip,
                viptanda: info.viptanda,
                points: info.points.length > 0 ? info.points[0] : null
            });
        }
    }

    seatData.forEach((data, index) => {
        setTimeout(() => {
            this.safeSend(ws, ["kursiBatchUpdate", room, [[data.seat, {
                noimageUrl: data.noimageUrl,
                namauser: data.namauser,
                color: data.color,
                itembawah: data.itembawah,
                itematas: data.itematas,
                vip: data.vip,
                viptanda: data.viptanda
            }]]]);

            if (data.points) {
                this.safeSend(ws, ["pointUpdated", room, data.seat, data.points.x, data.points.y, false]);
            }
        }, 50 * index);
    });
    
    setTimeout(() => {
        if (ws.idtarget && this.missedChatsBuffer.has(ws.idtarget)) {
            const missedChats = this.missedChatsBuffer.get(ws.idtarget);
            const roomMissedChats = missedChats.filter(chat => chat[1] === room);
            
            console.log(`📨 CHECK MISSED CHATS: User ${ws.idtarget}, Room ${room}, Total: ${missedChats.length}, RoomSpecific: ${roomMissedChats.length}`);
            
            if (roomMissedChats.length > 0) {
                console.log(`📤 KIRIM ${roomMissedChats.length} missed chats ke ${ws.idtarget}`);
                
                roomMissedChats.forEach((chatMsg, index) => {
                    setTimeout(() => {
                        const pendingChatMessage = ["pendingChat", ...chatMsg.slice(1)];
                        console.log(`📤 Kirim pendingChat: ${chatMsg[4]?.substring(0, 30)}...`);
                        this.safeSend(ws, pendingChatMessage);
                    }, 100 * index);
                });
                
                const remainingChats = missedChats.filter(chat => chat[1] !== room);
                if (remainingChats.length > 0) {
                    this.missedChatsBuffer.set(ws.idtarget, remainingChats);
                    console.log(`💾 Simpan ${remainingChats.length} missed chats untuk room lain`);
                } else {
                    this.missedChatsBuffer.delete(ws.idtarget);
                    console.log(`🧹 Hapus buffer missed chats untuk ${ws.idtarget}`);
                }
            } else {
                this.missedChatsBuffer.delete(ws.idtarget);
                console.log(`❌ Tidak ada missed chats untuk room ${room}`);
            }
        } else {
            console.log(`❌ Tidak ada missed chats buffer untuk ${ws.idtarget}`);
        }
    }, 1500);
  }

  cleanupClientById(idtarget) {
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget) {
        this.cleanupClient(c);
      }
    }
  }

  removeAllSeatsById(idtarget) {
    let removedCount = 0;
    
    console.log(`🔍 Mencari kursi untuk dihapus: ${idtarget}`);
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        if (info.namauser === idtarget) {
          console.log(`🗑️ Hapus kursi ${seat} di room ${room} untuk ${idtarget}`);
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          removedCount++;
        } else if (String(info.namauser).startsWith("__LOCK__" + idtarget)) {
          console.log(`🗑️ Hapus kursi LOCK ${seat} di room ${room} untuk ${idtarget}`);
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          removedCount++;
        }
      }
    }

    this.userToSeat.delete(idtarget);
    
    // ✅ Hapus disconnectTime (stop chat pending baru)
    if (this.disconnectTime.has(idtarget)) {
      this.disconnectTime.delete(idtarget);
      console.log(`🧹 Hapus disconnectTime untuk ${idtarget}`);
    }
    
    // ❌ JANGAN hapus missedChatsBuffer di sini!
    // Biarkan missed chats tetap tersimpan
    
    console.log(`✅ Selesai hapus ${removedCount} kursi untuk ${idtarget}`);
    return removedCount;
  }

  cleanupClientDestroy(ws) {
    const id = ws.idtarget;
    
    console.log(`💥 cleanupClientDestroy DIPANGGIL untuk: ${id}`);
    
    this.clients.delete(ws);
    
    if (id) {
      if (this.pendingRemove.has(id)) {
        clearTimeout(this.pendingRemove.get(id));
        this.pendingRemove.delete(id);
        console.log(`⏰ Batalkan timer remove untuk ${id}`);
      }

      this.disconnectTime.delete(id);
      console.log(`🧹 Hapus disconnectTime untuk ${id}`);

      // ❌ JANGAN hapus missedChatsBuffer di sini!
      // Biarkan untuk reconnect nanti

      this.removeAllSeatsById(id);
    }

    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  batalkanPendingRemoval(userId) {
    if (userId && this.pendingRemove.has(userId)) {
      clearTimeout(this.pendingRemove.get(userId));
      this.pendingRemove.delete(userId);
      console.log(`⏰ Batalkan pending removal untuk ${userId}`);
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const ws of this.clients) {
      if (ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
      }
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

  // ✅ CLEANUP CLIENT DENGAN LOG DETAIL
  cleanupClient(ws) {
    const id = ws.idtarget;
    
    console.log(`🔴 cleanupClient DIPANGGIL untuk: ${id}`);
    console.log(`🔴 WebSocket readyState: ${ws.readyState}`);
    console.log(`🔴 Clients sebelum delete: ${this.clients.size}`);
    
    this.clients.delete(ws);
    
    console.log(`🔴 Clients setelah delete: ${this.clients.size}`);
    
    if (id) {
      let hasActiveConnection = false;
      let activeConnections = [];
      
      for (const client of this.clients) {
        if (client.idtarget === id) {
          activeConnections.push({
            id: client.idtarget,
            readyState: client.readyState,
            room: client.roomname
          });
          if (client.readyState === 1) {
            hasActiveConnection = true;
          }
        }
      }
      
      console.log(`🔴 Koneksi aktif untuk ${id}:`, activeConnections);
      console.log(`🔴 hasActiveConnection: ${hasActiveConnection}`);
      
      if (!hasActiveConnection) {
        // ✅ TIMER 1: AKTIFKAN CHAT PENDING (langsung)
        this.disconnectTime.set(id, Date.now());
        console.log(`⏰ DISCONNECT TIME SET untuk ${id}: ${new Date().toISOString()}`);
        
        // ✅ TIMER 2: HAPUS KURSI (setelah 20 detik)
        if (this.pendingRemove.has(id)) {
          clearTimeout(this.pendingRemove.get(id));
          this.pendingRemove.delete(id);
        }

        const timeout = setTimeout(() => {
          console.log(`⏰ TIMER EXECUTE: Hapus kursi ${id}`);
          this.removeAllSeatsById(id);
          this.pendingRemove.delete(id);
        }, this.gracePeriod);

        this.pendingRemove.set(id, timeout);
        console.log(`⏰ TIMER KURSI SET: ${id} akan dihapus dalam ${this.gracePeriod/1000}s`);
        
      } else {
        console.log(`✅ User ${id} masih ada koneksi aktif, batalkan timer`);
        if (this.pendingRemove.has(id)) {
          clearTimeout(this.pendingRemove.get(id));
          this.pendingRemove.delete(id);
        }
      }
    }

    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  handleMessage(ws, raw) {
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      console.log("❌ JSON PARSE ERROR:", e.message);
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    console.log(`📨 MESSAGE RECEIVED: ${evt} dari ${ws.idtarget}`);

    switch (evt) {
      case "setIdTarget": {
        const newId = data[1];
        console.log(`🎯 setIdTarget: ${newId}`);

        this.batalkanPendingRemoval(newId);

        // ✅ Hentikan timer chat pending saat reconnect
        if (this.disconnectTime.has(newId)) {
          this.disconnectTime.delete(newId);
          console.log(`🔁 Hapus disconnectTime untuk ${newId} (reconnect)`);
        }

        ws.idtarget = newId;

        for (const client of Array.from(this.clients)) {
          if (client.idtarget === newId && client !== ws && client.readyState === 1) {
            try {
              console.log(`🔒 Tutup koneksi duplikat untuk ${newId}`);
              client.close(4000, "Duplicate connection");
              this.clients.delete(client);
            } catch (e) {
              console.log("❌ Error close duplicate:", e.message);
            }
          }
        }

        const seatInfo = this.userToSeat.get(newId);

        if (seatInfo) {
          const lastRoom = seatInfo.room;
          const lastSeat = seatInfo.seat;
          ws.roomname = lastRoom;
          console.log(`🔁 User ${newId} reconnect ke ${lastRoom} kursi ${lastSeat}`);
          this.sendPointKursi(ws, lastRoom);
        } else {
          ws.roomname = undefined;
          console.log(`👤 User ${newId} join pertama kali`);
          // ❌ JANGAN hapus missedChatsBuffer di sini!
        }

        if (this.privateMessageBuffer.has(ws.idtarget)) {
          for (const msg of this.privateMessageBuffer.get(ws.idtarget)) {
            this.safeSend(ws, msg);
          }
          this.privateMessageBuffer.delete(ws.idtarget);
        }

        if (ws.roomname) {
          this.broadcastRoomUserCount(ws.roomname);
        }

        break;
      }

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
        
        const chatMessage = ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor];
        
        console.log(`💬 CHAT DITERIMA: ${username} di ${roomname}: ${message.substring(0, 30)}`);
        
        // ✅ SIMPAN KE MISSED CHATS BUFFER untuk user yang offline
        let totalOfflineUsers = 0;
        for (const [userId, seatInfo] of this.userToSeat) {
          if (seatInfo.room === roomname) {
            let isUserCurrentlyOnline = false;
            
            // ✅ CEK APAKAH USER ONLINE
            for (const client of this.clients) {
              if (client.idtarget === userId && client.readyState === 1) {
                isUserCurrentlyOnline = true;
                break;
              }
            }
            
            console.log(`👤 User ${userId} di ${roomname}: Online=${isUserCurrentlyOnline}, DisconnectTime=${this.disconnectTime.has(userId)}`);
            
            // ✅ JIKA USER OFFLINE & MASIH DALAM GRACE PERIOD
            if (!isUserCurrentlyOnline && this.disconnectTime.has(userId)) {
              if (!this.missedChatsBuffer.has(userId)) {
                this.missedChatsBuffer.set(userId, []);
              }
              const buffer = this.missedChatsBuffer.get(userId);
              buffer.push(chatMessage);
              
              if (buffer.length > 100) {
                buffer.shift();
              }
              
              totalOfflineUsers++;
              console.log(`💾 SIMPAN missed chat untuk ${userId}: ${message.substring(0, 30)}...`);
            }
          }
        }
        
        console.log(`📊 TOTAL OFFLINE USERS: ${totalOfflineUsers}`);
        
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push(chatMessage);
        
        this.flushChatBuffer();
        
        break;
      }

      // ... case lainnya tetap sama ...

      default:
        console.log(`❌ UNKNOWN EVENT: ${evt}`);
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
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    this.clients.add(ws);

    console.log(`🟢 NEW WebSocket CONNECTED. Total clients: ${this.clients.size}`);

    ws.addEventListener("message", (ev) => {
      console.log(`📨 WebSocket MESSAGE dari client`);
      this.handleMessage(ws, ev.data);
    });

    ws.addEventListener("close", (event) => {
      console.log(`🔴 WebSocket CLOSE: code=${event.code}, reason=${event.reason}`);
      this.cleanupClient(ws);
    });

    ws.addEventListener("error", (error) => {
      console.log(`🔴 WebSocket ERROR:`, error);
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
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
