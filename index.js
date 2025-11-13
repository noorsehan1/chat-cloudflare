// ChatServer Durable Object (Bahasa Indonesia)
// Versi lengkap dengan semua perbaikan optimasi

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

    // Grace period 20 detik untuk reconnect
    this.gracePeriod = 20000; // 20 detik
    this.pendingRemove = new Map();
    
    // ✅ BUFFER BARU: Simpan chat yang terlewat per USER
    this.missedChatsBuffer = new Map(); // key: userid, value: array of missed messages
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
      }
    } catch (e) {
      // Tetap diam jika error pengiriman
    }
  }

  broadcastToRoom(room, msg) {
    // ✅ SIMPAN CHAT KE BUFFER: Jika ini message chat/gift, simpan ke buffer user yang offline
    if (msg[0] === "chat" || msg[0] === "gift") {
        const roomName = msg[1];
        
        // Cari semua user yang ada di room ini tapi sedang offline (dalam grace period)
        for (const [userId, seatInfo] of this.userToSeat) {
            if (seatInfo.room === roomName) {
                // Cek apakah user ini sedang offline (tidak ada koneksi aktif)
                let isUserOnline = false;
                for (const client of this.clients) {
                    if (client.idtarget === userId && client.readyState === 1) {
                        isUserOnline = true;
                        break;
                    }
                }
                
                // Jika user OFFLINE, simpan chat ke buffer-nya
                if (!isUserOnline) {
                    if (!this.missedChatsBuffer.has(userId)) {
                        this.missedChatsBuffer.set(userId, []);
                    }
                    const buffer = this.missedChatsBuffer.get(userId);
                    buffer.push(msg);
                    
                    // ✅ BATASI buffer maksimal 100 pesan per user
                    if (buffer.length > 100) {
                        buffer.shift(); // Hapus pesan paling lama
                    }
                }
            }
        }
    }
    
    // Broadcast ke user yang online
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
    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();

    // Cleanup WebSocket yang sudah closed
    for (const client of Array.from(this.clients)) {
      if (client.readyState === 2 || client.readyState === 3) {
        this.cleanupClient(client);
      }
    }

    // Kirim pesan private yang tertunda
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

    // Bersihkan lock yang expired
    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
        Object.assign(info, createEmptySeat());
      }
    }

    // Cari kursi kosong
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

    // Kumpulkan data kursi yang ADA USER dan POINT
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        // ✅ Hanya kursi yang ADA USER (bukan kosong atau lock)
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

    // ✅ Kirim kursiBatchUpdate dan pointUpdated dengan delay bertahap 50ms * index
    seatData.forEach((data, index) => {
        setTimeout(() => {
            // Kirim update kursi
            this.safeSend(ws, ["kursiBatchUpdate", room, [[data.seat, {
                noimageUrl: data.noimageUrl,
                namauser: data.namauser,
                color: data.color,
                itembawah: data.itembawah,
                itematas: data.itematas,
                vip: data.vip,
                viptanda: data.viptanda
            }]]]);

            // Kirim point jika ada
            if (data.points) {
                this.safeSend(ws, ["pointUpdated", room, data.seat, data.points.x, data.points.y, data.points.fast]);
            }
        }, 50 * index); // Delay bertahap
    });
    
    // ✅ KIRIM PENDING CHATS SETELAH KURSI SELESAI (TANPA DOUBLE)
    setTimeout(() => {
        if (ws.idtarget && this.missedChatsBuffer.has(ws.idtarget)) {
            const missedChats = this.missedChatsBuffer.get(ws.idtarget);
            const roomMissedChats = missedChats.filter(chat => chat[1] === room);
            
            if (roomMissedChats.length > 0) {
                // ✅ Kirim chat satu per satu dengan case KHUSUS "pendingChat"
                roomMissedChats.forEach((chatMsg, index) => {
                    setTimeout(() => {
                        // Format: ["pendingChat", roomname, noImageURL, username, message, usernameColor, chatTextColor]
                        const pendingChatMessage = ["pendingChat", ...chatMsg.slice(1)];
                        this.safeSend(ws, pendingChatMessage);
                        
                        if (index === roomMissedChats.length - 1) {
                            this.missedChatsBuffer.delete(ws.idtarget);
                        }
                    }, 100 * index); // Delay 100ms per chat
                });
            } else {
                this.missedChatsBuffer.delete(ws.idtarget);
            }
        }
    }, 500); // Delay 500ms setelah kursi selesai
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
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        if (info.namauser === idtarget) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          removedCount++;
        } else if (String(info.namauser).startsWith("__LOCK__" + idtarget)) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          removedCount++;
        }
      }
    }

    this.userToSeat.delete(idtarget);
    
    // ✅ HAPUS BUFFER CHAT: Saat hapus semua kursi user
    if (this.missedChatsBuffer.has(idtarget)) {
      this.missedChatsBuffer.delete(idtarget);
    }
    
    return removedCount;
  }

  cleanupClientDestroy(ws) {
    const id = ws.idtarget;
    
    this.clients.delete(ws);
    
    if (id) {
      // Batalkan pending removal lama jika ada
      if (this.pendingRemove.has(id)) {
        clearTimeout(this.pendingRemove.get(id));
        this.pendingRemove.delete(id);
      }

      // ✅ HAPUS BUFFER CHAT: Saat onDestroy()
      if (this.missedChatsBuffer.has(id)) {
        this.missedChatsBuffer.delete(id);
      }

      // ✅ KIRIM needJoinRoom KE CLIENT sebelum menghapus
      this.safeSend(ws, ["needJoinRoom", -1]);
      
      // Hapus kursi langsung TANPA timeout 20 detik
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

  // ✅ PERBAIKAN: cleanupClient yang benar - cek koneksi aktif sebelum set timeout
  cleanupClient(ws) {
    const id = ws.idtarget;
    
    this.clients.delete(ws);
    
    if (id) {
      // ✅ CEK: Apakah user ini masih punya koneksi aktif lain?
      let hasActiveConnection = false;
      for (const client of this.clients) {
        if (client.idtarget === id && client.readyState === 1) {
          hasActiveConnection = true;
          break;
        }
      }
      
      // ❌ JANGAN set timeout jika masih ada koneksi aktif
      if (!hasActiveConnection) {
        // Batalkan pending removal lama jika ada
        if (this.pendingRemove.has(id)) {
          clearTimeout(this.pendingRemove.get(id));
        }

        // ✅ SET TIMEOUT hanya jika benar-benar tidak ada koneksi aktif
        const timeout = setTimeout(() => {
          // Hapus buffer chat
          if (this.missedChatsBuffer.has(id)) {
            this.missedChatsBuffer.delete(id);
          }
          
          // Hapus kursi
          this.removeAllSeatsById(id);
          this.pendingRemove.delete(id);
        }, this.gracePeriod);

        this.pendingRemove.set(id, timeout);
      } else {
        // ✅ User masih ada koneksi aktif, batalkan timeout
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
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
      case "setIdTarget": {
        const newId = data[1];

        // ✅ BATALKAN TIMEOUT: Pastikan batalkan dulu sebelum lanjut
        this.batalkanPendingRemoval(newId);

        // ✅ SET ID TARGET DULU sebelum menutup koneksi duplikat
        ws.idtarget = newId;

        // Tutup koneksi duplikat
        for (const client of Array.from(this.clients)) {
          if (client.idtarget === newId && client !== ws && client.readyState === 1) {
            try {
              client.close(4000, "Duplicate connection");
              this.clients.delete(client);
            } catch (e) {
              // Silent catch
            }
          }
        }

        // ✅ HAPUS BUFFER CHAT: Saat pertama kali buka aplikasi
        if (this.missedChatsBuffer.has(newId)) {
          this.missedChatsBuffer.delete(newId);
        }

        const seatInfo = this.userToSeat.get(newId);

        if (seatInfo) {
          // User memiliki kursi aktif (dalam grace period 20 detik)
          const lastRoom = seatInfo.room;
          const lastSeat = seatInfo.seat;
          ws.roomname = lastRoom;
          
          // Kirim state lengkap dengan optimasi 50ms
          this.sendPointKursi(ws, lastRoom);
        } else {
          // Tidak ada kursi aktif
          ws.roomname = undefined;
        }

        // Kirim pesan private yang tertunda
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
          if (c.idtarget === idt && c.readyState === 1) { 
            this.safeSend(c, out); 
            delivered = true; 
          }
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
            } catch (e) {}
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
        
        // Batalkan pending removal sebelum pindah room
        if (ws.idtarget) {
          this.batalkanPendingRemoval(ws.idtarget);
          this.removeAllSeatsById(ws.idtarget);
          
          // ✅ HAPUS BUFFER CHAT: Saat pindah/join room baru
          if (this.missedChatsBuffer.has(ws.idtarget)) {
            this.missedChatsBuffer.delete(ws.idtarget);
          }
        }
        
        ws.roomname = newRoom;
        const seatMap = this.roomSeats.get(newRoom);
        const foundSeat = this.lockSeat(newRoom, ws);
        
        if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);
        
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        
        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        
        this.safeSend(ws, ["currentNumber", this.currentNumber]); 
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);

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

      case "onDestroy": {
        if (ws.idtarget) {
          // Batalkan pending removal dan hapus langsung
          this.batalkanPendingRemoval(ws.idtarget);
          this.cleanupClientDestroy(ws);
        }
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

    ws.addEventListener("message", (ev) => {
      this.handleMessage(ws, ev.data);
    });

    ws.addEventListener("close", (event) => {
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
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
