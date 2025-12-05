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
    lastPoint: null
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.graceTimers = new Map();
    
    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    this.roomClients = new Map();
    
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
      this.roomClients.set(room, new Set());
    }

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);

    // Timer untuk rotasi number
    this.currentNumber = 1;
    this.maxNumber = 6;
    
    // Simpan timer ID untuk cleanup
    this.mainIntervalId = null;
    
    this.updateKursiBuffer = new Map();
    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
    }
    
    this.initializeTimers();
  }

  initializeTimers() {
    // Hapus timer lama jika ada
    if (this.mainIntervalId) {
      clearInterval(this.mainIntervalId);
    }
    
    this.mainIntervalId = setInterval(() => {
      try {
        this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
        this.broadcastToAll(["currentNumber", this.currentNumber]);
      } catch (error) {
        console.error("Error in number rotation timer:", error);
      }
    }, 15 * 60 * 1000);
  }

  safeSend(ws, arr) {
    if (!ws || ws.readyState !== 1) return false;
    
    try {
      // Batasi buffer amount
      if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 5000000) {
        return false;
      }
      
      ws.send(JSON.stringify(arr));
      return true;
    } catch (error) {
      // Jika error kirim, anggap WebSocket mati
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clientSet = this.roomClients.get(room);
    if (!clientSet) return 0;
    
    let sentCount = 0;
    // Buat array copy untuk menghindari modifikasi saat iterasi
    const clientsArray = Array.from(clientSet);
    
    for (const c of clientsArray) {
      try {
        // Periksa ulang sebelum kirim
        if (c && c.readyState === 1 && c.roomname === room) {
          if (this.safeSend(c, msg)) sentCount++;
        }
      } catch (error) {
        // Abaikan error untuk client ini, lanjut ke berikutnya
        console.error(`Error broadcasting to client in room ${room}:`, error);
      }
    }
    return sentCount;
  }

  broadcastToAll(msg) {
    let sentCount = 0;
    // Buat array copy untuk menghindari modifikasi saat iterasi
    const clientsArray = Array.from(this.clients);
    
    for (const c of clientsArray) {
      try {
        if (c && c.readyState === 1) {
          if (this.safeSend(c, msg)) sentCount++;
        }
      } catch (error) {
        console.error("Error broadcasting to client:", error);
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      let count = 0;
      for (const info of seatMap.values()) {
        if (info.namauser) count++;
      }
      counts[room] = count;
    }
    return counts;
  }

  broadcastRoomUserCount(room) {
    try {
      if (!room || !roomList.includes(room)) return;
      const counts = this.getJumlahRoom();
      this.broadcastToRoom(room, ["roomUserCount", room, counts[room] || 0]);
    } catch (error) {
      console.error(`Error broadcasting room count for ${room}:`, error);
    }
  }

  isUserInRoom(userId, room) {
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return false;
      
      for (const info of seatMap.values()) {
        if (info.namauser === userId) {
          return true;
        }
      }
      return false;
    } catch (error) {
      console.error(`Error checking if user ${userId} is in room ${room}:`, error);
      return false;
    }
  }

  isUserInAnyRoom(userId) {
    for (const room of roomList) {
      if (this.isUserInRoom(userId, room)) {
        return true;
      }
    }
    return false;
  }

  removeUserFromAllRooms(userId) {
    try {
      for (const room of roomList) {
        this.removeUserFromRoom(userId, room);
      }
      this.userToSeat.delete(userId);
    } catch (error) {
      console.error(`Error removing user ${userId} from all rooms:`, error);
    }
  }

  removeUserFromRoom(userId, room) {
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      let seatToRemove = null;
      
      // Cari seat yang perlu dihapus
      for (const [seatNumber, info] of seatMap) {
        if (info.namauser === userId) {
          seatToRemove = seatNumber;
          break;
        }
      }
      
      if (seatToRemove) {
        const seatInfo = seatMap.get(seatToRemove);
        if (seatInfo.viptanda > 0) {
          this.vipManager.removeVipBadge(room, seatToRemove);
        }
        Object.assign(seatInfo, createEmptySeat());
        this.clearSeatBuffer(room, seatToRemove);
        this.broadcastToRoom(room, ["removeKursi", room, seatToRemove]);
        this.userToSeat.delete(userId);
      }
      
      // Hapus dari roomClients dengan aman
      const clientSet = this.roomClients.get(room);
      if (clientSet) {
        // Buat array copy untuk iterasi
        const clientsToRemove = [];
        for (const client of clientSet) {
          if (client.idtarget === userId) {
            clientsToRemove.push(client);
          }
        }
        
        // Hapus setelah iterasi selesai
        for (const client of clientsToRemove) {
          clientSet.delete(client);
        }
      }
      
      this.broadcastRoomUserCount(room);
    } catch (error) {
      console.error(`Error removing user ${userId} from room ${room}:`, error);
    }
  }

  clearSeatBuffer(room, seatNumber) {
    try {
      if (!room || typeof seatNumber !== "number") return;
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) roomMap.delete(seatNumber);
    } catch (error) {
      console.error(`Error clearing seat buffer for ${room} seat ${seatNumber}:`, error);
    }
  }

  scheduleUserCleanup(userId) {
    try {
      const oldTimer = this.graceTimers.get(userId);
      if (oldTimer) clearTimeout(oldTimer);

      const timer = setTimeout(() => {
        try {
          this.graceTimers.delete(userId);
          this.removeUserFromAllRooms(userId);
        } catch (error) {
          console.error(`Error in grace period cleanup for user ${userId}:`, error);
        }
      }, 5000);

      this.graceTimers.set(userId, timer);
    } catch (error) {
      console.error(`Error scheduling cleanup for user ${userId}:`, error);
    }
  }

  cancelUserCleanup(userId) {
    try {
      const timer = this.graceTimers.get(userId);
      if (timer) {
        clearTimeout(timer);
        this.graceTimers.delete(userId);
      }
    } catch (error) {
      console.error(`Error cancelling cleanup for user ${userId}:`, error);
    }
  }

  flushKursiUpdates() {
    try {
      for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
        if (!roomList.includes(room)) continue;
        
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
    } catch (error) {
      console.error("Error flushing kursi updates:", error);
    }
  }

  periodicFlush() {
    try {
      if (this.clients.size > 0) this.flushKursiUpdates();
    } catch (error) {
      console.error("Error in periodic flush:", error);
    }
  }

  handleOnDestroy(ws, idtarget) {
    try {
      if (!idtarget) return;
      
      // Hapus user secara permanen
      this.removeUserFromAllRooms(idtarget);
      this.cancelUserCleanup(idtarget);
      
      // Hapus dari roomClients
      if (ws.roomname) {
        const clientSet = this.roomClients.get(ws.roomname);
        if (clientSet) clientSet.delete(ws);
      }
      
      // Hapus dari clients
      this.clients.delete(ws);
      
      if (ws.readyState === 1) {
        try {
          ws.close(1000, "Manual destroy");
        } catch (error) {
          // Ignore close errors
        }
      }
    } catch (error) {
      console.error(`Error in handleOnDestroy for user ${idtarget}:`, error);
    }
  }

  getAllOnlineUsers() {
    try {
      const users = [];
      const clientsArray = Array.from(this.clients);
      
      for (const c of clientsArray) {
        if (c && c.idtarget && c.readyState === 1) {
          users.push(c.idtarget);
        }
      }
      return users;
    } catch (error) {
      console.error("Error getting all online users:", error);
      return [];
    }
  }

  getOnlineUsersByRoom(roomName) {
    try {
      const users = [];
      const clientSet = this.roomClients.get(roomName);
      if (clientSet) {
        const clientsArray = Array.from(clientSet);
        for (const c of clientsArray) {
          if (c && c.idtarget && c.readyState === 1) {
            users.push(c.idtarget);
          }
        }
      }
      return users;
    } catch (error) {
      console.error(`Error getting online users for room ${roomName}:`, error);
      return [];
    }
  }

  handleGetAllRoomsUserCount(ws) {
    try {
      if (ws.readyState !== 1) return;
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      console.error("Error handling getAllRoomsUserCount:", error);
    }
  }

  handleSetIdTarget2(ws, id, baru) {
    try {
      if (!id) return;

      if (baru === true) {
        // User baru, hapus semua data lama
        this.removeUserFromAllRooms(id);
        this.cancelUserCleanup(id);
        ws.idtarget = id;
        ws.roomname = undefined;
        this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      this.cancelUserCleanup(id);
      
      // Cek apakah user sudah ada di room
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        
        // Verifikasi seat masih milik user ini
        if (seatMap && seatMap.get(seat).namauser === id) {
          ws.roomname = room;
          
          const clientSet = this.roomClients.get(room);
          if (clientSet) clientSet.add(ws);
          
          this.sendAllStateTo(ws, room);
          this.broadcastRoomUserCount(room);
          this.vipManager.getAllVipBadges(ws, room);
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          return;
        }
      }
      
      // User tidak punya seat yang valid
      this.safeSend(ws, ["needJoinRoom"]);
    } catch (error) {
      console.error(`Error in handleSetIdTarget2 for user ${id}:`, error);
    }
  }

  handleJoinRoom(ws, room) {
    try {
      if (!ws.idtarget || !roomList.includes(room)) {
        this.safeSend(ws, ["error", "Invalid user or room"]);
        return false;
      }
      
      // Cek apakah user sudah ada di room ini
      if (this.isUserInRoom(ws.idtarget, room)) {
        // User sudah ada di room ini
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (seatInfo && seatInfo.room === room) {
          ws.roomname = room;
          const clientSet = this.roomClients.get(room);
          if (clientSet) clientSet.add(ws);
          
          this.sendAllStateTo(ws, room);
          this.broadcastRoomUserCount(room);
          this.safeSend(ws, ["rooMasuk", seatInfo.seat, room]);
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          return true;
        }
      }
      
      // Cek apakah user sudah ada di room lain
      if (this.isUserInAnyRoom(ws.idtarget)) {
        this.safeSend(ws, ["error", "You are already in another room"]);
        return false;
      }
      
      this.cancelUserCleanup(ws.idtarget);
      
      // Keluar dari room lama jika ada
      if (ws.roomname && ws.roomname !== room) {
        const oldClientSet = this.roomClients.get(ws.roomname);
        if (oldClientSet) oldClientSet.delete(ws);
      }
      
      // Cari seat kosong dengan prioritas
      const seatMap = this.roomSeats.get(room);
      let selectedSeat = null;
      
      // 1. Cari seat kosong
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const seatInfo = seatMap.get(i);
        if (!seatInfo.namauser) {
          selectedSeat = i;
          break;
        }
      }
      
      // 2. Jika tidak ada seat kosong, cari seat user yang sudah offline
      if (!selectedSeat) {
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const seatInfo = seatMap.get(i);
          if (seatInfo.namauser) {
            let isOccupantOnline = false;
            const clientsArray = Array.from(this.clients);
            for (const c of clientsArray) {
              if (c && c.idtarget === seatInfo.namauser && c.readyState === 1) {
                isOccupantOnline = true;
                break;
              }
            }
            
            if (!isOccupantOnline) {
              // Hapus user lama dari seat
              const oldUserId = seatInfo.namauser;
              
              if (seatInfo.viptanda > 0) {
                this.vipManager.removeVipBadge(room, i);
              }
              
              // Hapus dari userToSeat
              this.userToSeat.delete(oldUserId);
              
              // Hapus dari grace timers
              this.cancelUserCleanup(oldUserId);
              
              Object.assign(seatInfo, createEmptySeat());
              this.clearSeatBuffer(room, i);
              this.broadcastToRoom(room, ["removeKursi", room, i]);
              selectedSeat = i;
              break;
            }
          }
        }
      }
      
      if (!selectedSeat) {
        this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      // Update seat
      const seatInfo = seatMap.get(selectedSeat);
      seatInfo.namauser = ws.idtarget;
      
      // Simpan seat info untuk reconnect
      this.userToSeat.set(ws.idtarget, { room, seat: selectedSeat });
      
      ws.roomname = room;
      
      const clientSet = this.roomClients.get(room);
      if (clientSet) clientSet.add(ws);
      
      this.sendAllStateTo(ws, room);
      this.broadcastRoomUserCount(room);
      this.safeSend(ws, ["rooMasuk", selectedSeat, room]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      return true;
    } catch (error) {
      console.error(`Error in handleJoinRoom for user ${ws.idtarget} to room ${room}:`, error);
      this.safeSend(ws, ["error", "Internal server error"]);
      return false;
    }
  }

  sendAllStateTo(ws, room) {
    try {
      if (ws.readyState !== 1 || !room) return;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      const allKursiMeta = {};
      const lastPointsData = [];

      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (info.namauser) {
          allKursiMeta[seat] = {
            noimageUrl: info.noimageUrl,
            namauser: info.namauser,
            color: info.color,
            itembawah: info.itembawah,
            itematas: info.itematas,
            vip: info.vip,
            viptanda: info.viptanda
          };

          if (info.lastPoint) {
            lastPointsData.push({
              seat: seat,
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

      const counts = this.getJumlahRoom();
      this.safeSend(ws, ["roomUserCount", room, counts[room] || 0]);
    } catch (error) {
      console.error(`Error sending all state to user ${ws.idtarget} in room ${room}:`, error);
    }
  }

  handleMessage(ws, raw) {
    try {
      if (ws.readyState !== 1) return;

      if (raw.length > 100000) {
        ws.close(1009, "Message too large");
        return;
      }

      let data;
      try { 
        data = JSON.parse(raw); 
      } catch (e) { 
        return; 
      }
      
      if (!Array.isArray(data) || data.length === 0) return;

      const evt = data[0];

      switch (evt) {
        case "vipbadge":
        case "removeVipBadge":
        case "getAllVipBadges":
          this.vipManager.handleEvent(ws, data);
          break;

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
          
        case "setIdTarget2": 
          this.handleSetIdTarget2(ws, data[1], data[2]); 
          break;

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          const clientsArray = Array.from(this.clients);
          for (const c of clientsArray) {
            if (c && c.idtarget === idtarget && c.readyState === 1) {
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
          const clientsArray = Array.from(this.clients);
          for (const c of clientsArray) {
            if (c && c.idtarget === idt && c.readyState === 1) {
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
          const clientsArray = Array.from(this.clients);
          for (const c of clientsArray) {
            if (c && c.idtarget === username && c.readyState === 1) {
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

        case "joinRoom": 
          this.handleJoinRoom(ws, data[1]); 
          break;

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          
          if (ws.roomname !== roomname) {
            return;
          }
          
          if (ws.idtarget !== username) {
            return;
          }
          
          if (!roomList.includes(roomname)) return;

          const clientSet = this.roomClients.get(roomname);
          if (!clientSet) return;
          
          const clientsArray = Array.from(clientSet);
          for (const c of clientsArray) {
            if (c && c.readyState === 1 && c.roomname === roomname) {
              this.safeSend(c, [
                "chat", 
                roomname, 
                noImageURL, 
                username, 
                message, 
                usernameColor, 
                chatTextColor
              ]);
            }
          }
          
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          
          if (ws.roomname !== room) {
            return;
          }
          
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;
          
          // Cek apakah user punya akses ke seat ini
          if (si.namauser !== ws.idtarget) {
            return;
          }
          
          si.lastPoint = { x, y, fast, timestamp: Date.now() };
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          
          if (ws.roomname !== room) {
            return;
          }
          
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          const seatInfo = seatMap.get(seat);
          
          // Cek apakah user punya akses ke seat ini
          if (seatInfo.namauser !== ws.idtarget) {
            return;
          }
          
          Object.assign(seatInfo, createEmptySeat());
          this.clearSeatBuffer(room, seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          
          if (ws.roomname !== room) {
            return;
          }
          
          if (!roomList.includes(room)) return;

          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat) || createEmptySeat();
          
          // Cek apakah user punya akses ke seat ini
          if (currentInfo.namauser !== ws.idtarget && namauser !== ws.idtarget) {
            return;
          }
          
          Object.assign(currentInfo, {
            noimageUrl, namauser, color, itembawah, itematas,
            vip: vip || 0,
            viptanda: viptanda || 0
          });

          seatMap.set(seat, currentInfo);
          if (!this.updateKursiBuffer.has(room))
            this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          
          if (ws.roomname !== roomname) {
            return;
          }
          
          if (ws.idtarget !== sender) {
            return;
          }
          
          if (!roomList.includes(roomname)) return;

          const giftData = ["gift", roomname, sender, receiver, giftName];
          
          const clientSet = this.roomClients.get(roomname);
          if (clientSet) {
            const clientsArray = Array.from(clientSet);
            for (const c of clientsArray) {
              if (c && c.readyState === 1 && c.roomname === roomname) {
                this.safeSend(c, giftData);
              }
            }
          }
          
          break;
        }

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (ws.roomname === "LowCard") {
              this.lowcard.handleEvent(ws, data);
          }
          break;
          
        default: 
          break;
      }
    } catch (error) {
      console.error("Error handling message:", error);
      // Jangan crash server karena error di satu message
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      await server.accept();

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      
      this.clients.add(ws);

      // Timer untuk periodic flush
      const flushTimer = setInterval(() => {
        try {
          this.periodicFlush();
        } catch (error) {
          console.error("Error in flush timer:", error);
        }
      }, 50);
      
      const messageHandler = (ev) => {
        this.handleMessage(ws, ev.data);
      };
      
      const errorHandler = (event) => {
        // Catat error tapi jangan crash
        console.error("WebSocket error:", event);
      };
      
      const closeHandler = () => {
        try {
          clearInterval(flushTimer);
          
          if (ws.roomname) {
            const clientSet = this.roomClients.get(ws.roomname);
            if (clientSet) clientSet.delete(ws);
          }
          
          if (ws.idtarget) {
            this.scheduleUserCleanup(ws.idtarget);
          }
          
          this.clients.delete(ws);
          
          // Remove event listeners
          ws.removeEventListener("message", messageHandler);
          ws.removeEventListener("error", errorHandler);
          ws.removeEventListener("close", closeHandler);
        } catch (error) {
          console.error("Error in WebSocket close handler:", error);
        }
      };

      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("Error in fetch handler:", error);
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
      if (new URL(req.url).pathname === "/health") {
        return new Response("ok", { status: 200 });
      }
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      console.error("Error in default fetch:", error);
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
