// ============================
// VipBadgeManager (Real-time Multi-room)
// ============================
export class VipBadgeManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.vipBadges = new Map(); // key: room, value: Map(seat -> vipData)
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "vipbadge":
        this.sendVipBadge(ws, data[1], data[2], data[3], data[4]);
        break;
      case "removeVipBadge":
        this.removeVipBadge(ws, data[1], data[2]);
        break;
      case "getAllVipBadges":
        this.getAllVipBadges(ws, data[1]);
        break;
    }
  }

  sendVipBadge(ws, room, seat, numbadge, colortext) {
    try {
      // Validasi room dan seat
      if (!room || seat < 1 || seat > 35) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo || !seatInfo.namauser || seatInfo.namauser.startsWith("__LOCK__")) return false;

      // Update seat state
      seatInfo.vip = numbadge;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      // Update VIP badges storage
      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }
      
      this.vipBadges.get(room).set(seat, {
        badgeCount: numbadge,
        color: colortext,
        timestamp: Date.now()
      });

      // ✅ BROADCAST REAL-TIME KE SEMUA CLIENT DI ROOM
      this.chatServer.broadcastToRoom(room, [
        "vipbadge", 
        room, 
        seat, 
        numbadge, 
        colortext
      ]);

      return true;
    } catch (error) {
      return false;
    }
  }

  removeVipBadge(ws, room, seat) {
    try {
      if (!room || seat < 1 || seat > 35) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      // Update seat state
      seatInfo.vip = 0;
      seatInfo.viptanda = 0;
      seatInfo.lastActivity = Date.now();

      // Remove from VIP storage
      if (this.vipBadges.has(room)) {
        this.vipBadges.get(room).delete(seat);
      }

      // ✅ BROADCAST REAL-TIME REMOVAL
      this.chatServer.broadcastToRoom(room, [
        "removeVipBadge", 
        room, 
        seat
      ]);

      return true;
    } catch (error) {
      return false;
    }
  }

  getAllVipBadges(ws, room) {
    try {
      if (!room) return;

      const result = [];
      const roomVipData = this.vipBadges.get(room);
      
      if (roomVipData) {
        for (const [seat, vipData] of roomVipData) {
          result.push({
            seat: seat,
            badgeCount: vipData.badgeCount,
            color: vipData.color
          });
        }
      }

      // ✅ KIRIM KE REQUESTER SAJA
      this.chatServer.safeSend(ws, [
        "allVipBadges", 
        room, 
        result
      ]);

    } catch (error) {}
  }

  cleanupUserVipBadges(username) {
    try {
      for (const [room, seatMap] of this.vipBadges) {
        for (const [seat, vipData] of seatMap) {
          const seatInfo = this.chatServer.roomSeats.get(room)?.get(seat);
          if (seatInfo && seatInfo.namauser === username) {
            this.removeVipBadge(null, room, seat);
          }
        }
      }
    } catch (error) {}
  }

  // Get VIP data for specific room and seat
  getVipBadge(room, seat) {
    try {
      const roomVipData = this.vipBadges.get(room);
      return roomVipData ? roomVipData.get(seat) : null;
    } catch (error) {
      return null;
    }
  }

  // Get all VIP badges for room (internal use)
  getRoomVipBadges(room) {
    try {
      return this.vipBadges.get(room) || new Map();
    } catch (error) {
      return new Map();
    }
  }
}
