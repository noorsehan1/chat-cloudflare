export class VipBadgeManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.vipBadges = new Map();
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

  // ============================================
  //   ALWAYS WRITE, ALWAYS BROADCAST
  // ============================================
  sendVipBadge(ws, room, seat, numbadge, colortext) {
    try {
      if (!room || seat < 1 || seat > 35) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      // ✔ selalu update, tanpa lock dan tanpa cek sama
      seatInfo.vip = numbadge;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      // ✔ siapkan penyimpanan room
      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }

      // ✔ simpan berdasarkan nomor kursi
      this.vipBadges.get(room).set(seat, {
        badgeCount: numbadge,
        color: colortext
      });

      // ✔ broadcast selalu
      const vipMessage = ["vipbadge", room, seat, numbadge, colortext];
      this.chatServer.broadcastToRoom(room, vipMessage);

      return true;
    } catch (err) {
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

      seatInfo.vip = 0;
      seatInfo.viptanda = 0;
      seatInfo.lastActivity = Date.now();

      if (this.vipBadges.has(room)) {
        this.vipBadges.get(room).delete(seat);
      }

      const msg = ["removeVipBadge", room, seat];
      this.chatServer.broadcastToRoom(room, msg);

      return true;
    } catch (err) {
      return false;
    }
  }

  getAllVipBadges(ws, room) {
    try {
      if (!room) return;

      const roomData = this.vipBadges.get(room);
      const result = [];

      if (roomData) {
        for (const [seat, vipData] of roomData) {
          result.push({
            seat: seat,
            badgeCount: vipData.badgeCount,
            color: vipData.color
          });
        }
      }

      this.chatServer.safeSend(ws, ["allVipBadges", room, result]);
    } catch (err) {}
  }

  // OPTIONAL kalau kamu mau hapus vip berdasarkan user
  cleanupUserVipBadges(username) {
    try {
      for (const [room, seatMap] of this.vipBadges) {
        for (const [seat] of seatMap) {
          const seatInfo = this.chatServer.roomSeats.get(room)?.get(seat);
          if (seatInfo && seatInfo.namauser === username) {
            this.removeVipBadge(null, room, seat);
          }
        }
      }
    } catch (err) {}
  }
}
