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

  // ======================================================
  //    ALWAYS UPDATE - ALWAYS BROADCAST - NO FILTERS
  // ======================================================
  sendVipBadge(ws, room, seat, numbadge, colortext) {
    try {
      if (!room || seat < 1 || seat > 35) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      // ✔ Selalu update kursi
      seatInfo.vip = numbadge;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      // ✔ Siapkan penyimpanan room jika belum ada
      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }

      // ✔ Simpan (overwrite) berdasarkan nomor seat
      this.vipBadges.get(room).set(seat, {
        badgeCount: numbadge,
        color: colortext,
        updateAt: Date.now() // dipakai untuk memaksa perubahan
      });

      // ======================================================
      //   FIX 100% BROADCAST — timestamp memastikan berubah
      // ======================================================
      const vipMessage = [
        "vipbadge",
        room,
        seat,
        numbadge,
        colortext,
        Date.now() // memaksa client menerima update
      ];

      this.chatServer.broadcastToRoom(room, vipMessage);

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

      seatInfo.vip = 0;
      seatInfo.viptanda = 0;
      seatInfo.lastActivity = Date.now();

      if (this.vipBadges.has(room)) {
        this.vipBadges.get(room).delete(seat);
      }

      const msg = ["removeVipBadge", room, seat, Date.now()];

      this.chatServer.broadcastToRoom(room, msg);

      return true;
    } catch (error) {
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
            seat,
            badgeCount: vipData.badgeCount,
            color: vipData.color,
            updateAt: vipData.updateAt
          });
        }
      }

      this.chatServer.safeSend(ws, ["allVipBadges", room, result]);
    } catch (error) {}
  }

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
    } catch (error) {}
  }
}
