export class VipBadgeManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.vipBadges = new Map();
  }

  sendVipBadge(room, seat, numbadge, colortext) {
    try {
      if (!room || seat < 1 || seat > 35) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo || !seatInfo.namauser || seatInfo.namauser.startsWith("__LOCK__")) return false;

      seatInfo.vip = numbadge;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }

      this.vipBadges.get(room).set(seat, {
        badgeCount: numbadge,
        color: colortext
      });

      const vipMessage = ["vipbadge", room, seat, numbadge, colortext];
      this.chatServer.broadcastToRoom(room, vipMessage);

      return true;
    } catch (error) {
      return false;
    }
  }

  removeVipBadge(room, seat) {
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

      const removeMessage = ["removeVipBadge", room, seat];
      this.chatServer.broadcastToRoom(room, removeMessage);

      return true;
    } catch (error) {
      return false;
    }
  }

  getAllVipBadges(room) {
    try {
      if (!room) return [];

      const result = [];
      const roomVipData = this.vipBadges.get(room);
      
      if (!roomVipData) return [];

      for (const [seat, vipData] of roomVipData) {
        result.push({
          seat: seat,
          badgeCount: vipData.badgeCount,
          color: vipData.color
        });
      }

      return result;
    } catch (error) {
      return [];
    }
  }

  cleanupUserVipBadges(username) {
    try {
      for (const [room, seatMap] of this.vipBadges) {
        for (const [seat, vipData] of seatMap) {
          const seatInfo = this.chatServer.roomSeats.get(room)?.get(seat);
          if (seatInfo && seatInfo.namauser === username) {
            this.removeVipBadge(room, seat);
          }
        }
      }
    } catch (error) {}
  }
}
