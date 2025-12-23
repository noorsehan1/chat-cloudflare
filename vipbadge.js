export class VipBadgeManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.vipBadges = new Map();
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "vipbadge":
        // Support both old (5 params) and new (6 params) versions
        if (data.length === 6) {
          this.sendVipBadge(ws, data[1], data[2], data[3], data[4], data[5]);
        } else {
          this.sendVipBadge(ws, data[1], data[2], data[3], data[4], null);
        }
        break;
      case "removeVipBadge":
        this.removeVipBadge(ws, data[1], data[2]);
        break;
      case "getAllVipBadges":
        this.getAllVipBadges(ws, data[1]);
        break;
    }
  }

  sendVipBadge(ws, room, seat, numbadge, colortext, bingkai = null) {
    try {
      if (!room || seat < 1 || seat > 35) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      let badgeString;
      
      if (numbadge === null || numbadge === undefined) {
        badgeString = "0";
      } else if (typeof numbadge === 'number') {
        badgeString = numbadge.toString();
      } else if (typeof numbadge === 'string') {
        badgeString = numbadge;
      } else {
        badgeString = String(numbadge);
      }

      if (badgeString.trim() === "") {
        badgeString = "0";
      }

      seatInfo.vip = badgeString;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }

      this.vipBadges.get(room).set(seat, {
        badgeCount: badgeString,
        color: colortext,
        bingkai: bingkai || null, // Store bingkai parameter
        updateAt: Date.now()
      });

      const vipMessage = [
        "vipbadge",
        room,
        seat,
        badgeString,
        colortext,
        bingkai || null, // Include bingkai in broadcast
        Date.now()
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

      seatInfo.vip = "0";
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
            bingkai: vipData.bingkai || null, // Include bingkai in response
            updateAt: vipData.updateAt
          });
        }
      }

      this.chatServer.safeSend(ws, ["allVipBadges", room, result]);
    } catch (error) {
    }
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
    } catch (error) {
    }
  }

  getVipBadge(room, seat) {
    try {
      if (!room || seat < 1 || seat > 35) return null;
      
      const roomData = this.vipBadges.get(room);
      if (!roomData) return null;
      
      return roomData.get(seat) || null;
    } catch (error) {
      return null;
    }
  }

  hasVipBadge(room, seat) {
    try {
      if (!room || seat < 1 || seat > 35) return false;
      
      const roomData = this.vipBadges.get(room);
      if (!roomData) return false;
      
      return roomData.has(seat);
    } catch (error) {
      return false;
    }
  }

  getRoomVipBadges(room) {
    try {
      if (!room) return new Map();
      
      return this.vipBadges.get(room) || new Map();
    } catch (error) {
      return new Map();
    }
  }

  cleanupOldBadges(maxAge = 24 * 60 * 60 * 1000) {
    try {
      const now = Date.now();
      let cleanedCount = 0;

      for (const [room, seatMap] of this.vipBadges) {
        for (const [seat, badgeData] of seatMap) {
          if (now - badgeData.updateAt > maxAge) {
            this.removeVipBadge(null, room, seat);
            cleanedCount++;
          }
        }
      }

      return cleanedCount;
    } catch (error) {
      return 0;
    }
  }

  // New method to update bingkai only
  updateBingkai(ws, room, seat, bingkai) {
    try {
      if (!room || seat < 1 || seat > 35) return false;

      const roomData = this.vipBadges.get(room);
      if (!roomData) return false;

      const vipData = roomData.get(seat);
      if (!vipData) return false;

      // Update bingkai
      vipData.bingkai = bingkai || null;
      vipData.updateAt = Date.now();

      // Broadcast update
      const updateMessage = [
        "updateBingkai",
        room,
        seat,
        bingkai || null,
        Date.now()
      ];

      this.chatServer.broadcastToRoom(room, updateMessage);

      return true;
    } catch (error) {
      return false;
    }
  }
}
