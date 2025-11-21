// VIP Badge Manager Class
export class VipBadgeManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.vipBadges = new Map(); // room -> Map(seat -> vipData)
    this.vipHistory = new Map(); // user -> vip history
  }

  /**
   * Mengirim VIP badge ke client berdasarkan room dan seat
   * @param {string} room - Nama room
   * @param {number} seat - Nomor seat
   * @param {number} numbadge - Jumlah badge
   * @param {string} colortext - Warna teks badge
   */
  sendVipBadge(room, seat, numbadge, colortext) {
    try {
      if (!this.chatServer.roomList.includes(room)) {
        console.warn(`Room ${room} tidak ditemukan`);
        return false;
      }

      if (seat < 1 || seat > this.chatServer.MAX_SEATS) {
        console.warn(`Seat ${seat} tidak valid`);
        return false;
      }

      // Get seat map dari chat server
      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      // Update informasi VIP di seat
      seatInfo.vip = numbadge;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      // Simpan data VIP badge
      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }
      this.vipBadges.get(room).set(seat, {
        badgeCount: numbadge,
        color: colortext,
        timestamp: Date.now(),
        username: seatInfo.namauser
      });

      // Simpan history untuk user
      if (seatInfo.namauser && !seatInfo.namauser.startsWith("__LOCK__")) {
        if (!this.vipHistory.has(seatInfo.namauser)) {
          this.vipHistory.set(seatInfo.namauser, []);
        }
        this.vipHistory.get(seatInfo.namauser).push({
          room,
          seat,
          badgeCount: numbadge,
          color: colortext,
          timestamp: Date.now()
        });
      }

      // ✅ BROADCAST LANGSUNG KE SEMUA USER DI ROOM
      const vipBadgeMessage = ["vipbadge", room, seat, numbadge, colortext];
      this.chatServer.broadcastToRoom(room, vipBadgeMessage);

      // Update buffer untuk konsistensi
      if (!this.chatServer.updateKursiBuffer.has(room)) {
        this.chatServer.updateKursiBuffer.set(room, new Map());
      }
      this.chatServer.updateKursiBuffer.get(room).set(seat, { 
        ...seatInfo, 
        points: [] 
      });

      console.log(`✅ VIP badge dikirim: Room=${room}, Seat=${seat}, Badges=${numbadge}, Color=${colortext}`);
      return true;

    } catch (error) {
      console.error("❌ Error sending VIP badge:", error);
      return false;
    }
  }

  /**
   * Remove VIP badge dari user tertentu
   * @param {string} room - Nama room
   * @param {number} seat - Nomor seat
   */
  removeVipBadge(room, seat) {
    try {
      if (!this.chatServer.roomList.includes(room)) return false;

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      // Reset VIP information
      seatInfo.vip = 0;
      seatInfo.viptanda = 0;
      seatInfo.lastActivity = Date.now();

      // Hapus dari VIP badges map
      if (this.vipBadges.has(room)) {
        this.vipBadges.get(room).delete(seat);
      }

      // ✅ BROADCAST REMOVAL KE SEMUA CLIENT
      const removeMessage = ["removeVipBadge", room, seat];
      this.chatServer.broadcastToRoom(room, removeMessage);

      // Update buffer
      if (!this.chatServer.updateKursiBuffer.has(room)) {
        this.chatServer.updateKursiBuffer.set(room, new Map());
      }
      this.chatServer.updateKursiBuffer.get(room).set(seat, { 
        ...seatInfo, 
        points: [] 
      });

      console.log(`🗑️ VIP badge dihapus: Room=${room}, Seat=${seat}`);
      return true;

    } catch (error) {
      console.error("❌ Error removing VIP badge:", error);
      return false;
    }
  }

  /**
   * Get VIP badge information untuk seat tertentu
   * @param {string} room - Nama room
   * @param {number} seat - Nomor seat
   * @returns {Object} VIP badge info
   */
  getVipBadgeInfo(room, seat) {
    try {
      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return null;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return null;

      const vipData = this.vipBadges.get(room)?.get(seat);

      return {
        hasVip: seatInfo.viptanda > 0,
        badgeCount: seatInfo.vip,
        seat: seat,
        room: room,
        username: seatInfo.namauser,
        color: vipData?.color || "",
        timestamp: vipData?.timestamp || 0
      };
    } catch (error) {
      console.error("❌ Error getting VIP badge info:", error);
      return null;
    }
  }

  /**
   * Get semua VIP badges dalam room tertentu
   * @param {string} room - Nama room
   * @returns {Array} List of VIP badges
   */
  getAllVipBadgesInRoom(room) {
    try {
      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return [];

      const vipBadges = [];
      for (const [seat, seatInfo] of seatMap) {
        if (seatInfo.viptanda > 0 && seatInfo.vip > 0) {
          const vipData = this.vipBadges.get(room)?.get(seat);
          vipBadges.push({
            seat: seat,
            badgeCount: seatInfo.vip,
            username: seatInfo.namauser,
            color: vipData?.color || "gold",
            timestamp: vipData?.timestamp || Date.now()
          });
        }
      }
      return vipBadges;
    } catch (error) {
      console.error("❌ Error getting all VIP badges:", error);
      return [];
    }
  }

  /**
   * Get VIP history untuk user tertentu
   * @param {string} username - Username
   * @param {number} limit - Jumlah history yang diambil
   * @returns {Array} VIP history
   */
  getUserVipHistory(username, limit = 10) {
    try {
      if (!this.vipHistory.has(username)) {
        return [];
      }
      
      const history = this.vipHistory.get(username);
      return history
        .sort((a, b) => b.timestamp - a.timestamp)
        .slice(0, limit);
    } catch (error) {
      console.error("❌ Error getting user VIP history:", error);
      return [];
    }
  }

  /**
   * Update VIP badge count untuk seat tertentu
   * @param {string} room - Nama room
   * @param {number} seat - Nomor seat
   * @param {number} newCount - Jumlah badge baru
   */
  updateVipBadgeCount(room, seat, newCount) {
    try {
      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo || seatInfo.viptanda === 0) return false;

      const oldCount = seatInfo.vip;
      seatInfo.vip = newCount;
      seatInfo.lastActivity = Date.now();

      // Update VIP data
      if (this.vipBadges.has(room) && this.vipBadges.get(room).has(seat)) {
        this.vipBadges.get(room).get(seat).badgeCount = newCount;
      }

      // Broadcast update
      const vipData = this.vipBadges.get(room)?.get(seat);
      const updateMessage = ["vipbadge", room, seat, newCount, vipData?.color || "gold"];
      this.chatServer.broadcastToRoom(room, updateMessage);

      console.log(`🔄 VIP badge updated: Room=${room}, Seat=${seat}, ${oldCount} → ${newCount}`);
      return true;

    } catch (error) {
      console.error("❌ Error updating VIP badge count:", error);
      return false;
    }
  }

  /**
   * Get statistics VIP badges
   * @returns {Object} VIP statistics
   */
  getVipStatistics() {
    try {
      let totalBadges = 0;
      let activeBadges = 0;
      const roomStats = {};

      for (const [room, seatMap] of this.vipBadges) {
        roomStats[room] = seatMap.size;
        activeBadges += seatMap.size;
      }

      for (const history of this.vipHistory.values()) {
        totalBadges += history.length;
      }

      return {
        totalBadgesGiven: totalBadges,
        activeBadges: activeBadges,
        rooms: roomStats,
        uniqueUsers: this.vipHistory.size
      };
    } catch (error) {
      console.error("❌ Error getting VIP statistics:", error);
      return {
        totalBadgesGiven: 0,
        activeBadges: 0,
        rooms: {},
        uniqueUsers: 0
      };
    }
  }

  /**
   * Cleanup expired VIP badges (older than 24 hours)
   */
  cleanupExpiredBadges() {
    try {
      const now = Date.now();
      const twentyFourHours = 24 * 60 * 60 * 1000;
      let cleanedCount = 0;

      for (const [room, seatMap] of this.vipBadges) {
        for (const [seat, vipData] of seatMap) {
          if (now - vipData.timestamp > twentyFourHours) {
            this.removeVipBadge(room, seat);
            cleanedCount++;
          }
        }
      }

      if (cleanedCount > 0) {
        console.log(`🧹 Cleaned ${cleanedCount} expired VIP badges`);
      }

      return cleanedCount;
    } catch (error) {
      console.error("❌ Error cleaning expired VIP badges:", error);
      return 0;
    }
  }

  /**
   * Transfer VIP badge dari satu seat ke seat lain
   * @param {string} room - Nama room
   * @param {number} fromSeat - Seat asal
   * @param {number} toSeat - Seat tujuan
   */
  transferVipBadge(room, fromSeat, toSeat) {
    try {
      const fromInfo = this.getVipBadgeInfo(room, fromSeat);
      if (!fromInfo || !fromInfo.hasVip) {
        return false;
      }

      // Remove dari seat asal
      this.removeVipBadge(room, fromSeat);

      // Tambahkan ke seat tujuan
      return this.sendVipBadge(room, toSeat, fromInfo.badgeCount, fromInfo.color);
    } catch (error) {
      console.error("❌ Error transferring VIP badge:", error);
      return false;
    }
  }

  /**
   * Destroy dan cleanup resources
   */
  destroy() {
    this.vipBadges.clear();
    this.vipHistory.clear();
    console.log("♻️ VIP Badge Manager destroyed");
  }
}

export default VipBadgeManager;