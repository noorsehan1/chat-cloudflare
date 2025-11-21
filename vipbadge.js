// VIP Badge Manager Class - Simple Version
export class VipBadgeManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    // Structure: Map(room -> Map(seat -> vipData))
    this.vipBadges = new Map();
  }

  /**
   * 1. SEND VIP BADGE - Kirim VIP badge ke kursi tertentu dan broadcast real-time
   * @param {string} room - Nama room
   * @param {number} seat - Nomor kursi (1-35)
   * @param {number} numbadge - Jumlah badge
   * @param {string} colortext - Warna badge
   */
  sendVipBadge(room, seat, numbadge, colortext) {
    try {
      // Validasi basic
      if (!room || seat < 1 || seat > 35) {
        console.warn(`❌ Invalid room or seat: ${room}, ${seat}`);
        return false;
      }

      // Dapatkan seat map dari chat server
      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) {
        console.warn(`❌ Room tidak ditemukan: ${room}`);
        return false;
      }

      const seatInfo = seatMap.get(seat);
      if (!seatInfo || !seatInfo.namauser || seatInfo.namauser.startsWith("__LOCK__")) {
        console.warn(`❌ Kursi ${seat} kosong atau tidak ada user`);
        return false;
      }

      console.log(`🎯 SEND VIP: Room=${room}, Seat=${seat}, User=${seatInfo.namauser}, Badges=${numbadge}, Color=${colortext}`);

      // Update data di seat
      seatInfo.vip = numbadge;
      seatInfo.viptanda = 1;
      seatInfo.lastActivity = Date.now();

      // Simpan di VIP storage
      if (!this.vipBadges.has(room)) {
        this.vipBadges.set(room, new Map());
      }

      this.vipBadges.get(room).set(seat, {
        badgeCount: numbadge,
        color: colortext,
        username: seatInfo.namauser,
        timestamp: Date.now()
      });

      // ✅ BROADCAST REAL-TIME KE SEMUA USER DI ROOM
      const vipMessage = ["vipbadge", room, seat, numbadge, colortext];
      const sentCount = this.chatServer.broadcastToRoom(room, vipMessage);

      console.log(`✅ VIP SENT: Room=${room}, Seat=${seat}, Sent to ${sentCount} users`);

      return true;

    } catch (error) {
      console.error("❌ Error sendVipBadge:", error);
      return false;
    }
  }

  /**
   * 2. REMOVE VIP BADGE - Hapus VIP badge dari kursi tertentu
   * @param {string} room - Nama room
   * @param {number} seat - Nomor kursi (1-35)
   */
  removeVipBadge(room, seat) {
    try {
      if (!room || seat < 1 || seat > 35) {
        return false;
      }

      const seatMap = this.chatServer.roomSeats.get(room);
      if (!seatMap) return false;

      const seatInfo = seatMap.get(seat);
      if (!seatInfo) return false;

      console.log(`🗑️ REMOVE VIP: Room=${room}, Seat=${seat}, User=${seatInfo.namauser}`);

      // Reset data di seat
      seatInfo.vip = 0;
      seatInfo.viptanda = 0;
      seatInfo.lastActivity = Date.now();

      // Hapus dari VIP storage
      if (this.vipBadges.has(room)) {
        this.vipBadges.get(room).delete(seat);
      }

      // ✅ BROADCAST REMOVAL REAL-TIME KE SEMUA USER DI ROOM
      const removeMessage = ["removeVipBadge", room, seat];
      const sentCount = this.chatServer.broadcastToRoom(room, removeMessage);

      console.log(`✅ VIP REMOVED: Room=${room}, Seat=${seat}, Sent to ${sentCount} users`);

      return true;

    } catch (error) {
      console.error("❌ Error removeVipBadge:", error);
      return false;
    }
  }

  /**
   * 3. GET ALL VIP BADGES - Ambil semua data VIP badge berdasarkan kursi di room tertentu
   * @param {string} room - Nama room
   * @returns {Array} Data VIP badge per kursi
   */
  getAllVipBadges(room) {
    try {
      if (!room) return [];

      const result = [];
      const seatMap = this.chatServer.roomSeats.get(room);
      
      if (!seatMap) return [];

      // Loop semua kursi 1-35
      for (let seat = 1; seat <= 35; seat++) {
        const seatInfo = seatMap.get(seat);
        const vipData = this.vipBadges.get(room)?.get(seat);

        if (seatInfo && seatInfo.viptanda > 0 && vipData) {
          result.push({
            seat: seat,
            badgeCount: seatInfo.vip,
            username: seatInfo.namauser,
            color: vipData.color,
            timestamp: vipData.timestamp
          });
        }
      }

      console.log(`📊 GET ALL VIP: Room=${room}, Found ${result.length} VIP badges`);
      return result;

    } catch (error) {
      console.error("❌ Error getAllVipBadges:", error);
      return [];
    }
  }

  /**
   * Get VIP badge untuk kursi tertentu
   * @param {string} room - Nama room
   * @param {number} seat - Nomor kursi (1-35)
   * @returns {Object} Data VIP badge
   */
  getVipBadge(room, seat) {
    try {
      if (!room || seat < 1 || seat > 35) return null;

      const seatInfo = this.chatServer.roomSeats.get(room)?.get(seat);
      const vipData = this.vipBadges.get(room)?.get(seat);

      if (!seatInfo || seatInfo.viptanda === 0 || !vipData) {
        return null;
      }

      return {
        seat: seat,
        badgeCount: seatInfo.vip,
        username: seatInfo.namauser,
        color: vipData.color,
        timestamp: vipData.timestamp
      };

    } catch (error) {
      console.error("❌ Error getVipBadge:", error);
      return null;
    }
  }

  /**
   * Cleanup VIP badges ketika user keluar
   * @param {string} username - Username yang keluar
   */
  cleanupUserVipBadges(username) {
    try {
      for (const [room, seatMap] of this.vipBadges) {
        for (const [seat, vipData] of seatMap) {
          if (vipData.username === username) {
            this.removeVipBadge(room, seat);
          }
        }
      }
    } catch (error) {
      console.error("❌ Error cleanupUserVipBadges:", error);
    }
  }
}
