// GANTI method assignNewSeat dengan versi yang lebih aman:
async assignNewSeat(room, userId) {
  const release = await this.seatLocker.acquire(`room_seat_assign_${room}`);
  try {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return null;

    // CEK: User sudah punya kursi di room ini?
    const existingSeatForUser = this.userToSeat.get(userId);
    if (existingSeatForUser && existingSeatForUser.room === room) {
      // Verifikasi kursi masih valid
      const seatData = roomManager.getSeat(existingSeatForUser.seat);
      if (seatData && seatData.namauser === userId) {
        return existingSeatForUser.seat; // Kembalikan kursi yang sudah ada
      } else {
        // Kursi tidak valid, hapus mapping
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
    }

    // CEK: User sudah punya kursi di room LAIN?
    const existingInOtherRoom = this.userToSeat.get(userId);
    if (existingInOtherRoom && existingInOtherRoom.room !== room) {
      // Hapus dari room lain dulu
      const otherRoomManager = this.roomManagers.get(existingInOtherRoom.room);
      if (otherRoomManager) {
        otherRoomManager.removeSeat(existingInOtherRoom.seat);
        otherRoomManager.removePoint(existingInOtherRoom.seat);
        this.broadcastToRoom(existingInOtherRoom.room, ["removeKursi", existingInOtherRoom.room, existingInOtherRoom.seat]);
        this.updateRoomCount(existingInOtherRoom.room);
      }
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
    }

    if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
      return null;
    }

    // Cari kursi yang benar-benar kosong dengan verifikasi ganda
    let newSeatNumber = null;
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      const seatData = roomManager.getSeat(seat);
      
      // Kursi kosong di roomManager?
      const isSeatEmptyInRoom = !seatData || !seatData.namauser || seatData.namauser === "";
      
      if (isSeatEmptyInRoom) {
        // Verifikasi tidak ada mapping zombie ke kursi ini
        let hasZombieMapping = false;
        for (const [uid, seatInfo] of this.userToSeat) {
          if (seatInfo.room === room && seatInfo.seat === seat) {
            hasZombieMapping = true;
            // Bersihkan mapping zombie
            this.userToSeat.delete(uid);
            this.userCurrentRoom.delete(uid);
            break;
          }
        }
        
        if (!hasZombieMapping) {
          newSeatNumber = seat;
          break;
        }
      }
    }
    
    if (!newSeatNumber) return null;

    // Double check: pastikan kursi masih kosong setelah verifikasi
    const finalCheck = roomManager.getSeat(newSeatNumber);
    if (finalCheck && finalCheck.namauser && finalCheck.namauser !== "") {
      return null; // Kursi tiba-tiba terisi!
    }

    const success = roomManager.updateSeat(newSeatNumber, {
      noimageUrl: "", 
      namauser: userId, 
      color: "", 
      itembawah: 0,
      itematas: 0, 
      vip: 0, 
      viptanda: 0
    });
    
    if (!success) return null;

    this.userToSeat.set(userId, { room, seat: newSeatNumber });
    this.userCurrentRoom.set(userId, room);

    this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
    this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);

    return newSeatNumber;
  } finally {
    release();
  }
}

// TAMBAHKAN method baru untuk force cleanup user tertentu:
async _forceCleanupUserFromAllRooms(userId) {
  if (!userId) return;
  
  // Hapus dari semua room
  for (const [roomName, roomManager] of this.roomManagers) {
    let removed = false;
    for (const [seatNum, seatData] of roomManager.seats) {
      if (seatData && seatData.namauser === userId) {
        roomManager.removeSeat(seatNum);
        roomManager.removePoint(seatNum);
        removed = true;
        this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
      }
    }
    if (removed) {
      this.updateRoomCount(roomName);
    }
  }
  
  // Hapus semua mapping
  this.userToSeat.delete(userId);
  this.userCurrentRoom.delete(userId);
}

// UPDATE method _forceCleanupStaleData menjadi lebih agresif:
async _forceCleanupStaleData() {
  let cleanedCount = 0;
  let seatCleanedCount = 0;
  
  // 1. Bersihkan userToSeat yang tidak valid
  for (const [userId, seatInfo] of this.userToSeat) {
    const roomManager = this.roomManagers.get(seatInfo.room);
    if (!roomManager) {
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      cleanedCount++;
      continue;
    }
    
    const seatData = roomManager.getSeat(seatInfo.seat);
    if (!seatData || seatData.namauser !== userId) {
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      cleanedCount++;
    }
  }
  
  // 2. Bersihkan kursi yang tidak memiliki koneksi aktif DAN tidak dalam grace period
  const now = Date.now();
  for (const [roomName, roomManager] of this.roomManagers) {
    let changed = false;
    for (const [seatNum, seatData] of roomManager.seats) {
      if (seatData && seatData.namauser) {
        const hasActiveConnection = this.userConnections.has(seatData.namauser);
        const hasValidMapping = this.userToSeat.has(seatData.namauser);
        const isInGracePeriod = this._reconnectingUsers.has(seatData.namauser);
        
        // Hapus jika: tidak ada koneksi, tidak ada mapping, dan tidak dalam grace period
        if (!hasActiveConnection && !hasValidMapping && !isInGracePeriod) {
          roomManager.removeSeat(seatNum);
          roomManager.removePoint(seatNum);
          this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
          changed = true;
          seatCleanedCount++;
        }
        
        // Juga hapus jika mapping ada tapi sudah sangat lama (lebih dari 2 menit) tanpa koneksi
        if (hasValidMapping && !hasActiveConnection && !isInGracePeriod) {
          const seatAge = now - (seatData.lastUpdated || now);
          if (seatAge > 120000) { // 2 menit
            roomManager.removeSeat(seatNum);
            roomManager.removePoint(seatNum);
            this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
            this.userToSeat.delete(seatData.namauser);
            this.userCurrentRoom.delete(seatData.namauser);
            changed = true;
            seatCleanedCount++;
          }
        }
      }
    }
    if (changed) {
      this.updateRoomCount(roomName);
    }
  }
  
  // 3. Logging jika ada pembersihan
  if (cleanedCount > 0 || seatCleanedCount > 0) {
    console.log(`[CLEANUP] Removed ${cleanedCount} stale mappings, ${seatCleanedCount} stale seats`);
  }
}

// UPDATE method handleJoinRoom dengan lock yang lebih ketat:
async handleJoinRoom(ws, room) {
  if (!ws?.idtarget) {
    await this.safeSend(ws, ["error", "User ID not set"]);
    return false;
  }
  if (!roomList.includes(room)) {
    await this.safeSend(ws, ["error", "Invalid room"]);
    return false;
  }

  // Gunakan lock untuk seluruh proses join room
  const release = await this.roomLocker.acquire(`join_room_${ws.idtarget}`);
  try {
    // CLEANUP TOTAL: Hapus user dari MANA PUN sebelum join
    await this._forceCleanupUserFromAllRooms(ws.idtarget);
    
    // Reset ws state
    const oldRoom = ws.roomname;
    if (oldRoom) {
      const oldClientSet = this.roomClients.get(oldRoom);
      if (oldClientSet) oldClientSet.delete(ws);
    }
    ws.roomname = undefined;
    
    // Cek room penuh
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) {
      await this.safeSend(ws, ["error", "Room not found"]);
      return false;
    }
    
    if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }

    // Assign seat baru
    const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
    if (!assignedSeat) {
      // Jika gagal, coba sekali lagi setelah delay pendek
      await new Promise(resolve => setTimeout(resolve, 100));
      const retrySeat = await this.assignNewSeat(room, ws.idtarget);
      if (!retrySeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      ws.roomname = room;
    } else {
      ws.roomname = room;
    }

    // Update clients
    let clientSet = this.roomClients.get(room);
    if (!clientSet) {
      clientSet = new Set();
      this.roomClients.set(room, clientSet);
    }
    clientSet.add(ws);

    let userConnections = this.userConnections.get(ws.idtarget);
    if (!userConnections) {
      userConnections = new Set();
      this.userConnections.set(ws.idtarget, userConnections);
    }
    userConnections.add(ws);
    this._activeClients.add(ws);

    // Kirim response ke client
    const finalSeat = this.userToSeat.get(ws.idtarget)?.seat || assignedSeat;
    await this.safeSend(ws, ["rooMasuk", finalSeat, room]);
    await new Promise(resolve => setTimeout(resolve, 500));
    await this.safeSend(ws, ["numberKursiSaya", finalSeat]);
    await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
    await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    await this.sendAllStateTo(ws, room);

    return true;
  } catch (error) {
    console.error(`Join room error for ${ws.idtarget}:`, error);
    await this.safeSend(ws, ["error", "Failed to join room"]);
    return false;
  } finally {
    release();
  }
}

// UPDATE method _masterTick untuk cleanup lebih sering:
async _masterTick() {
  if (this._isClosing) return;
  this._masterTickCounter++;
  const now = Date.now();

  try {
    if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
      this._handleNumberTick().catch(() => {});
    }

    if (this.chatBuffer) this.chatBuffer.tick(now);

    // Cleanup lebih sering - setiap 15 tick (15 detik)
    if (this._masterTickCounter % 15 === 0) {
      await this._forceCleanupStaleData();
    }
    
    if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
      this._checkConnectionPressure().catch(() => {});
      this._sweepMessageCounts();
      this._checkMemoryPressure();
    }
    
    if (this._masterTickCounter % CONSTANTS.EMERGENCY_SWEEP_INTERVAL_TICKS === 0) {
      this._emergencySweep();
    }

    if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
      try {
        const result = this.lowcard.masterTick();
        if (result && typeof result.catch === 'function') {
          result.catch((err) => {
            console.error("[LowCard] masterTick promise error:", err);
          });
        }
      } catch (syncError) {
        console.error("[LowCard] masterTick sync error:", syncError);
      }
    }
  } catch (error) {
    console.error("[MasterTick] Unhandled error:", error);
  }
}
