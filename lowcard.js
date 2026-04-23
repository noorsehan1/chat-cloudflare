async handleJoinRoom(ws, room) {
  if (!ws?.idtarget) {
    await this.safeSend(ws, ["error", "User ID not set"]);
    return false;
  }
  if (!roomList.includes(room)) {
    await this.safeSend(ws, ["error", "Invalid room"]);
    return false;
  }

  let release;
  try {
    release = await this.roomLock.acquire();
  } catch(e) {
    await this.safeSend(ws, ["error", "Server busy"]);
    return false;
  }
  
  try {
    const oldRoom = ws.roomname;
    
    if (oldRoom && oldRoom !== room) {
      // ... hapus dari room lama (sama seperti kode Anda)
      const oldClientSet = this.roomClients.get(oldRoom);
      if (oldClientSet) oldClientSet.delete(ws);
      
      const oldRoomManager = this.roomManagers.get(oldRoom);
      if (oldRoomManager) {
        let oldSeat = null;
        for (const [seat, seatData] of oldRoomManager.seats) {
          if (seatData && seatData.namauser === ws.idtarget) {
            oldSeat = seat;
            break;
          }
        }
        if (oldSeat) {
          oldRoomManager.removeSeat(oldSeat);
          this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.updateRoomCount(oldRoom);
        }
      }
      
      const currentSeatInfo = this.userToSeat.get(ws.idtarget);
      if (currentSeatInfo && currentSeatInfo.room === oldRoom) {
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
    }
    
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) {
      if (release) release();
      return false;
    }

    let assignedSeat = null;
    const existingSeatInfo = this.userToSeat.get(ws.idtarget);
    
    if (existingSeatInfo && existingSeatInfo.room === room) {
      const seatNum = existingSeatInfo.seat;
      const seatData = roomManager.getSeat(seatNum);
      if (seatData && seatData.namauser === ws.idtarget) {
        assignedSeat = seatNum;
      }
    }
    
    if (!assignedSeat) {
      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        if (release) release();
        return false;
      }
      assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        if (release) release();
        return false;
      }
    }

    ws.roomname = room;
    
    let clientSet = this.roomClients.get(room);
    if (!clientSet) {
      clientSet = new Set();
      this.roomClients.set(room, clientSet);
    }
    clientSet.add(ws);

    let userConns = this.userConnections.get(ws.idtarget);
    if (!userConns) {
      userConns = new Set();
      this.userConnections.set(ws.idtarget, userConns);
    }
    userConns.add(ws);

    // ========== KIRIM KE USER BARU ==========
    await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
    await new Promise(resolve => setTimeout(resolve, 100));
    await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
    await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
    await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    
    // ========== KIRIM SEMUA DATA USER LAIN KE USER BARU ==========
    // Dapatkan semua data kursi dari room (termasuk semua user)
    const allSeatsMeta = roomManager.getAllSeatsMeta();
    const allPoints = roomManager.getAllPoints();
    
    // Kirim semua kursi ke user baru (excludeSelfSeat = false)
    if (Object.keys(allSeatsMeta).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, allSeatsMeta]);
    }
    
    // Kirim semua points ke user baru
    if (allPoints.length > 0) {
      await this.safeSend(ws, ["allPointsList", room, allPoints]);
    }
    
    // ========== BROADCAST KE USER LAIN bahwa ada user baru ==========
    // Kirim hanya data kursi user baru ke semua user lain
    const newSeatData = roomManager.getSeat(assignedSeat);
    if (newSeatData) {
      this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[assignedSeat, {
        noimageUrl: newSeatData.noimageUrl,
        namauser: newSeatData.namauser,
        color: newSeatData.color,
        itembawah: newSeatData.itembawah,
        itematas: newSeatData.itematas,
        vip: newSeatData.vip,
        viptanda: newSeatData.viptanda
      }]]]);
    }
    
    // Update jumlah user di room untuk semua client
    this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);

    const point = roomManager.getPoint(assignedSeat);
    if (point) {
      await this.safeSend(ws, ["pointUpdated", room, assignedSel, point.x, point.y, point.fast ? 1 : 0]);
    }

    if (release) release();
    return true;
    
  } catch (error) {
    console.error(`[JOIN_ROOM] Error:`, error);
    await this.safeSend(ws, ["error", "Failed to join room"]);
    if (release) release();
    return false;
  }
}
