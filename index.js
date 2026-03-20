async handleSetIdTarget2(ws, id, baru) {
  if (!id || !ws) return;
  
  try {
    await this.withLock(`reconnect-${id}`, async () => {
      this.cancelCleanup(id);
      
      if (baru === true) {
        await this.cleanupQueue.add(async () => {
          await this.forceUserCleanup(id);
        });
        
        ws.idtarget = id;
        ws.roomname = undefined;
        ws.numkursi = new Set();
        ws._connectionTime = Date.now();
        
        this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._connectionTime = Date.now();
      
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo) {
        const { room, seat } = seatInfo;
        
        if (seat < 1 || seat > this.MAX_SEATS) {
          this.userToSeat.delete(id);
          this.userCurrentRoom.delete(id);
          this.safeSend(ws, ["needJoinRoom"]);
          return;
        }
        
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (seatMap && occupancyMap) {
          const seatData = seatMap.get(seat);
          const occupantId = occupancyMap.get(seat);
          
          if (seatData?.namauser === id && occupantId === id) {
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            
            const clientArray = this.roomClients.get(room);
            if (clientArray && !clientArray.includes(ws)) {
              clientArray.push(ws);
            }
            
            this._addUserConnection(id, ws);
            this.sendAllStateTo(ws, room);
            
            if (seatData.lastPoint) {
              this.safeSend(ws, [
                "pointUpdated", 
                room, 
                seat, 
                seatData.lastPoint.x, 
                seatData.lastPoint.y, 
                seatData.lastPoint.fast
              ]);
            }
            
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
            this.updateRoomCount(room);
            return;
          }
        }
        
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
        
        if (seatInfo.room) {
          await this.cleanupQueue.add(async () => {
            await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, id, true);
          });
        }
      }
      
      this.safeSend(ws, ["needJoinRoom"]);
    });
    
  } catch {
    this.safeSend(ws, ["error", "Reconnection failed"]);
  }
}
