case "lockSeat": {
  const [, room, seat] = data;
  if (ws.roomname !== room || !ws.idtarget) return;
  
  const seatMap = this.roomSeats.get(room);
  const seatInfo = seatMap.get(seat);
  
  if (seatInfo && seatInfo.namauser === ws.idtarget) {
    this.lockSeat(room, seat, ws.idtarget);
    this.safeSend(ws, ['seatLocked', room, seat]);
  }
  break;
}

case "unlockSeat": {
  const [, room, seat] = data;
  if (ws.roomname !== room || !ws.idtarget) return;
  
  const seatMap = this.roomSeats.get(room);
  const seatInfo = seatMap.get(seat);
  
  if (seatInfo && seatInfo.namauser === ws.idtarget) {
    this.unlockSeat(room, seat);
    this.safeSend(ws, ['seatUnlocked', room, seat]);
  }
  break;
}
