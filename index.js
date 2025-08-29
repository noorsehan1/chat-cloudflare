// ============================
// Cloudflare Workers + DO Chat (Smooth Points)
// ============================

const roomList = [
  "Chill Zone","Catch Up","Casual Vibes","Lounge Talk",
  "Easy Talk","Friendly Corner","The Hangout",
  "Relax & Chat","Just Chillin","The Chatter Room"
];

function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "",
    itembawah: 0, itematas: 0, vip: false, viptanda: 0,
    points: [], lockTime: undefined
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.pointUpdateBuffer = new Map();
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;

    // flush interval lebih pendek, semua di-handle di sini
    this._flushTimer = setInterval(()=>this.periodicFlush(), 100);
  }

  safeSend(ws, arr) {
    try {
      if(ws.readyState===1) ws.send(JSON.stringify(arr));
      else this.cleanupClient(ws);
    } catch(err) {
      console.error("safeSend error:", ws.idtarget, err);
      this.cleanupClient(ws);
    }
  }

  broadcastToRoom(room, msg) {
    for(const c of Array.from(this.clients)) {
      if (c.roomname === room) this.safeSend(c,msg);
    }
  }

  flushPointUpdates(){
    for(const [room,seatMap] of this.pointUpdateBuffer){
      for(const [seat,points] of seatMap){
        for(const p of points){ 
          try{ this.broadcastToRoom(room,["pointUpdated",room,seat,p.x,p.y,p.fast]); } 
          catch(e){ console.error("flushPointUpdates:",e); } 
        }
        points.length=0;
      }
    }
  }

  flushKursiUpdates(){
    for(const [room,seatMap] of this.updateKursiBuffer){
      const updates=[];
      for(const [seat,info] of seatMap){
        const {points,...rest}=info;
        updates.push([seat,rest]);
      }
      if(updates.length>0) this.broadcastToRoom(room,["kursiBatchUpdate",room,updates]);
      seatMap.clear();
    }
  }

  flushChatBuffer(){
    for(const [room,messages] of this.chatMessageBuffer){
      for(const msg of messages) this.broadcastToRoom(room,msg);
      messages.length=0;
    }
  }

  flushPrivateMessageBuffer(){
    for(const [idtarget,messages] of this.privateMessageBuffer){
      for(const c of Array.from(this.clients)){
        if(c.idtarget===idtarget) for(const msg of messages) this.safeSend(c,msg);
      }
      messages.length=0;
    }
  }

  cleanExpiredLocks(){
    const now=Date.now();
    for(const room of roomList){
      const seatMap=this.roomSeats.get(room);
      for(const [seat,info] of seatMap){
        if(String(info.namauser).startsWith("__LOCK__") && info.lockTime && now-info.lockTime>10000){
          Object.assign(info,createEmptySeat());
          this.broadcastToRoom(room,["removeKursi",room,seat]);
        }
      }
    }
  }

  periodicFlush(){
    try{
      this.flushPointUpdates();
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.flushPrivateMessageBuffer();
      this.cleanExpiredLocks(); // opsional, tetap aman
    } catch(err){ console.error("periodicFlush error:", err); }
  }

  lockSeat(room,ws){
    const seatMap=this.roomSeats.get(room);
    if(!ws.idtarget) return null;

    if(this.userToSeat.has(ws.idtarget)){
      const prev=this.userToSeat.get(ws.idtarget);
      if(prev.room===room) return prev.seat;
    }

    for(let i=1;i<=this.MAX_SEATS;i++){
      const k=seatMap.get(i);
      if(k && k.namauser===""){
        k.namauser="__LOCK__"+ws.idtarget;
        k.lockTime=Date.now();
        this.userToSeat.set(ws.idtarget,{room,seat:i});
        return i;
      }
    }
    return null;
  }

  removeAllSeatsById(idtarget){
    for (const [room, seatMap] of this.roomSeats) {
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
        }
      }
    }
  }

  handleMessage(ws,raw){
    let data;
    try{ data=JSON.parse(raw); } catch(e){ return this.safeSend(ws,["error","Invalid JSON"]); }
    if(!Array.isArray(data)||data.length===0) return this.safeSend(ws,["error","Invalid message format"]);
    const evt=data[0];

    try{
      switch(evt){
        case "setIdTarget": {
          const newId=data[1];
          this.cleanupClientById(newId);
          ws.idtarget=newId;
          this.safeSend(ws,["setIdTargetAck",ws.idtarget]);
          break;
        }

        case "joinRoom": {
          const newRoom=data[1];
          if(ws.idtarget) this.removeAllSeatsById(ws.idtarget);
          ws.roomname=newRoom;
          const seatMap=this.roomSeats.get(newRoom);
          const foundSeat=this.lockSeat(newRoom,ws);
          if(foundSeat===null) return this.safeSend(ws,["roomFull",newRoom]);
          ws.numkursi=new Set([foundSeat]);
          this.safeSend(ws,["numberKursiSaya",foundSeat]);
          this.userToSeat.set(ws.idtarget,{room:newRoom,seat:foundSeat});
          break;
        }

        case "updatePoint": {
          const [,room,seat,x,y,fast]=data;
          const seatMap=this.roomSeats.get(room);
          const si=seatMap.get(seat);
          si.points.push({x,y,fast});
          if(!this.pointUpdateBuffer.has(room)) this.pointUpdateBuffer.set(room,new Map());
          const roomBuf=this.pointUpdateBuffer.get(room);
          if(!roomBuf.has(seat)) roomBuf.set(seat,[]);
          roomBuf.get(seat).push({x,y,fast});
          break;
        }

        // … tambahkan case lain sama seperti kode kedua untuk chat, updateKursi, removeKursi
      }
    } catch(err){ console.error("handleMessage error:", ws.idtarget, err); }
  }

  cleanupClientById(idtarget){
    for(const c of Array.from(this.clients)) if(c.idtarget===idtarget) this.cleanupClient(c);
  }

  cleanupClient(ws){
    try{
      const id=ws.idtarget;
      if(id) this.removeAllSeatsById(id);
    } finally{
      this.clients.delete(ws);
      ws.numkursi?.clear?.();
      ws.roomname=undefined;
      ws.idtarget=undefined;
    }
  }

  async fetch(request){
    const upgrade=request.headers.get("Upgrade")||"";
    if(upgrade.toLowerCase()!=="websocket") return new Response("Expected WebSocket",{status:426});
    const pair=new WebSocketPair();
    const [client,server]=Object.values(pair);
    server.accept();
    const ws=server;
    ws.roomname=undefined; ws.idtarget=undefined; ws.numkursi=new Set();
    this.clients.add(ws);
    ws.addEventListener("message",(ev)=>this.handleMessage(ws,ev.data));
    ws.addEventListener("close",()=>this.cleanupClient(ws));
    ws.addEventListener("error",()=>this.cleanupClient(ws));
    return new Response(null,{status:101,webSocket:client});
  }
}
