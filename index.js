// ============================
// Cloudflare Workers + DO Chat
// ============================

// ---- Konstanta Room ----
const roomList = [
  "Chill Zone","Catch Up","Casual Vibes","Lounge Talk",
  "Easy Talk","Friendly Corner","The Hangout",
  "Relax & Chat","Just Chillin","The Chatter Room"
];

// ---- Util seat ----
function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "",
    itembawah: 0, itematas: 0, vip: false, viptanda: 0,
    points: [], lockTime: undefined
  };
}

// =====================
// Durable Object Server
// =====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set(); // ws augmented: {roomname, idtarget, numkursi:Set<number>}
    this.userToSeat = new Map(); // idtarget -> { room, seat }

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.pointUpdateBuffer = new Map();   // room -> Map(seat -> [{x,y,fast}])
    this.updateKursiBuffer = new Map();   // room -> Map(seat -> seatInfo)
    this.chatMessageBuffer = new Map();   // room -> [msg...]
    this.privateMessageBuffer = new Map();// idtarget -> [msg...]

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15*60*1000;

    this._tickTimer = setInterval(()=>this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(()=>this.periodicFlush(), 100);
  }

  // ---------- Helpers ----------
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
      if (c.roomname === room) {
        try { this.safeSend(c,msg); } 
        catch(e) { console.error("broadcastToRoom error:", e); }
      }
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r=>[r,0]));
    for(const room of roomList){
      const seatMap=this.roomSeats.get(room);
      for(const info of seatMap.values()){
        if(info.namauser && !String(info.namauser).startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room){
    const count=this.getJumlahRoom()[room]||0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushPrivateMessageBuffer(){
    for(const [idtarget,messages] of this.privateMessageBuffer){
      for(const c of Array.from(this.clients)){
        if(c.idtarget===idtarget){
          for(const msg of messages){ 
            try{ this.safeSend(c,msg); } 
            catch(e){ console.error("flushPrivateMessageBuffer:", e); } 
          }
        }
      }
      messages.length=0;
    }
  }

  flushChatBuffer(){
    for(const [room,messages] of this.chatMessageBuffer){
      for(const msg of messages){ 
        try{ this.broadcastToRoom(room,msg); } 
        catch(e){ console.error("flushChatBuffer:", e); } 
      }
      messages.length=0;
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
      if(updates.length>0){ 
        try{ this.broadcastToRoom(room,["kursiBatchUpdate",room,updates]); } 
        catch(e){ console.error("flushKursiUpdates:", e); }
      }
      seatMap.clear();
    }
  }

  tick(){
    this.currentNumber=this.currentNumber<this.maxNumber?this.currentNumber+1:1;
    for(const c of Array.from(this.clients)){ 
      try{ this.safeSend(c,["currentNumber",this.currentNumber]); } 
      catch(e){ console.error("tick error:", e); } 
    }
  }

  cleanExpiredLocks(){
    const now=Date.now();
    for(const room of roomList){
      const seatMap=this.roomSeats.get(room);
      for(const [seat,info] of seatMap){
        if(String(info.namauser).startsWith("__LOCK__") && info.lockTime && now-info.lockTime>10000){
          Object.assign(info,createEmptySeat());
          try{ this.broadcastToRoom(room,["removeKursi",room,seat]); } 
          catch(e){ console.error("cleanExpiredLocks:", e); }
          this.broadcastRoomUserCount(room);
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
      this.cleanExpiredLocks();
    } catch(err){ console.error("periodicFlush error:", err); }
  }

  handleGetAllRoomsUserCount(ws){
    const allCounts=this.getJumlahRoom();
    const result=roomList.map(room=>[room,allCounts[room]]);
    this.safeSend(ws,["allRoomsUserCount",result]);
  }

  lockSeat(room,ws){
    const seatMap=this.roomSeats.get(room);
    if(!ws.idtarget) return null;

    if(this.userToSeat.has(ws.idtarget)){
      const prev=this.userToSeat.get(ws.idtarget);
      if(prev.room===room){
        return prev.seat;
      }
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

  sendAllStateTo(ws,room){
    const seatMap=this.roomSeats.get(room);
    const allPoints=[];
    const meta={};
    for(const [seat,info] of seatMap){
      for(const p of info.points) allPoints.push({seat,...p});
      if(info.namauser && !String(info.namauser).startsWith("__LOCK__")){
        const {points,...rest}=info;
        meta[seat]=rest;
      }
    }
    this.safeSend(ws,["allPointsList",room,allPoints]);
    this.safeSend(ws,["allUpdateKursiList",room,meta]);
  }

  cleanupClientById(idtarget){
    for(const c of Array.from(this.clients)){
      if(c.idtarget===idtarget){
        this.cleanupClient(c);
      }
    }
  }

  // 🔥 helper baru untuk hapus semua kursi ID lama sebelum join
  removeAllSeatsById(idtarget) {
    for (const [room, seatMap] of this.roomSeats) {
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          try { 
            this.broadcastToRoom(room, ["removeKursi", room, seat]); 
          } catch(e) { console.error(e); }
        }
      }
    }
  }

  // ==========================
  // 🔹 Optimized cleanupClient
  // ==========================
  cleanupClient(ws){
    try{
      const id=ws.idtarget;
      const kursis=ws.numkursi;
      const room=ws.roomname;

      if(id){
        for(const [r,seatMap] of this.roomSeats){
          for(const [seat,info] of seatMap){
            if(info.namauser==="__LOCK__"+id || info.namauser===id){
              Object.assign(seatMap.get(seat),createEmptySeat());
              try{
                this.broadcastToRoom(r,["removeKursi",r,seat]);
              } catch(e){ console.error("cleanupClient broadcast error:", e); }
            }
          }
        }
        this.userToSeat.delete(id);
      }

      if(room && kursis && this.roomSeats.has(room)){
        const seatMap=this.roomSeats.get(room);
        for(const seat of kursis){
          Object.assign(seatMap.get(seat),createEmptySeat());
          try{
            this.broadcastToRoom(room,["removeKursi",room,seat]);
          } catch(e){ console.error("cleanupClient broadcast error:", e); }
        }
        this.broadcastRoomUserCount(room);
      }

    } catch(e){ console.error("cleanupClient error:", e); }
    finally{
      this.clients.delete(ws);
      ws.numkursi?.clear?.();
      ws.roomname=undefined;
      ws.idtarget=undefined;
    }
  }

  handleMessage(ws,raw){
    // ... semua handleMessage tetap sama ...
  }

  async fetch(request){
    const upgrade=request.headers.get("Upgrade")||request.headers.get("upgrade")||"";
    if(upgrade.toLowerCase()!=="websocket") return new Response("Expected WebSocket",{status:426});

    const pair=new WebSocketPair();
    const [client,server]=Object.values(pair);
    server.accept();

    const ws=server;
    ws.roomname=undefined;
    ws.idtarget=undefined;
    ws.numkursi=new Set();
    this.clients.add(ws);

    ws.addEventListener("message",(ev)=>this.handleMessage(ws,ev.data));
    ws.addEventListener("close",()=>this.cleanupClient(ws));
    ws.addEventListener("error",()=>this.cleanupClient(ws));

    return new Response(null,{status:101,webSocket:client});
  }
}

// ======================
// Worker Entry (Router)
// ======================
export default {
  async fetch(req,env){
    if((req.headers.get("Upgrade")||"").toLowerCase()==="websocket"){
      const id=env.CHAT_SERVER.idFromName("global-chat");
      const obj=env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if(new URL(req.url).pathname==="/health") 
      return new Response("ok",{status:200,headers:{"content-type":"text/plain"}});
    return new Response("WebSocket endpoint at wss://<your-subdomain>.workers.dev",{status:200});
  }
};
