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

  flushChatBuffer(){
    for(const [room,messages] of this.chatMessageBuffer){
      for(const msg of messages){ 
        try{ this.broadcastToRoom(room,msg); } 
        catch(e){ console.error("flushChatBuffer:", e); } 
      }
      messages.length=0;
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
        const updates = [];
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
            if (!seatMapUpdates.has(seat)) continue;
            const info = seatMapUpdates.get(seat);
            const { points, ...rest } = info;
            updates.push([seat, rest]);
        }
        if (updates.length > 0) {
            try { this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]); }
            catch(e) { console.error("flushKursiUpdates:", e); }
        }
        seatMapUpdates.clear();
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
      this.flushKursiUpdates();
      this.flushChatBuffer();
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

  // ----------------------
  // Kirim semua state fixed sesuai seat ke WS Java
  // ----------------------
  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;
        for (const p of info.points) allPoints.push({ seat, ...p });
        if(info.namauser && !String(info.namauser).startsWith("__LOCK__")){
            meta[seat] = {
                noimageUrl: info.noimageUrl,
                namauser: info.namauser,
                color: info.color,
                itembawah: info.itembawah,
                itematas: info.itematas,
                vip: info.vip,
                viptanda: info.viptanda
            };
        }
    }
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, meta]);
  }

  cleanupClientById(idtarget){
    for(const c of Array.from(this.clients)){
      if(c.idtarget===idtarget){
        this.cleanupClient(c);
      }
    }
  }

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

  handleMessage(ws,raw){
    let data;
    try{
      data=JSON.parse(raw);
    } catch(e){
      console.error("Invalid JSON:", raw, e);
      return this.safeSend(ws,["error","Invalid JSON"]);
    }
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

        case "ping": {
          const pingId=data[1];
          if(pingId && ws.idtarget===pingId) this.safeSend(ws,["pong"]);
          break;
        }

        case "sendnotif": {
          const [,idtarget,noimageUrl,username,deskripsi]=data;
          const notif=["notif",noimageUrl,username,deskripsi,Date.now()];
          let delivered=false;
          for(const c of this.clients){ if(c.idtarget===idtarget){ this.safeSend(c,notif); delivered=true; } }
          if(!delivered){
            if(!this.privateMessageBuffer.has(idtarget)) this.privateMessageBuffer.set(idtarget,[]);
            this.privateMessageBuffer.get(idtarget).push(notif);
          }
          break;
        }

        case "private": {
          const [,idt,url,msg,sender]=data;
          const ts=Date.now();
          const out=["private",idt,url,msg,ts,sender];
          this.safeSend(ws,out);
          let delivered=false;
          for(const c of this.clients){ if(c.idtarget===idt){ this.safeSend(c,out); delivered=true; } }
          if(!delivered){
            if(!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt,[]);
            this.privateMessageBuffer.get(idt).push(out);
            this.safeSend(ws,["privateFailed",idt,"User offline"]);
          }
          break;
        }

        case "isUserOnline": {
          const target=data[1];
          const tanda=data[2]??"";
          const online=Array.from(this.clients).some(c=>c.idtarget===target);
          this.safeSend(ws,["userOnlineStatus",target,online,tanda]);
          break;
        }

        case "getAllRoomsUserCount": this.handleGetAllRoomsUserCount(ws); break;
        case "getCurrentNumber": this.safeSend(ws,["currentNumber",this.currentNumber]); break;

        // ----------------------
        // Join Room
        // ----------------------
        case "joinRoom": {
          const newRoom = data[1];
          if(!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

          if(ws.idtarget) this.removeAllSeatsById(ws.idtarget);

          ws.roomname = newRoom;
          const seatMap = this.roomSeats.get(newRoom);
          const foundSeat = this.lockSeat(newRoom, ws);
          if(foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);

          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          if(ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

          // Kirim semua kursi & points real-time
          this.sendAllStateTo(ws, newRoom);

          this.broadcastRoomUserCount(newRoom);
          break;
        }

        // ----------------------
        // Chat
        // ----------------------
        case "chat": {
          const [,roomname,noImageURL,username,message,usernameColor,chatTextColor]=data;
          if(!roomList.includes(roomname)) return this.safeSend(ws,["error","Invalid room for chat"]);
          if(!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname,[]);
          this.chatMessageBuffer.get(roomname).push(["chat",roomname,noImageURL,username,message,usernameColor,chatTextColor]);
          break;
        }

        // ----------------------
        // Update Point (real-time)
        // ----------------------
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if(!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);

          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if(!si) return;

          si.points.push({ x, y, fast });
          if(si.points.length > 200) si.points.shift();

          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [,room,seat]=data;
          if(!roomList.includes(room)) return this.safeSend(ws,["error",`Unknown room: ${room}`]);
          const seatMap=this.roomSeats.get(room);
          Object.assign(seatMap.get(seat),createEmptySeat());
          for(const c of this.clients) c.numkursi?.delete(seat);
          this.broadcastToRoom(room,["removeKursi",room,seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [,room,seat,noimageUrl,namauser,color,itembawah,itematas,vip,viptanda]=data;
          if(!roomList.includes(room)) return this.safeSend(ws,["error",`Unknown room: ${room}`]);
          const seatInfo={noimageUrl,namauser,color,itembawah,itematas,vip,viptanda,points:[]};
          if(!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room,new Map());
          this.updateKursiBuffer.get(room).set(seat,seatInfo);
          this.roomSeats.get(room).set(seat,seatInfo);
          this.broadcastRoomUserCount(room);
          break;
        }

        default: this.safeSend(ws,["error","Unknown event"]);
      }
    } catch(err){ 
      console.error("handleMessage error:", ws.idtarget, err); 
      this.safeSend(ws,["error","Internal error"]); 
    }
  }

  cleanupClient(ws){
    try{
      const id=ws.idtarget;
      if(id){
        for(const [room,seatMap] of this.roomSeats){
          for(const [seat,info] of seatMap){
            if(info.namauser==="__LOCK__"+id || info.namauser===id){
              Object.assign(seatMap.get(seat),createEmptySeat());
              try{ this.broadcastToRoom(room,["removeKursi",room,seat]); } catch(e){ console.error("cleanupClient broadcast error:", e); }
            }
          }
        }
        this.userToSeat.delete(id);
      }
      const room=ws.roomname;
      const kursis=ws.numkursi;
      if(room && kursis && this.roomSeats.has(room)){
        const seatMap=this.roomSeats.get(room);
        for(const seat of kursis){
          Object.assign(seatMap.get(seat),createEmptySeat());
          try{ this.broadcastToRoom(room,["removeKursi",room,seat]); } 
          catch(e){ console.error("cleanupClient broadcast error:", e); }
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
