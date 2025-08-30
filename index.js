// ============================
// Cloudflare Workers + DO Chat (Real-Time)
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

    this.clients = new Set(); // semua WebSocket
    this.userToSeat = new Map(); // idtarget -> { room, seat }

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }
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
      if (c.roomname === room && c.readyState===1) {
        this.safeSend(c,msg);
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
    this.broadcastToRoom(room,["roomUserCount", room, count]);
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
    if(ws.readyState===1){
      this.safeSend(ws,["allPointsList",room,allPoints]);
      this.safeSend(ws,["allUpdateKursiList",room,meta]);
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
              this.broadcastToRoom(room,["removeKursi",room,seat]);
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
          this.broadcastToRoom(room,["removeKursi",room,seat]);
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
    let data;
    try{
      data=JSON.parse(raw);
    } catch(e){
      return this.safeSend(ws,["error","Invalid JSON"]);
    }
    if(!Array.isArray(data)||data.length===0) return this.safeSend(ws,["error","Invalid message format"]);

    const evt=data[0];

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

        const targetClients = Array.from(this.clients).filter(c=>c.idtarget===idtarget && c.readyState===1);
        if(targetClients.length){
          for(const c of targetClients) this.safeSend(c,notif);
        } else {
          // langsung gagal jika user offline
          this.safeSend(ws,["privateFailed",idtarget,"User offline"]);
        }
        break;
      }
      case "private": {
        const [,idt,url,msg,sender]=data;
        const ts=Date.now();
        const out=["private",idt,url,msg,ts,sender];
        const targetClients = Array.from(this.clients).filter(c=>c.idtarget===idt && c.readyState===1);
        if(targetClients.length){
          for(const c of targetClients) this.safeSend(c,out);
        } else {
          this.safeSend(ws,["privateFailed",idt,"User offline"]);
        }
        break;
      }
      case "joinRoom": {
        const newRoom=data[1];
        if(!roomList.includes(newRoom)) return this.safeSend(ws,["error",`Unknown room: ${newRoom}`]);

        if(ws.idtarget) this.removeAllSeatsById(ws.idtarget);

        ws.roomname=newRoom;
        const seatMap=this.roomSeats.get(newRoom);
        const foundSeat=this.lockSeat(newRoom,ws);
        if(foundSeat===null) return this.safeSend(ws,["roomFull",newRoom]);

        ws.numkursi=new Set([foundSeat]);
        this.safeSend(ws,["numberKursiSaya",foundSeat]);
        this.userToSeat.set(ws.idtarget,{room:newRoom,seat:foundSeat});
        this.sendAllStateTo(ws,newRoom);
        this.broadcastRoomUserCount(newRoom);
        break;
      }
      case "chat": {
        const [,roomname,noImageURL,username,message,usernameColor,chatTextColor]=data;
        if(!roomList.includes(roomname)) return this.safeSend(ws,["error","Invalid room for chat"]);
        this.broadcastToRoom(roomname,["chat",roomname,noImageURL,username,message,usernameColor,chatTextColor]);
        break;
      }
      case "updatePoint": {
        const [,room,seat,x,y,fast]=data;
        if(!roomList.includes(room)) return this.safeSend(ws,["error","Unknown room"]);
        const seatMap=this.roomSeats.get(room);
        const si=seatMap.get(seat);
        if(!si) return;
        si.points.push({x,y,fast});
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }
      case "updateKursi": {
        const [,room,seat,noimageUrl,namauser,color,itembawah,itematas,vip,viptanda]=data;
        if(!roomList.includes(room)) return this.safeSend(ws,["error","Unknown room"]);
        const seatInfo={noimageUrl,namauser,color,itembawah,itematas,vip,viptanda,points:[]};
        this.roomSeats.get(room).set(seat,seatInfo);
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, seatInfo]]]);
        this.broadcastRoomUserCount(room);
        break;
      }
      default:
        this.safeSend(ws,["error","Unknown event"]);
    }
  }

  cleanupClientById(idtarget){
    for(const c of Array.from(this.clients)){
      if(c.idtarget===idtarget){
        this.cleanupClient(c);
      }
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
    ws.addEventListener("close",(ev)=>{
      this.cleanupClient(ws);
    });
    ws.addEventListener("error",(ev)=>{
      this.cleanupClient(ws);
    });

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
