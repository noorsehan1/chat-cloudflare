// ==================== CHAT-WORKER.JS ====================
// VERSION: 4.0.0 - D1 FULL INTEGRATION

const CONFIG = {
    MAX_SEATS: 45,
    MAX_GLOBAL_CONNECTIONS: 150,
    MAX_MESSAGE_SIZE: 5000,
    NUMBER_UPDATE_TIK: 6,
    MAX_NUMBER: 6,
    BATCH_SIZE: 20,
    LOCK_TIMEOUT: 10000,
    MAX_EVENT_QUEUE: 100,
    MAX_PROCESS_TIME_MS: 500,
};

const ROOMS = [
    "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
    "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
    "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

export class ChatWorker {
    constructor(env) {
        this.env = env;
        this.closing = false;
        this.isDestroyed = false;
        this._startTime = Date.now();
        
        // WebSocket connections
        this.wsSet = new Set();
        this.userConnections = new Map();
        this.userSeat = new Map();
        this.userRoom = new Map();
        this.roomClients = new Map();
        this.wsActiveMulti = new Map();
        this.wsIdMap = new Map();
        
        // Processing
        this._processingMessages = new Set();
        this._cleaningUp = new Set();
        this._eventQueue = [];
        this._isProcessingQueue = false;
        
        // Locks
        this._joinLocks = new Map();
        this._kursiLocks = new Map();
        
        // Counter
        this.currentNumber = 1;
        this._tikCounter = 0;
        this._wsIdCounter = 0;
        
        // Intervals
        this._cleanupInterval = null;
        this._mainInterval = null;
        
        // Start intervals
        this._startIntervals();
        this._loadInitialData();
    }

    // ========== LOAD INITIAL DATA ==========
    async _loadInitialData() {
        try {
            // Load current number
            const result = await this.env.DB.prepare(
                `SELECT number FROM rooms WHERE room_name = 'General'`
            ).first();
            if (result) {
                this.currentNumber = result.number || 1;
            }
            
            // Init room clients
            for (const room of ROOMS) {
                if (!this.roomClients.has(room)) {
                    this.roomClients.set(room, new Set());
                }
            }
        } catch(e) {
            console.error("Load initial data error:", e);
        }
    }

    // ========== START INTERVALS ==========
    _startIntervals() {
        if (this.closing || this.isDestroyed) return;

        this._mainInterval = setInterval(() => {
            if (this.closing || this.isDestroyed) {
                clearInterval(this._mainInterval);
                this._mainInterval = null;
                return;
            }
            this._doTick();
        }, 10000);

        this._cleanupInterval = setInterval(() => {
            if (this.closing || this.isDestroyed) {
                clearInterval(this._cleanupInterval);
                this._cleanupInterval = null;
                return;
            }
            this._performCleanup();
        }, 600000);
    }

    // ========== TICK ==========
    _doTick() {
        try {
            this._cleanupDeadConnections();
            this._processEventQueue();
        } catch(e) {}
    }

    // ========== PERFORM CLEANUP ==========
    _performCleanup() {
        try {
            this._cleanupDeadConnections();
            this._cleanupStaleLocks();
            this._cleanupEventQueue();
        } catch(e) {}
    }

    _cleanupDeadConnections() {
        try {
            const toRemove = [];
            for (const ws of this.wsSet) {
                if (!ws || ws.readyState !== 1 || ws._closing) {
                    toRemove.push(ws);
                }
            }
            for (const ws of toRemove) {
                this.cleanup(ws);
            }
        } catch(e) {}
    }

    _cleanupStaleLocks() {
        try {
            const now = Date.now();
            for (const [key, time] of this._joinLocks) {
                if (now - time > CONFIG.LOCK_TIMEOUT) {
                    this._joinLocks.delete(key);
                }
            }
            for (const [key, time] of this._kursiLocks) {
                if (now - time > CONFIG.LOCK_TIMEOUT) {
                    this._kursiLocks.delete(key);
                }
            }
        } catch(e) {}
    }

    _cleanupEventQueue() {
        try {
            if (this._eventQueue.length > CONFIG.MAX_EVENT_QUEUE) {
                this._eventQueue.splice(0, this._eventQueue.length - CONFIG.MAX_EVENT_QUEUE);
            }
        } catch(e) {}
    }

    // ========== PROCESS EVENT QUEUE ==========
    _processEventQueue() {
        try {
            if (this._isProcessingQueue || this._eventQueue.length === 0) return;
            this._isProcessingQueue = true;

            const startTime = Date.now();
            let processed = 0;

            while (this._eventQueue.length > 0 && processed < 5) {
                if (Date.now() - startTime > CONFIG.MAX_PROCESS_TIME_MS) break;

                const item = this._eventQueue.shift();
                try {
                    this._handleEventInternal(item.ws, item.data);
                } catch(e) {}
                processed++;
            }

            this._isProcessingQueue = false;
        } catch(e) {
            this._isProcessingQueue = false;
        }
    }

    // ========== DATABASE HELPERS ==========
    async _getRoomData(roomName) {
        try {
            return await this.env.DB.prepare(
                `SELECT * FROM rooms WHERE room_name = ?`
            ).bind(roomName).first();
        } catch(e) {
            return null;
        }
    }

    async _getSeats(roomName) {
        try {
            const result = await this.env.DB.prepare(
                `SELECT * FROM seats WHERE room_name = ? ORDER BY seat_number`
            ).bind(roomName).all();
            return result.results || [];
        } catch(e) {
            return [];
        }
    }

    async _getPoints(roomName) {
        try {
            const result = await this.env.DB.prepare(
                `SELECT * FROM points WHERE room_name = ? ORDER BY seat_number`
            ).bind(roomName).all();
            return result.results || [];
        } catch(e) {
            return [];
        }
    }

    async _updateSeat(roomName, seatNumber, data) {
        try {
            await this.env.DB.prepare(`
                INSERT OR REPLACE INTO seats 
                (room_name, seat_number, namauser, noimage_url, color, itembawah, itematas, vip, viptanda, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).bind(
                roomName, seatNumber,
                data.namauser || '',
                data.noimageUrl || '',
                data.color || '',
                data.itembawah || 0,
                data.itematas || 0,
                data.vip || 0,
                data.viptanda || 0,
                Math.floor(Date.now() / 1000)
            ).run();
            return true;
        } catch(e) {
            return false;
        }
    }

    async _updatePoint(roomName, seatNumber, x, y, fast) {
        try {
            await this.env.DB.prepare(`
                INSERT OR REPLACE INTO points 
                (room_name, seat_number, x, y, fast, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            `).bind(
                roomName, seatNumber,
                x || 0, y || 0,
                fast ? 1 : 0,
                Math.floor(Date.now() / 1000)
            ).run();
            return true;
        } catch(e) {
            return false;
        }
    }

    async _removeSeat(roomName, seatNumber) {
        try {
            await this.env.DB.prepare(
                `DELETE FROM seats WHERE room_name = ? AND seat_number = ?`
            ).bind(roomName, seatNumber).run();
            
            await this.env.DB.prepare(
                `DELETE FROM points WHERE room_name = ? AND seat_number = ?`
            ).bind(roomName, seatNumber).run();
            
            await this.env.DB.prepare(
                `DELETE FROM user_connections WHERE room_name = ? AND seat_number = ?`
            ).bind(roomName, seatNumber).run();
            
            return true;
        } catch(e) {
            return false;
        }
    }

    async _updateUserConnection(username, roomName, seatNumber, isMulti = 0) {
        try {
            await this.env.DB.prepare(`
                INSERT OR REPLACE INTO user_connections 
                (username, room_name, seat_number, is_multi, updated_at)
                VALUES (?, ?, ?, ?, ?)
            `).bind(
                username,
                roomName,
                seatNumber,
                isMulti ? 1 : 0,
                Math.floor(Date.now() / 1000)
            ).run();
            return true;
        } catch(e) {
            return false;
        }
    }

    async _removeUserConnection(username) {
        try {
            await this.env.DB.prepare(
                `DELETE FROM user_connections WHERE username = ?`
            ).bind(username).run();
            return true;
        } catch(e) {
            return false;
        }
    }

    async _updateNumber(roomName, number) {
        try {
            await this.env.DB.prepare(
                `UPDATE rooms SET number = ? WHERE room_name = ?`
            ).bind(number, roomName).run();
            return true;
        } catch(e) {
            return false;
        }
    }

    async _updateMuted(roomName, muted) {
        try {
            await this.env.DB.prepare(
                `UPDATE rooms SET muted = ? WHERE room_name = ?`
            ).bind(muted ? 1 : 0, roomName).run();
            return true;
        } catch(e) {
            return false;
        }
    }

    async _getRoomCount(room) {
        try {
            const result = await this.env.DB.prepare(
                `SELECT COUNT(*) as count FROM seats WHERE room_name = ?`
            ).bind(room).first();
            return result?.count || 0;
        } catch(e) {
            return 0;
        }
    }

    // ========== BROADCAST ==========
    _broadcastToRoom(room, msgStr) {
        if (this.closing || this.isDestroyed || !room) return;

        const clients = this.roomClients.get(room);
        if (!clients || clients.size === 0) return;

        const clientArray = Array.from(clients);
        const toRemove = new Set();

        for (let i = 0; i < clientArray.length; i += CONFIG.BATCH_SIZE) {
            const batch = clientArray.slice(i, Math.min(i + CONFIG.BATCH_SIZE, clientArray.length));

            for (const ws of batch) {
                if (!ws) {
                    toRemove.add(ws);
                    continue;
                }

                try {
                    if (ws.readyState === 1 && !ws._closing) {
                        ws.send(msgStr);
                    } else {
                        toRemove.add(ws);
                    }
                } catch(e) {
                    toRemove.add(ws);
                }
            }
        }

        if (toRemove.size > 0) {
            for (const ws of toRemove) {
                try {
                    clients.delete(ws);
                    this.cleanup(ws);
                } catch(e) {}
            }
        }
    }

    broadcast(room, msg) {
        if (this.closing || this.isDestroyed || !room || !msg) return;
        try {
            this._broadcastToRoom(room, JSON.stringify(msg));
        } catch(e) {}
    }

    safeSend(ws, msg) {
        if (!ws) return false;

        try {
            if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
                return false;
            }
            ws.send(JSON.stringify(msg));
            return true;
        } catch(e) {
            this.cleanup(ws);
            return false;
        }
    }

    async updateRoomCount(room) {
        if (this.closing || this.isDestroyed || !room) return 0;
        try {
            const count = await this._getRoomCount(room);
            this.broadcast(room, ["roomUserCount", room, count]);
            return count;
        } catch(e) {
            return 0;
        }
    }

    // ========== SEND ALL STATE ==========
    async sendAllStateTo(ws, room, excludeSelf = false) {
        if (!ws || !ws.username) return;

        try {
            if (ws.readyState !== 1 || ws._closing) return;
        } catch(e) {
            return;
        }

        try {
            const seats = await this._getSeats(room);
            const points = await this._getPoints(room);
            const count = await this._getRoomCount(room);
            const selfSeat = this.userSeat.get(ws.username)?.seat;

            this.safeSend(ws, ["roomUserCount", room, count]);

            if (seats && seats.length > 0) {
                const seatMap = {};
                for (const seat of seats) {
                    seatMap[seat.seat_number] = {
                        noimageUrl: seat.noimage_url || "",
                        namauser: seat.namauser || "",
                        color: seat.color || "",
                        itembawah: seat.itembawah || 0,
                        itematas: seat.itematas || 0,
                        vip: seat.vip || 0,
                        viptanda: seat.viptanda || 0,
                    };
                }

                if (excludeSelf && selfSeat && seatMap[selfSeat]) {
                    const filtered = { ...seatMap };
                    delete filtered[selfSeat];
                    if (Object.keys(filtered).length > 0) {
                        this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
                    }
                } else {
                    this.safeSend(ws, ["allUpdateKursiList", room, seatMap]);
                }
            }

            if (points && points.length > 0) {
                let filteredPoints = points.map(p => ({
                    seat: p.seat_number,
                    x: p.x || 0,
                    y: p.y || 0,
                    fast: p.fast || 0
                }));

                if (excludeSelf && selfSeat) {
                    filteredPoints = filteredPoints.filter(p => p.seat !== selfSeat);
                }

                if (filteredPoints.length > 0) {
                    this.safeSend(ws, ["allPointsList", room, filteredPoints]);
                }
            }
        } catch(e) {}
    }

    // ========== CLEANUP ==========
    cleanup(ws) {
        if (!ws || ws._cleaning || this._cleaningUp.has(ws)) {
            return;
        }

        ws._cleaning = true;
        this._cleaningUp.add(ws);

        try {
            const username = ws.username;
            const room = ws.room;

            if (room) {
                try {
                    const clients = this.roomClients.get(room);
                    if (clients) clients.delete(ws);
                } catch(e) {}
            }

            try {
                const activeData = this.wsActiveMulti.get(ws);
                if (activeData?.room) {
                    const clients = this.roomClients.get(activeData.room);
                    if (clients) clients.delete(ws);
                }
                this.wsActiveMulti.delete(ws);
            } catch(e) {}

            if (username) {
                try {
                    const connections = this.userConnections.get(username);
                    if (connections) {
                        connections.delete(ws);

                        const seatInfo = this.userSeat.get(username);
                        const isMulti = seatInfo?.isMulti === true;

                        if (!isMulti && connections.size === 0) {
                            this.userConnections.delete(username);

                            if (seatInfo?.room) {
                                this._removeSeat(seatInfo.room, seatInfo.seat);
                                this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
                                this.updateRoomCount(seatInfo.room);
                            }

                            this.userSeat.delete(username);
                            this.userRoom.delete(username);
                            this._removeUserConnection(username);
                        }
                    }
                } catch(e) {}
            }

            try {
                this.wsSet.delete(ws);
            } catch(e) {}

        } catch(e) {} finally {
            ws._cleaning = false;
            this._cleaningUp.delete(ws);

            try {
                if (ws && ws.readyState === 1) {
                    ws.close(1000, "Cleanup");
                }
            } catch(e) {}
        }
    }

    // ========== HANDLE MESSAGE ==========
    async handleMessage(ws, raw) {
        if (!ws) return;

        try {
            if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
                return;
            }
        } catch(e) {
            return;
        }

        if (this._processingMessages.has(ws)) return;
        this._processingMessages.add(ws);

        try {
            let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
            if (str.length > CONFIG.MAX_MESSAGE_SIZE) return;

            let data;
            try {
                data = JSON.parse(str);
            } catch(e) {
                return;
            }
            if (!Array.isArray(data) || !data.length) return;

            const [evt, ...args] = data;

            if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
                const room = args[0];
                if (room && !ROOMS_SET.has(room)) return;
            }

            if (this._eventQueue.length < CONFIG.MAX_EVENT_QUEUE) {
                this._eventQueue.push({ ws, data: [evt, ...args] });
                if (!this._isProcessingQueue) {
                    this._processEventQueue();
                }
            }

        } catch(e) {} finally {
            try {
                this._processingMessages.delete(ws);
            } catch(e) {}
        }
    }

    // ========== HANDLE EVENT INTERNAL ==========
    _handleEventInternal(ws, data) {
        try {
            if (!ws || !data || !data[0]) return;
            const [evt, ...args] = data;

            switch (evt) {
                case "setIdTarget2":
                    this._handleSetId(ws, args[0], args[1]);
                    break;
                case "joinRoom":
                    this._handleJoin(ws, args[0]);
                    break;
                case "multiJoin":
                    this._handleMultiJoin(ws, args[0], args[1]);
                    break;
                case "exitMulti":
                    this._handleExitMulti(ws, args[0]);
                    break;
                case "setActiveMulti":
                    this._handleSetActiveMulti(ws, args[0]);
                    break;
                case "updateKursi":
                    this._handleUpdateKursi(ws, args);
                    break;
                case "chat":
                    this._handleChat(ws, args);
                    break;
                case "updatePoint":
                    this._handleUpdatePoint(ws, args);
                    break;
                case "removeKursiAndPoint":
                    this._handleRemoveKursi(ws, args);
                    break;
                case "private":
                    this._handlePrivate(ws, args);
                    break;
                case "gift":
                    this._handleGift(ws, args);
                    break;
                case "rollangak":
                    this._handleRoll(ws, args);
                    break;
                case "sendnotif":
                    this._handleNotif(ws, args);
                    break;
                case "getCurrentNumber":
                    this.safeSend(ws, ["currentNumber", this.currentNumber]);
                    break;
                case "isUserOnline":
                    this._handleIsUserOnline(ws, args);
                    break;
                case "getOnlineUsers":
                    this._handleGetOnlineUsers(ws);
                    break;
                case "getAllRoomsUserCount":
                    this._handleGetAllRoomsUserCount(ws);
                    break;
                case "getRoomUserCount":
                    this._handleGetRoomUserCount(ws, args);
                    break;
                case "setMuteType":
                    this._handleSetMuteType(ws, args);
                    break;
                case "modwarning":
                    this._handleModWarning(ws, args);
                    break;
                case "getMuteType":
                    this._handleGetMuteType(ws, args);
                    break;
                case "onDestroy":
                    this.cleanup(ws);
                    break;
                default:
                    this.safeSend(ws, ["error", `Unknown event: ${evt}`]);
                    break;
            }
        } catch(e) {}
    }

    // ========== HANDLE SET ID ==========
    _handleSetId(ws, username, isNewUser) {
        if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
            try {
                if (ws?.readyState === 1) ws.close(1000, "Invalid username");
            } catch(e) {}
            return;
        }

        if (ws.readyState !== 1) {
            try {
                this.cleanup(ws);
            } catch(e) {}
            return;
        }

        const existingSeatInfo = this.userSeat.get(username);
        if (existingSeatInfo?.isMulti === true && isNewUser === false) {
            try {
                const oldConnections = this.userConnections.get(username);
                if (oldConnections) {
                    const toRemove = [];
                    for (const conn of oldConnections) {
                        if (!conn || conn.readyState !== 1 || conn._closing) {
                            toRemove.push(conn);
                        }
                    }
                    for (const conn of toRemove) {
                        oldConnections.delete(conn);
                        this.wsSet.delete(conn);
                        this.wsActiveMulti.delete(conn);
                    }
                }

                let connections = this.userConnections.get(username);
                if (!connections) {
                    connections = new Set();
                    this.userConnections.set(username, connections);
                }
                if (!connections.has(ws)) {
                    connections.add(ws);
                }

                if (!this.wsSet.has(ws)) {
                    this.wsSet.add(ws);
                }

                ws.username = username;
                ws.idtarget = username;
                ws.room = null;
                ws.roomname = null;
                ws._closing = false;

                this.safeSend(ws, ["multiUserActive", username]);
            } catch(e) {}
            return;
        }

        try {
            const oldConnections = this.userConnections.get(username);
            if (oldConnections) {
                const toRemove = [];
                for (const conn of oldConnections) {
                    if (!conn || conn.readyState !== 1 || conn._closing) {
                        toRemove.push(conn);
                    }
                }
                for (const conn of toRemove) {
                    oldConnections.delete(conn);
                    this.wsSet.delete(conn);
                    this.wsActiveMulti.delete(conn);
                }
                if (oldConnections.size === 0) {
                    this.userConnections.delete(username);
                }
            }

            ws.username = username;
            ws.idtarget = username;
            ws.room = null;
            ws.roomname = null;
            ws._closing = false;

            let connections = this.userConnections.get(username);
            if (!connections) {
                connections = new Set();
                this.userConnections.set(username, connections);
            }
            if (!connections.has(ws)) {
                connections.add(ws);
            }

            if (!this.wsSet.has(ws)) {
                this.wsSet.add(ws);
            }

            if (isNewUser) {
                this.safeSend(ws, ["joinroomawal"]);
            } else {
                this.safeSend(ws, ["needJoinRoom"]);
            }
        } catch(e) {}
    }

    // ========== HANDLE JOIN ==========
    async _handleJoin(ws, roomName) {
        if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
            return false;
        }

        const username = ws.username;
        const lockKey = `join_${roomName}_${username}`;

        if (this._joinLocks.has(lockKey)) {
            this.safeSend(ws, ["roomFull", roomName]);
            return false;
        }

        this._joinLocks.set(lockKey, Date.now());

        try {
            return await this._handleJoinInternal(ws, roomName, username);
        } finally {
            this._joinLocks.delete(lockKey);
        }
    }

    async _handleJoinInternal(ws, roomName, username) {
        const oldRoom = ws.room;

        if (oldRoom && oldRoom !== roomName) {
            try {
                const oldSeat = this.userSeat.get(username)?.seat;
                if (oldSeat) {
                    await this._removeSeat(oldRoom, oldSeat);
                    this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
                    this.updateRoomCount(oldRoom);
                }
                const oldClients = this.roomClients.get(oldRoom);
                if (oldClients) oldClients.delete(ws);
                this.userSeat.delete(username);
                this.userRoom.delete(username);
            } catch(e) {}
            ws.room = null;
            ws.roomname = null;
        }

        let seat = null;
        const seats = await this._getSeats(roomName);
        for (const s of seats) {
            if (s.namauser === username) {
                seat = s.seat_number;
                break;
            }
        }

        if (!seat) {
            const count = await this._getRoomCount(roomName);
            if (count >= CONFIG.MAX_SEATS) {
                this.safeSend(ws, ["roomFull", roomName]);
                return false;
            }

            const usedSeats = new Set(seats.map(s => s.seat_number));
            for (let i = 1; i <= CONFIG.MAX_SEATS; i++) {
                if (!usedSeats.has(i)) {
                    seat = i;
                    break;
                }
            }

            if (!seat) {
                this.safeSend(ws, ["roomFull", roomName]);
                return false;
            }

            await this._updateSeat(roomName, seat, {
                namauser: username,
                noimageUrl: "",
                color: "",
                itembawah: 0,
                itematas: 0,
                vip: 0,
                viptanda: 0
            });
        }

        try {
            this.userSeat.set(username, { room: roomName, seat, isMulti: false });
            this.userRoom.set(username, roomName);
            await this._updateUserConnection(username, roomName, seat, 0);

            ws.room = roomName;
            ws.roomname = roomName;
            ws.idtarget = username;

            const roomClients = this.roomClients.get(roomName);
            if (roomClients && !roomClients.has(ws)) roomClients.add(ws);

            const roomData = await this._getRoomData(roomName);

            this.safeSend(ws, ["rooMasuk", seat, roomName]);
            this.safeSend(ws, ["numberKursiSaya", seat]);
            this.safeSend(ws, ["muteTypeResponse", roomData?.muted || 0, roomName]);
            this.safeSend(ws, ["roomUserCount", roomName, await this._getRoomCount(roomName)]);

            this.updateRoomCount(roomName);

            setTimeout(async () => {
                try {
                    if (ws && ws.readyState === 1 && !this.closing && !this.isDestroyed) {
                        await this.sendAllStateTo(ws, roomName, true);
                    }
                } catch(e) {}
            }, 1000);

        } catch(e) {}

        return true;
    }

    // ========== HANDLE MULTI JOIN ==========
    async _handleMultiJoin(ws, multiUsername, multiRoomname) {
        if (!multiUsername || !multiRoomname || this.closing || this.isDestroyed) return;

        try {
            const existingSeat = this.userSeat.get(multiUsername);
            if (existingSeat) {
                await this._removeSeat(existingSeat.room, existingSeat.seat);
                this.broadcast(existingSeat.room, ["removeKursi", existingSeat.room, existingSeat.seat]);
                this.updateRoomCount(existingSeat.room);
                this.userSeat.delete(multiUsername);
                this.userRoom.delete(multiUsername);
            }

            const count = await this._getRoomCount(multiRoomname);
            if (count >= CONFIG.MAX_SEATS) return;

            const seats = await this._getSeats(multiRoomname);
            const usedSeats = new Set(seats.map(s => s.seat_number));
            let seat = null;
            for (let i = 1; i <= CONFIG.MAX_SEATS; i++) {
                if (!usedSeats.has(i)) {
                    seat = i;
                    break;
                }
            }

            if (!seat) return;

            await this._updateSeat(multiRoomname, seat, {
                namauser: multiUsername,
                noimageUrl: "",
                color: "",
                itembawah: 0,
                itematas: 0,
                vip: 0,
                viptanda: 0
            });

            this.userSeat.set(multiUsername, { room: multiRoomname, seat, isMulti: true });
            this.userRoom.set(multiUsername, multiRoomname);
            await this._updateUserConnection(multiUsername, multiRoomname, seat, 1);

            let connections = this.userConnections.get(multiUsername);
            if (!connections) connections = new Set();
            if (!connections.has(ws)) connections.add(ws);
            this.userConnections.set(multiUsername, connections);

            this.wsActiveMulti.set(ws, { username: multiUsername, room: multiRoomname });
            const roomClients = this.roomClients.get(multiRoomname);
            if (roomClients && !roomClients.has(ws)) roomClients.add(ws);

            this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
            this.updateRoomCount(multiRoomname);
        } catch(e) {}
    }

    // ========== HANDLE EXIT MULTI ==========
    async _handleExitMulti(ws, targetUsername) {
        if (!targetUsername) return;

        try {
            const seatInfo = this.userSeat.get(targetUsername);
            if (!seatInfo) return;

            const roomName = seatInfo.room;
            const seatNumber = seatInfo.seat;

            const activeData = this.wsActiveMulti.get(ws);
            if (activeData?.username === targetUsername) {
                const roomClients = this.roomClients.get(roomName);
                if (roomClients) roomClients.delete(ws);
                this.wsActiveMulti.delete(ws);
            }

            await this._removeSeat(roomName, seatNumber);
            this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
            this.updateRoomCount(roomName);

            this.userSeat.delete(targetUsername);
            this.userRoom.delete(targetUsername);
            await this._removeUserConnection(targetUsername);

            const connections = this.userConnections.get(targetUsername);
            if (connections) {
                connections.delete(ws);
                if (connections.size === 0) {
                    this.userConnections.delete(targetUsername);
                }
            }

            if (ws.username === targetUsername) {
                ws.username = null;
                ws.idtarget = null;
            }
        } catch(e) {}
    }

    // ========== HANDLE SET ACTIVE MULTI ==========
    async _handleSetActiveMulti(ws, targetUsername) {
        try {
            const seatInfo = this.userSeat.get(targetUsername);
            if (!seatInfo) return;

            const roomName = seatInfo.room;
            const seatNumber = seatInfo.seat;

            const oldActive = this.wsActiveMulti.get(ws);
            if (oldActive?.room) {
                const oldClients = this.roomClients.get(oldActive.room);
                if (oldClients) oldClients.delete(ws);
            }

            this.wsActiveMulti.set(ws, { username: targetUsername, room: roomName });
            const roomClients = this.roomClients.get(roomName);
            if (roomClients && !roomClients.has(ws)) roomClients.add(ws);

            ws.username = targetUsername;
            ws.idtarget = targetUsername;
            ws.room = roomName;
            ws.roomname = roomName;

            this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
            this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
        } catch(e) {}
    }

    // ========== HANDLE UPDATE KURSI ==========
    async _handleUpdateKursi(ws, args) {
        try {
            const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;

            const lockKey = `kursi_${kursiRoom}_${kursiSeat}`;
            if (this._kursiLocks.has(lockKey)) return;

            this._kursiLocks.set(lockKey, Date.now());

            try {
                await this._updateSeat(kursiRoom, kursiSeat, {
                    noimageUrl: kursiNoimg || "",
                    namauser: kursiName || "",
                    color: kursiColor || "",
                    itembawah: kursiBawah || 0,
                    itematas: kursiAtas || 0,
                    vip: kursiVip || 0,
                    viptanda: kursiVt || 0
                });

                const updatedSeat = {
                    noimageUrl: kursiNoimg || "",
                    namauser: kursiName || "",
                    color: kursiColor || "",
                    itembawah: kursiBawah || 0,
                    itematas: kursiAtas || 0,
                    vip: kursiVip || 0,
                    viptanda: kursiVt || 0
                };

                this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
            } finally {
                this._kursiLocks.delete(lockKey);
            }
        } catch(e) {}
    }

    // ========== HANDLE CHAT ==========
    _handleChat(ws, args) {
        try {
            const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;

            if (!chatMsg || !ROOMS_SET.has(chatRoom)) return;

            const clients = this.roomClients.get(chatRoom);
            if (!clients || clients.size === 0) return;

            this._broadcastToRoom(chatRoom, JSON.stringify([
                "chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor
            ]));
        } catch(e) {}
    }

    // ========== HANDLE UPDATE POINT ==========
    async _handleUpdatePoint(ws, args) {
        try {
            const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
            if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= CONFIG.MAX_SEATS) {
                await this._updatePoint(pointRoom, pointSeat, pointX, pointY, pointFast === 1);
                this._broadcastToRoom(pointRoom, JSON.stringify([
                    "pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast
                ]));
            }
        } catch(e) {}
    }

    // ========== HANDLE REMOVE KURSI ==========
    async _handleRemoveKursi(ws, args) {
        try {
            const [removeRoom, removeSeat] = args;
            for (const [username, info] of this.userSeat) {
                if (info.seat === removeSeat && info.room === removeRoom) {
                    this.userSeat.delete(username);
                    this.userRoom.delete(username);
                    await this._removeUserConnection(username);
                    break;
                }
            }
            await this._removeSeat(removeRoom, removeSeat);
            this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
            this.updateRoomCount(removeRoom);
        } catch(e) {}
    }

    // ========== HANDLE PRIVATE ==========
    _handlePrivate(ws, args) {
        try {
            const [privTarget, privNoimg, privMsg, privSender] = args;
            if (privTarget && privMsg) {
                const targetConns = this.userConnections.get(privTarget);
                if (targetConns) {
                    for (const targetWs of targetConns) {
                        if (targetWs?.readyState === 1) {
                            this.safeSend(targetWs, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                            break;
                        }
                    }
                }
                this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
            }
        } catch(e) {}
    }

    // ========== HANDLE GIFT ==========
    _handleGift(ws, args) {
        try {
            const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
            if (giftRoom && ROOMS_SET.has(giftRoom)) {
                const clients = this.roomClients.get(giftRoom);
                if (!clients || clients.size === 0) return;
                this._broadcastToRoom(giftRoom, JSON.stringify([
                    "gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()
                ]));
            }
        } catch(e) {}
    }

    // ========== HANDLE ROLL ==========
    _handleRoll(ws, args) {
        try {
            const [rollRoom, rollUser, rollAngka] = args;
            if (rollRoom && ROOMS_SET.has(rollRoom)) {
                const clients = this.roomClients.get(rollRoom);
                if (!clients || clients.size === 0) return;
                this._broadcastToRoom(rollRoom, JSON.stringify([
                    "rollangakBroadcast", rollRoom, rollUser, rollAngka
                ]));
            }
        } catch(e) {}
    }

    // ========== HANDLE NOTIF ==========
    _handleNotif(ws, args) {
        try {
            const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
            if (notifTarget && notifMsg) {
                const targetConns = this.userConnections.get(notifTarget);
                if (targetConns) {
                    for (const c of targetConns) {
                        if (c?.readyState === 1) {
                            this.safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                            break;
                        }
                    }
                }
            }
        } catch(e) {}
    }

    // ========== HANDLE IS USER ONLINE ==========
    _handleIsUserOnline(ws, args) {
        try {
            const [onlineTarget, onlineCallback] = args;
            let isOnline = false;
            const seatInfo = this.userSeat.get(onlineTarget);
            if (seatInfo?.seat) {
                if (seatInfo.isMulti) {
                    isOnline = true;
                } else {
                    const connections = this.userConnections.get(onlineTarget);
                    if (connections) {
                        for (const conn of connections) {
                            if (conn?.readyState === 1) {
                                isOnline = true;
                                break;
                            }
                        }
                    }
                }
            }
            this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
        } catch(e) {}
    }

    // ========== HANDLE GET ONLINE USERS ==========
    _handleGetOnlineUsers(ws) {
        try {
            const users = [];
            for (const [username, seatInfo] of this.userSeat) {
                if (seatInfo?.seat) {
                    if (seatInfo.isMulti) {
                        users.push(username);
                    } else {
                        const connections = this.userConnections.get(username);
                        if (connections) {
                            for (const conn of connections) {
                                if (conn?.readyState === 1) {
                                    users.push(username);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            this.safeSend(ws, ["allOnlineUsers", users]);
        } catch(e) {}
    }

    // ========== HANDLE GET ALL ROOMS USER COUNT ==========
    async _handleGetAllRoomsUserCount(ws) {
        try {
            const counts = {};
            for (const room of ROOMS) {
                counts[room] = await this._getRoomCount(room);
            }
            this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
        } catch(e) {}
    }

    // ========== HANDLE GET ROOM USER COUNT ==========
    async _handleGetRoomUserCount(ws, args) {
        try {
            const roomName = args[0];
            if (roomName && ROOMS_SET.has(roomName)) {
                const count = await this._getRoomCount(roomName);
                this.safeSend(ws, ["roomUserCount", roomName, count]);
            }
        } catch(e) {}
    }

    // ========== HANDLE SET MUTE TYPE ==========
    async _handleSetMuteType(ws, args) {
        try {
            const [muteVal, muteRoom] = args;
            if (!muteRoom || !ROOMS_SET.has(muteRoom)) return;

            await this._updateMuted(muteRoom, muteVal);
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
            this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
        } catch(e) {}
    }

    // ========== HANDLE MOD WARNING ==========
    _handleModWarning(ws, args) {
        try {
            const modRoom = args[0];
            if (modRoom && ROOMS_SET.has(modRoom)) {
                this.broadcast(modRoom, ["modwarning", modRoom]);
            }
        } catch(e) {}
    }

    // ========== HANDLE GET MUTE TYPE ==========
    async _handleGetMuteType(ws, args) {
        try {
            const getMuteRoom = args[0];
            if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
                const roomData = await this._getRoomData(getMuteRoom);
                this.safeSend(ws, ["muteTypeResponse", roomData?.muted || false, getMuteRoom]);
            }
        } catch(e) {}
    }

    // ========== FETCH ==========
    async fetch(req) {
        if (this.closing || this.isDestroyed) {
            return new Response("Shutting down", { status: 503 });
        }

        try {
            const upgrade = req.headers.get("Upgrade");
            if (upgrade !== "websocket") {
                return new Response("Chat Server - D1 Version", {
                    status: 200,
                    headers: {
                        "Cache-Control": "no-cache"
                    }
                });
            }

            if (this.wsSet.size >= CONFIG.MAX_GLOBAL_CONNECTIONS) {
                return new Response("Server full", { status: 503 });
            }

            const pair = new WebSocketPair();
            const [client, server] = [pair[0], pair[1]];

            try {
                server.accept();
            } catch(e) {
                return new Response("WebSocket acceptance failed", { status: 500 });
            }

            const wsId = ++this._wsIdCounter;
            server._wsId = wsId;
            server.username = null;
            server.room = null;
            server.roomname = null;
            server.idtarget = null;
            server._closing = false;

            this.wsIdMap.set(wsId, server);

            server.addEventListener("message", async (event) => {
                try {
                    if (server._closing || this.closing || this.isDestroyed) return;
                    await this.handleMessage(server, event.data);
                } catch(e) {}
            });

            server.addEventListener("close", () => {
                this.cleanup(server);
            });

            server.addEventListener("error", () => {
                this.cleanup(server);
            });

            if (!this.wsSet.has(server)) {
                this.wsSet.add(server);
            }

            for (const room of ROOMS) {
                if (!this.roomClients.has(room)) {
                    this.roomClients.set(room, new Set());
                }
            }

            return new Response(null, { status: 101, webSocket: client });

        } catch(e) {
            console.error("Fetch error:", e);
            return new Response("Internal Server Error", { status: 500 });
        }
    }

    // ========== DESTROY ==========
    async destroy() {
        if (this.isDestroyed) return;
        this.closing = true;
        this.isDestroyed = true;

        this._joinLocks.clear();
        this._kursiLocks.clear();

        if (this._cleanupInterval) {
            clearInterval(this._cleanupInterval);
            this._cleanupInterval = null;
        }

        if (this._mainInterval) {
            clearInterval(this._mainInterval);
            this._mainInterval = null;
        }

        const wsCopy = Array.from(this.wsSet);
        for (const ws of wsCopy) {
            if (ws?.readyState === 1) {
                try {
                    ws.send(JSON.stringify(["serverShutdown", "Server shutting down"]));
                } catch(e) {}
                try {
                    ws.close(1000, "Shutdown");
                } catch(e) {}
            }
            try {
                this.cleanup(ws);
            } catch(e) {}
        }

        this.wsSet.clear();
        this.userConnections.clear();
        this.userSeat.clear();
        this.userRoom.clear();
        this.wsActiveMulti.clear();
        this.roomClients.clear();
        this._processingMessages.clear();
        this._cleaningUp.clear();
        this._eventQueue.clear();
        this.wsIdMap.clear();
    }
}
