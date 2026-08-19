// ==================== WORKER.JS ====================
// VERSION: 4.0.0 - D1 FULL INTEGRATION

import { ChatWorker } from "./chat-worker.js";
import { GameWorker } from "./game-worker.js";

// Room list
const ROOMS = [
    "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
    "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
    "Happy Vibes", "The Chatter Room"
];

// ==================== MAIN WORKER ====================
export default {
    async fetch(request, env) {
        try {
            const url = new URL(request.url);
            const pathname = url.pathname;

            // ========== INIT DATABASE ==========
            if (pathname === "/init") {
                try {
                    // Baca schema
                    const schema = await fetch(new URL('./schema.sql', import.meta.url)).then(r => r.text());
                    const statements = schema.split(';').filter(s => s.trim());
                    
                    for (const stmt of statements) {
                        if (stmt.trim()) {
                            await env.DB.prepare(stmt).run();
                        }
                    }
                    
                    // Insert rooms
                    for (const room of ROOMS) {
                        await env.DB.prepare(
                            `INSERT OR IGNORE INTO rooms (room_name, number) VALUES (?, 1)`
                        ).bind(room).run();
                    }
                    
                    return new Response(JSON.stringify({
                        status: "success",
                        message: "Database initialized",
                        rooms: ROOMS.length
                    }), {
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch(e) {
                    return new Response(JSON.stringify({
                        status: "error",
                        message: e.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json' }
                    });
                }
            }

            // ========== CHAT ==========
            if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
                const chatWorker = new ChatWorker(env);
                return chatWorker.fetch(request);
            }

            // ========== GAME ==========
            if (pathname === "/game/ws" || pathname === "/game") {
                const gameWorker = new GameWorker(env);
                return gameWorker.fetch(request);
            }

            // ========== HEALTH CHECK ==========
            if (pathname === "/health") {
                try {
                    const [roomCount, userCount, gameCount] = await Promise.all([
                        env.DB.prepare(`SELECT COUNT(*) as count FROM rooms`).first(),
                        env.DB.prepare(`SELECT COUNT(*) as count FROM user_connections`).first(),
                        env.DB.prepare(`SELECT COUNT(*) as count FROM active_games WHERE is_active = 1`).first(),
                    ]);

                    return new Response(JSON.stringify({
                        status: "ok",
                        timestamp: Date.now(),
                        version: "4.0.0",
                        database: "D1",
                        rooms: roomCount?.count || 0,
                        users: userCount?.count || 0,
                        activeGames: gameCount?.count || 0,
                    }), {
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch(e) {
                    return new Response(JSON.stringify({
                        status: "error",
                        message: e.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json' }
                    });
                }
            }

            // ========== ROOMS ==========
            if (pathname === "/rooms") {
                try {
                    const rooms = await env.DB.prepare(`
                        SELECT 
                            r.room_name,
                            r.muted,
                            r.number,
                            COUNT(s.seat_number) as user_count
                        FROM rooms r
                        LEFT JOIN seats s ON r.room_name = s.room_name
                        GROUP BY r.room_name
                        ORDER BY r.room_name
                    `).all();

                    return new Response(JSON.stringify({
                        rooms: rooms.results,
                        total: rooms.results.length
                    }), {
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch(e) {
                    return new Response(JSON.stringify({ error: e.message }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json' }
                    });
                }
            }

            return new Response("Chat & Game Server - D1 Version", { 
                status: 200,
                headers: {
                    'Content-Type': 'text/plain'
                }
            });

        } catch(e) {
            console.error("Fetch error:", e);
            return new Response(JSON.stringify({
                error: "Internal Server Error",
                message: e.message || "Unknown error"
            }), {
                status: 500,
                headers: {
                    'Retry-After': '30',
                    'Content-Type': 'application/json'
                }
            });
        }
    },

    // ========== SCHEDULED CLEANUP ==========
    async scheduled(event, env, ctx) {
        ctx.waitUntil(cleanupDatabase(env));
    }
};

// ========== CLEANUP FUNCTION ==========
async function cleanupDatabase(env) {
    try {
        const now = Date.now();
        const staleThreshold = Math.floor((now - 300000) / 1000); // 5 menit

        // Hapus user connections yang sudah tidak aktif
        await env.DB.prepare(`
            DELETE FROM user_connections 
            WHERE updated_at < ?
        `).bind(staleThreshold).run();

        // Hapus seats yang tidak terpakai
        await env.DB.prepare(`
            DELETE FROM seats 
            WHERE room_name NOT IN (SELECT room_name FROM rooms)
        `).run();

        // Hapus points yang tidak terpakai
        await env.DB.prepare(`
            DELETE FROM points 
            WHERE (room_name, seat_number) NOT IN (SELECT room_name, seat_number FROM seats)
        `).run();

        // Cleanup game yang sudah selesai (> 1 jam)
        const oneHourAgo = Math.floor((now - 3600000) / 1000);
        await env.DB.prepare(`
            DELETE FROM active_games 
            WHERE game_ended = 1 AND created_at < ?
        `).bind(oneHourAgo).run();

        console.log("Cleanup completed at:", new Date().toISOString());
    } catch(e) {
        console.error("Cleanup error:", e);
    }
}

export { ChatWorker, GameWorker };
