// ==================== WORKER.JS ====================
// VERSION: 4.0.0 - D1 MIGRATION

import { ChatWorker } from "./chat-worker.js";
import { GameWorker } from "./game-worker.js";

// Konfigurasi
const CONFIG = {
    MAX_ROOMS: 12,
    MAX_GLOBAL_CONNECTIONS: 150,
    MAX_SEATS: 45,
    CLEANUP_INTERVAL_MS: 60000,
    LOCK_TIMEOUT: 10000,
    BATCH_SIZE: 20,
};

// Cache untuk instance
const instanceCache = new Map();
const CACHE_TTL = 30000;

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
                    // Baca dan jalankan schema
                    const schema = await fetch(new URL('./schema.sql', import.meta.url)).then(r => r.text());
                    const statements = schema.split(';').filter(s => s.trim());
                    
                    for (const stmt of statements) {
                        if (stmt.trim()) {
                            await env.DB.prepare(stmt).run();
                        }
                    }
                    
                    // Inisialisasi rooms
                    for (const room of ROOMS) {
                        await env.DB.prepare(
                            `INSERT OR IGNORE INTO rooms (room_name, number) VALUES (?, 1)`
                        ).bind(room).run();
                    }
                    
                    return new Response(JSON.stringify({
                        status: "success",
                        message: "Database initialized",
                        rooms: ROOMS
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
            if (pathname === "/game/ws") {
                const gameWorker = new GameWorker(env);
                return gameWorker.fetch(request);
            }

            // ========== HEALTH CHECK ==========
            if (pathname === "/health" || pathname === "/game/health") {
                try {
                    const [chatCount, gameCount, roomCount] = await Promise.all([
                        env.DB.prepare(`SELECT COUNT(*) as count FROM user_connections`).first(),
                        env.DB.prepare(`SELECT COUNT(*) as count FROM active_games WHERE is_active = 1`).first(),
                        env.DB.prepare(`SELECT COUNT(*) as count FROM rooms`).first(),
                    ]);

                    return new Response(JSON.stringify({
                        status: "ok",
                        timestamp: Date.now(),
                        version: "4.0.0",
                        database: "D1",
                        connections: chatCount?.count || 0,
                        activeGames: gameCount?.count || 0,
                        rooms: roomCount?.count || 0,
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

            // ========== STATS ==========
            if (pathname === "/stats") {
                try {
                    const stats = await env.DB.prepare(`
                        SELECT 
                            (SELECT COUNT(*) FROM user_connections) as total_users,
                            (SELECT COUNT(*) FROM active_games WHERE is_active = 1) as active_games,
                            (SELECT COUNT(*) FROM seats) as total_seats,
                            (SELECT COUNT(*) FROM dice_points) as dice_players,
                            (SELECT COUNT(*) FROM game_players) as game_players
                    `).first();

                    return new Response(JSON.stringify(stats), {
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch(e) {
                    return new Response(JSON.stringify({ error: e.message }), {
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

            return new Response("Server running", { status: 200 });

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
        const staleThreshold = now - 300000; // 5 menit

        // Hapus user connections yang sudah tidak aktif
        await env.DB.prepare(`
            DELETE FROM user_connections 
            WHERE updated_at < ?
        `).bind(Math.floor(staleThreshold / 1000)).run();

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

        // Cleanup game yang sudah selesai
        await env.DB.prepare(`
            DELETE FROM active_games 
            WHERE game_ended = 1 AND created_at < ?
        `).bind(Math.floor((now - 3600000) / 1000)).run();

        console.log("Cleanup completed");
    } catch(e) {
        console.error("Cleanup error:", e);
    }
}
