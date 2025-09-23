// lowcard.js
export class GameLowCard {
  constructor(server, room) {
    this.server = server;   // reference ke ChatServer
    this.room = room;
    this.phase = "registering";
    this.players = [];
    this.eliminated = new Set();
    this.submissions = new Map();
    this.round = 0;
    this.registerTimer = null;
    this.roundTimer = null;

    // auto end register after 20s
    this.registerTimer = setTimeout(() => this.beginRound(), 20 * 1000);

    this.broadcast(`🎮 Game LowCard dimulai! Ketik 'ij' untuk ikut (20 detik)`);
  }

  broadcast(msg, color = "#3498db") {
    this.server.broadcastToRoom(this.room, [
      "chat", this.room, "", "SYSTEM", msg, color, "#000"
    ]);
  }

  join(id) {
    if (this.phase !== "registering") return;
    if (!this.players.includes(id)) {
      this.players.push(id);
      this.broadcast(`✅ ${id} ikut bermain`, "#2ecc71");
    }
  }

  beginRound() {
    if (this.players.length < 2) {
      this.broadcast("❌ Pemain kurang dari 2, game dibatalkan", "#e74c3c");
      this.server.activeGames.delete(this.room);
      return;
    }
    this.phase = "playing";
    this.round = 1;
    this.submissions.clear();
    this.broadcast(`👉 Ronde 1 dimulai! Kirim angka (1–11) dalam 20 detik`);
    this.broadcast(`Masih bertahan: ${this.players.join(", ")}`, "#2ecc71");
    this.setupCountdown();
  }

  setupCountdown() {
    const countdownIntervals = [15, 10, 5];
    for (const t of countdownIntervals) {
      setTimeout(() => {
        if (this.phase === "playing" && this.roundTimer) {
          this.broadcast(`⏳ ${t} detik lagi`, "#9b59b6");
        }
      }, (20 - t) * 1000);
    }
    this.roundTimer = setTimeout(() => this.endRound(), 20 * 1000);
  }

  handleInput(id, num) {
    if (this.phase !== "playing") return "❌ Tidak ada game aktif";
    if (!this.players.includes(id)) return "❌ Kamu tidak terdaftar";
    if (this.eliminated.has(id)) return "❌ Kamu sudah tereliminasi";
    if (this.submissions.has(id)) return "❌ Sudah input angka";

    if (isNaN(num) || num < 1 || num > 11) return "❌ Input harus 1–11";

    this.submissions.set(id, num);
    this.broadcast(`${id} sudah mengirim angka ✔️`, "#2ecc71");
    return null;
  }

  endRound() {
    if (this.submissions.size === 0) {
      this.broadcast("😅 Tidak ada yang input angka di ronde ini", "#e67e22");
      return;
    }

    let lowest = Math.min(...this.submissions.values());
    const losers = [...this.submissions.entries()]
      .filter(([_, n]) => n === lowest)
      .map(([id]) => id);

    for (const id of losers) {
      this.eliminated.add(id);
      this.broadcast(`🚪 ${id} tereliminasi (angka: ${lowest})`, "#e74c3c");
    }

    const survivors = this.players.filter(p => !this.eliminated.has(p));
    if (survivors.length === 1) {
      this.broadcast(`🏆 Pemenang adalah ${survivors[0]}!`, "#f1c40f");
      this.server.activeGames.delete(this.room);
      return;
    }
    if (survivors.length === 0) {
      this.broadcast("😅 Game berakhir tanpa pemenang", "#95a5a6");
      this.server.activeGames.delete(this.room);
      return;
    }

    // ronde baru
    this.round++;
    this.submissions.clear();
    this.broadcast(`Masih bertahan: ${survivors.join(", ")}`, "#2ecc71");
    this.broadcast(`👉 Ronde ${this.round} dimulai! Kirim angka (1–11) dalam 20 detik`);
    this.setupCountdown();
  }
}
