// 평가위원회 타이머 — 정적 파일 + WebSocket 동기화 서버
// 실행: npm install && npm start  (기본 포트 3000, PORT 환경변수로 변경)
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") { res.writeHead(200); res.end("ok"); return; }
  let file = req.url.split("?")[0];
  if (file === "/") file = "/index.html";
  const full = path.join(ROOT, path.normalize(file));
  if (!full.startsWith(ROOT)) { res.writeHead(403); res.end(); return; }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404); res.end("not found"); return; }
    res.writeHead(200, { "Content-Type": MIME[path.extname(full)] || "application/octet-stream" });
    res.end(data);
  });
});

// room id → { state, clients:Set<ws> }
const rooms = new Map();

function roomOf(ws) {
  return ws._room ? rooms.get(ws._room) : null;
}
function broadcastClients(room) {
  const msg = JSON.stringify({ type: "clients", count: room.clients.size, serverNow: Date.now() });
  for (const c of room.clients) if (c.readyState === 1) c.send(msg);
}

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

wss.on("connection", ws => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === "join" && typeof msg.room === "string" && msg.room.length <= 64) {
      // 기존 방에서 제거
      const prev = roomOf(ws);
      if (prev) { prev.clients.delete(ws); broadcastClients(prev); }

      ws._room = msg.room;
      let room = rooms.get(msg.room);
      if (!room) { room = { state: null, clients: new Set() }; rooms.set(msg.room, room); }
      room.clients.add(ws);
      ws.send(JSON.stringify({
        type: "joined", room: msg.room,
        clients: room.clients.size,
        state: room.state,
        serverNow: Date.now(),
      }));
      broadcastClients(room);
    }

    if (msg.type === "update" && msg.state && typeof msg.state === "object") {
      const room = roomOf(ws);
      if (!room) return;
      room.state = msg.state;
      const out = JSON.stringify({ type: "state", state: room.state, serverNow: Date.now() });
      for (const c of room.clients) {
        if (c !== ws && c.readyState === 1) c.send(out);
      }
    }

    if (msg.type === "ping") ws.send(JSON.stringify({ type: "pong", serverNow: Date.now() }));
  });

  ws.on("close", () => {
    const room = roomOf(ws);
    if (!room) return;
    room.clients.delete(ws);
    if (room.clients.size === 0) {
      // 마지막 화면이 나가도 10분간 상태 유지 (재접속 대비)
      setTimeout(() => {
        const r = rooms.get(ws._room);
        if (r && r.clients.size === 0) rooms.delete(ws._room);
      }, 10 * 60 * 1000);
    } else {
      broadcastClients(room);
    }
  });
});

// 죽은 연결 정리
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`평가위원회 타이머: http://localhost:${PORT}`);
});
