const http = require('http');
const { WebSocketServer } = require('ws');

const port = process.env.PORT || 8080;

// ── Auth Code Relay (for mobile OAuth) ──────────────────────
// Stores auth codes temporarily: sessionId -> { code, timestamp }
const authCodes = new Map();

// Clean up expired codes every 60 seconds
setInterval(() => {
    const now = Date.now();
    for (const [key, val] of authCodes.entries()) {
        if (now - val.timestamp > 300000) { // 5 minutes expiry
            authCodes.delete(key);
        }
    }
}, 60000);

// ── HTTP Server ─────────────────────────────────────────────
const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://localhost:${port}`);

    // CORS headers for game client polling
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET');

    if (url.pathname === '/auth/callback') {
        // Google OAuth redirects here with ?code=XXX&state=SESSION_ID
        const code = url.searchParams.get('code');
        const state = url.searchParams.get('state'); // session ID from game

        if (code && state) {
            authCodes.set(state, { code, timestamp: Date.now() });
            console.log(`[Auth] Stored auth code for session: ${state}`);

            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(`<html><body style="font-family:sans-serif; text-align:center; padding-top:100px; background:#121116; color:#fff;">
                <h1 style="color:#1db954;">Đăng nhập thành công!</h1>
                <p style="font-size:18px;">Bạn có thể đóng trình duyệt này và quay lại trò chơi.</p>
            </body></html>`);
        } else {
            res.writeHead(400, { 'Content-Type': 'text/plain' });
            res.end('Missing code or state parameter');
        }
    }
    else if (url.pathname === '/auth/poll') {
        // Game client polls this endpoint to retrieve the auth code
        const session = url.searchParams.get('session');
        if (session && authCodes.has(session)) {
            const data = authCodes.get(session);
            authCodes.delete(session); // One-time use
            console.log(`[Auth] Delivered auth code for session: ${session}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ code: data.code }));
        } else {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ code: null }));
        }
    }
    else {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Chess3D Signaling Server - Running');
    }
});

// ── WebSocket Server (attached to HTTP server) ──────────────
const wss = new WebSocketServer({ server });

console.log(`[Signaling] Server running on port ${port}`);

const rooms = new Map(); // roomCode -> Map(peerId -> socket)

wss.on('connection', (ws) => {
    let currentRoom = null;
    let currentPeerId = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const type = data.type;

            if (type === 'create') {
                const roomCode = data.room;
                const peerId = data.peer_id;
                currentRoom = roomCode;
                currentPeerId = peerId;

                if (!rooms.has(roomCode)) {
                    rooms.set(roomCode, new Map());
                }
                rooms.get(roomCode).set(peerId, ws);
                console.log(`[Create] Peer ${peerId} created/joined room ${roomCode}`);
            } 
            else if (type === 'join') {
                const roomCode = data.room;
                const peerId = data.peer_id;
                currentRoom = roomCode;
                currentPeerId = peerId;

                if (!rooms.has(roomCode)) {
                    rooms.set(roomCode, new Map());
                }
                rooms.get(roomCode).set(peerId, ws);
                console.log(`[Join] Peer ${peerId} joined room ${roomCode}`);

                // Notify other peers in the room that a new peer joined
                const peers = rooms.get(roomCode);
                for (const [otherId, otherWs] of peers.entries()) {
                    if (otherId !== peerId) {
                        otherWs.send(JSON.stringify({ type: 'peer_joined', peer_id: peerId }));
                    }
                }
            } 
            else if (type === 'join_random') {
                const peerId = data.peer_id;
                let foundRoom = null;

                for (const [roomCode, peers] of rooms.entries()) {
                    if (peers.size === 1) {
                        foundRoom = roomCode;
                        break;
                    }
                }

                if (foundRoom) {
                    currentRoom = foundRoom;
                    currentPeerId = peerId;
                    rooms.get(foundRoom).set(peerId, ws);
                    console.log(`[Join Random] Peer ${peerId} joined random room ${foundRoom}`);

                    ws.send(JSON.stringify({ type: 'joined_random', room: foundRoom }));

                    const peers = rooms.get(foundRoom);
                    for (const [otherId, otherWs] of peers.entries()) {
                        if (otherId !== peerId) {
                            otherWs.send(JSON.stringify({ type: 'peer_joined', peer_id: peerId }));
                        }
                    }
                } else {
                    ws.send(JSON.stringify({ type: 'no_rooms_available' }));
                }
            }
            else if (type === 'signal') {
                // Forward signaling message (offer/answer/candidate) to the target peer
                const roomCode = data.room;
                const targetId = data.target_id;
                
                if (rooms.has(roomCode)) {
                    const peers = rooms.get(roomCode);
                    if (peers.has(targetId)) {
                        peers.get(targetId).send(JSON.stringify({
                            type: 'signal',
                            sender_id: currentPeerId,
                            signal: data.signal
                        }));
                    }
                }
            }
        } catch (e) {
            console.error('[Error] Parsing message:', e);
        }
    });

    ws.on('close', () => {
        if (currentRoom && currentPeerId) {
            if (rooms.has(currentRoom)) {
                const peers = rooms.get(currentRoom);
                peers.delete(currentPeerId);
                console.log(`[Disconnect] Peer ${currentPeerId} left room ${currentRoom}`);
                
                // Notify others
                for (const [otherId, otherWs] of peers.entries()) {
                    otherWs.send(JSON.stringify({ type: 'peer_left', peer_id: currentPeerId }));
                }

                if (peers.size === 0) {
                    rooms.delete(currentRoom);
                    console.log(`[Cleanup] Room ${currentRoom} is empty, deleted.`);
                }
            }
        }
    });
});

server.listen(port, () => {
    console.log(`[Server] HTTP + WebSocket server listening on port ${port}`);
});
