const { WebSocketServer } = require('ws');

const port = process.env.PORT || 8080;
const wss = new WebSocketServer({ port });

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
