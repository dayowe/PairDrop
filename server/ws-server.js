import WebSocket, {WebSocketServer} from "ws";
import crypto from "crypto"

import Peer from "./peer.js";
import {hasher, randomizer} from "./helper.js";

const PAIR_KEY_LIFETIME = 10 * 60 * 1000;

export default class PairDropWsServer {

    constructor(server, conf) {
        this._conf = conf

        // Each kind of room owns a separate registry, even if two room IDs match.
        this._rooms = new Map([
            ['ip', new Map()],
            ['secret', new Map()],
            ['public-id', new Map()]
        ]);
        this._roomSecrets = new Map(); // pending invitations by pairing code

        this._wss = new WebSocketServer({ server });
        this._wss.on('connection', (socket, request) => this._onConnection(new Peer(socket, request, conf)));
    }

    _onConnection(peer) {
        peer.socket.on('message', message => this._onMessage(peer, message));
        peer.socket.onerror = e => console.error(e);

        peer.lastBeat = Date.now();
        this._keepAlive(peer);

        this._send(peer, {
            type: 'ws-config',
            wsConfig: {
                rtcConfig: this._conf.rtcConfig,
                wsFallback: this._conf.wsFallback
            }
        });

        // send displayName
        this._send(peer, {
            type: 'display-name',
            displayName: peer.name.displayName,
            deviceName: peer.name.deviceName,
            peerId: peer.id,
            peerIdHash: hasher.hashCodeSalted(peer.id)
        });
    }

    _onMessage(sender, message) {
        // Try to parse message
        try {
            message = JSON.parse(message);
        } catch (e) {
            console.warn("WS: Received JSON is malformed");
            return;
        }

        if (!message || typeof message !== 'object') return;
        switch (message.type) {
            case 'disconnect':
                this._onDisconnect(sender);
                break;
            case 'pong':
                sender.lastBeat = Date.now();
                break;
            case 'join-ip-room':
                this._joinIpRoom(sender);
                break;
            case 'room-secrets':
                this._onRoomSecrets(sender, message);
                break;
            case 'room-secrets-deleted':
                this._onRoomSecretsDeleted(sender, message);
                break;
            case 'pair-device-initiate':
                this._onPairDeviceInitiate(sender);
                break;
            case 'pair-device-join':
                this._onPairDeviceJoin(sender, message);
                break;
            case 'pair-device-cancel':
                this._onPairDeviceCancel(sender);
                break;
            case 'regenerate-room-secret':
                this._onRegenerateRoomSecret(sender, message);
                break;
            case 'create-public-room':
                this._onCreatePublicRoom(sender);
                break;
            case 'join-public-room':
                this._onJoinPublicRoom(sender, message);
                break;
            case 'leave-public-room':
                this._onLeavePublicRoom(sender);
                break;
            case 'signal':
                this._signalAndRelay(sender, message);
                break;
            case 'request':
            case 'header':
            case 'partition':
            case 'partition-received':
            case 'progress':
            case 'files-transfer-response':
            case 'file-transfer-complete':
            case 'message-transfer-complete':
            case 'text':
            case 'display-name-changed':
            case 'ws-chunk':
                // relay ws-fallback
                if (this._conf.wsFallback) {
                    this._signalAndRelay(sender, message);
                }
                else {
                    console.log("Websocket fallback is not activated on this instance.")
                }
        }
    }

    _memberRoom(peer, roomType, roomId) {
        const room = this._rooms.get(roomType)?.get(roomId);
        if (room?.get(peer.id) === peer) return room;
    }

    _signalAndRelay(sender, message) {
        const roomId = message.roomType === 'ip' ? sender.ip : message.roomId;
        const room = this._memberRoom(sender, message.roomType, roomId);
        if (!room || !Peer.isValidUuid(message.to)) return;

        const recipient = room.get(message.to);
        if (!recipient) return;
        delete message.to;
        message.sender = {id: sender.id, rtcSupported: sender.rtcSupported};
        this._send(recipient, message);
    }

    _onDisconnect(sender) {
        this._disconnect(sender);
    }

    _disconnect(sender) {
        this._removePairKey(sender.pairKey);
        clearTimeout(sender.keepAliveTimer);

        this._leaveIpRoom(sender, true);
        this._leaveAllSecretRooms(sender, true);
        this._leavePublicRoom(sender, true);

        sender.socket.terminate();
    }

    _onRoomSecrets(sender, message) {
        if (!Array.isArray(message.roomSecrets)) return;
        for (const secret of new Set(message.roomSecrets)) {
            if (typeof secret === 'string' && /^[\x00-\x7F]{64,256}$/.test(secret)) {
                this._joinSecretRoom(sender, secret);
            }
        }
    }

    _onRoomSecretsDeleted(sender, message) {
        if (!Array.isArray(message.roomSecrets)) return;
        for (const roomSecret of message.roomSecrets) {
            const room = this._memberRoom(sender, 'secret', roomSecret);
            if (!room) continue;
            for (const peer of room.values()) {
                this._leaveSecretRoom(peer, roomSecret, true);
                this._send(peer, {type: 'secret-room-deleted', roomSecret});
            }
        }
    }

    _onPairDeviceInitiate(sender) {
        this._removePairKey(sender.pairKey);
        const roomSecret = randomizer.getRandomString(256);
        const pairKey = this._createPairKey(sender, roomSecret);
        sender.pairKey = pairKey;

        this._send(sender, {
            type: 'pair-device-initiated',
            roomSecret: roomSecret,
            pairKey: pairKey
        });
    }

    _onPairDeviceJoin(sender, message) {
        if (sender.rateLimitReached()) {
            this._send(sender, {type: 'join-key-rate-limit'});
            return;
        }
        const entry = this._roomSecrets.get(message.pairKey);
        if (entry && (entry.expiresAt <= Date.now() || entry.creator.socket.readyState !== WebSocket.OPEN)) {
            this._onPairDeviceCancel(entry.creator);
        }
        if (!entry || entry.creator.pairKey !== message.pairKey || entry.creator.id === sender.id ||
            sender.socket.readyState !== WebSocket.OPEN) {
            this._send(sender, {type: 'pair-device-join-key-invalid'});
            return;
        }

        const {roomSecret, creator} = entry;
        this._removePairKey(message.pairKey);
        this._removePairKey(sender.pairKey);
        // The clients wait for these confirmations before handling peer discovery.
        this._send(sender, {type: 'pair-device-joined', roomSecret, peerId: creator.id});
        this._send(creator, {type: 'pair-device-joined', roomSecret, peerId: sender.id});
        this._joinSecretRoom(creator, roomSecret);
        this._joinSecretRoom(sender, roomSecret);
    }

    _onPairDeviceCancel(sender) {
        const pairKey = sender.pairKey

        if (!pairKey) return;

        this._removePairKey(pairKey);
        this._send(sender, {
            type: 'pair-device-canceled',
            pairKey: pairKey,
        });
    }

    _onCreatePublicRoom(sender) {
        let publicRoomId;
        do {
            publicRoomId = randomizer.getRandomString(5, true).toLowerCase();
        } while (this._rooms.get('public-id').has(publicRoomId));

        this._send(sender, {
            type: 'public-room-created',
            roomId: publicRoomId
        });

        this._joinPublicRoom(sender, publicRoomId);
    }

    _onJoinPublicRoom(sender, message) {
        if (sender.rateLimitReached()) {
            this._send(sender, { type: 'join-key-rate-limit' });
            return;
        }

        if (typeof message.publicRoomId !== 'string' || !/^[a-z]{5}$/.test(message.publicRoomId) ||
            (!this._rooms.get('public-id').has(message.publicRoomId) && !message.createIfInvalid)) {
            this._send(sender, { type: 'public-room-id-invalid', publicRoomId: message.publicRoomId });
            return;
        }

        this._joinPublicRoom(sender, message.publicRoomId);
    }

    _onLeavePublicRoom(sender) {
        this._leavePublicRoom(sender, true);
        this._send(sender, { type: 'public-room-left' });
    }

    _onRegenerateRoomSecret(sender, message) {
        const oldRoomSecret = message.roomSecret;
        const room = this._memberRoom(sender, 'secret', oldRoomSecret);
        if (!room) return;
        const newRoomSecret = randomizer.getRandomString(256);

        // notify all other peers
        for (const peer of room.values()) {
            this._send(peer, {
                type: 'room-secret-regenerated',
                oldRoomSecret: oldRoomSecret,
                newRoomSecret: newRoomSecret,
            });
            peer.removeRoomSecret(oldRoomSecret);
        }
        this._rooms.get('secret').delete(oldRoomSecret);
    }

    _createPairKey(creator, roomSecret) {
        let pairKey;
        do {
            // get randomInt until keyRoom not occupied
            pairKey = crypto.randomInt(1000000, 1999999).toString().substring(1); // include numbers with leading 0s
        } while (this._roomSecrets.has(pairKey));

        const expiresAt = Date.now() + PAIR_KEY_LIFETIME;
        const timer = setTimeout(() => this._onPairDeviceCancel(creator), PAIR_KEY_LIFETIME);
        timer.unref();
        this._roomSecrets.set(pairKey, {roomSecret, creator, expiresAt, timer});
        return pairKey;
    }

    _removePairKey(pairKey) {
        const entry = this._roomSecrets.get(pairKey);
        if (!entry) return;
        clearTimeout(entry.timer);
        entry.creator.pairKey = null;
        this._roomSecrets.delete(pairKey);
    }

    _joinIpRoom(peer) {
        this._joinRoom(peer, 'ip', peer.ip);
    }

    _joinSecretRoom(peer, roomSecret) {
        this._joinRoom(peer, 'secret', roomSecret);

        // add secret to peer
        peer.addRoomSecret(roomSecret);
    }

    _joinPublicRoom(peer, publicRoomId) {
        // prevent joining of 2 public rooms simultaneously
        this._leavePublicRoom(peer);

        this._joinRoom(peer, 'public-id', publicRoomId);

        peer.publicRoomId = publicRoomId;
    }

    _joinRoom(peer, roomType, roomId) {
        const rooms = this._rooms.get(roomType);
        const previous = rooms.get(roomId)?.get(peer.id);
        if (previous) {
            // Announce departure before rediscovery. Late cleanup only affects its own socket.
            this._leaveRoom(previous, roomType, roomId);
        }
        if (!rooms.has(roomId)) rooms.set(roomId, new Map());
        const room = rooms.get(roomId);
        room.set(peer.id, peer);
        this._notifyPeers(peer, roomType, roomId, room);
    }

    _leaveIpRoom(peer, disconnect = false) {
        this._leaveRoom(peer, 'ip', peer.ip, disconnect);
    }

    _leaveSecretRoom(peer, roomSecret, disconnect = false) {
        this._leaveRoom(peer, 'secret', roomSecret, disconnect)

        //remove secret from peer
        peer.removeRoomSecret(roomSecret);
    }

    _leavePublicRoom(peer, disconnect = false) {
        if (!peer.publicRoomId) return;

        this._leaveRoom(peer, 'public-id', peer.publicRoomId, disconnect);

        peer.publicRoomId = null;
    }

    _leaveRoom(peer, roomType, roomId, disconnect = false) {
        const room = this._memberRoom(peer, roomType, roomId);
        if (!room) return;
        room.delete(peer.id);
        if (!room.size) this._rooms.get(roomType).delete(roomId);
        for (const otherPeer of room.values()) {
            this._send(otherPeer, {type: 'peer-left', peerId: peer.id, roomType, roomId, disconnect});
        }
    }

    _notifyPeers(peer, roomType, roomId, room) {
        const peers = [];
        for (const otherPeer of room.values()) {
            if (otherPeer === peer) continue;
            this._send(otherPeer, {type: 'peer-joined', peer: peer.getInfo(), roomType, roomId});
            peers.push(otherPeer.getInfo());
        }
        this._send(peer, {type: 'peers', peers, roomType, roomId});
    }

    _leaveAllSecretRooms(peer, disconnect = false) {
        for (const secret of peer.roomSecrets) this._leaveSecretRoom(peer, secret, disconnect);
    }

    _send(peer, message) {
        if (peer.socket.readyState === WebSocket.OPEN) peer.socket.send(JSON.stringify(message));
    }

    _keepAlive(peer) {
        if (Date.now() - peer.lastBeat > 5000) {
            this._disconnect(peer);
            return;
        }
        this._send(peer, {type: 'ping'});
        peer.keepAliveTimer = setTimeout(() => this._keepAlive(peer), 1000);
    }

}
