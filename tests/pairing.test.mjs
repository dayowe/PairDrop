import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {EventEmitter} from 'node:events';
import {createServer} from 'node:http';
import {test} from 'node:test';
import WebSocket from 'ws';

import Peer from '../server/peer.js';
import PairDropWsServer from '../server/ws-server.js';

function setup(t) {
    const server = new PairDropWsServer(createServer(), {wsFallback: true});
    const clients = [];
    t.after(() => {
        for (const {peer} of clients) server._disconnect(peer);
        server._wss.close();
    });

    function connect(identity, headers = {}) {
        const messages = [];
        const socket = new EventEmitter();
        socket.readyState = WebSocket.OPEN;
        socket.send = raw => messages.push(JSON.parse(raw));
        socket.terminate = () => { socket.readyState = WebSocket.CLOSED; };
        const url = new URL('http://localhost/server');
        if (identity) {
            url.searchParams.set('peer_id', identity.peerId);
            url.searchParams.set('peer_id_hash', identity.peerIdHash);
        }
        const peer = new Peer(socket, {url: url.href, socket: {remoteAddress: '127.0.0.1'}, headers}, {});
        server._onConnection(peer);
        const client = {
            peer, messages,
            send: (type, data = {}) => socket.emit('message', JSON.stringify({type, ...data})),
            last: type => messages.filter(msg => msg.type === type).at(-1)
        };
        clients.push(client);
        return client;
    }

    return {server, connect};
}

test('clients without a verified address can pair while their IP discovery stays empty', t => {
    const {server, connect} = setup(t);
    const a = connect(undefined, {'x-forwarded-for': '192.0.2.1'});
    const b = connect(undefined, {'x-forwarded-for': '192.0.2.2'});
    for (const client of [a, b]) {
        client.send('join-ip-room');
        assert.deepEqual(client.last('peers').peers, []);
    }
    assert.equal(server._rooms.get('ip').size, 0);
    a.send('pair-device-initiate');
    b.send('pair-device-join', {pairKey: a.last('pair-device-initiated').pairKey});
    assert.equal(a.last('peer-joined').peer.id, b.peer.id);
    assert.equal(b.last('peers').roomType, 'secret');
});

test('unused, replaced and cancelled invitations never create memberships', t => {
    const {server, connect} = setup(t);
    const a = connect();
    a.send('pair-device-initiate');
    const first = a.last('pair-device-initiated');
    a.send('pair-device-initiate');
    const second = a.last('pair-device-initiated');
    assert.equal(server._roomSecrets.get(second.pairKey).roomSecret, second.roomSecret);
    assert.equal(server._roomSecrets.size, 1);
    assert.notEqual(first.roomSecret, second.roomSecret);
    a.send('pair-device-cancel');
    assert.equal(a.last('pair-device-canceled').pairKey, second.pairKey);
    assert.equal(server._roomSecrets.size, 0);
    assert.equal(a.peer.roomSecrets.size, 0);
    assert.equal(server._rooms.get('secret').size, 0);
});

test('an expired or disconnected invitation cannot confirm a pairing', t => {
    const {server, connect} = setup(t);
    const a = connect(), b = connect();
    for (const reason of ['expired', 'closed']) {
        a.send('pair-device-initiate');
        const {pairKey} = a.last('pair-device-initiated');
        if (reason === 'expired') server._roomSecrets.get(pairKey).expiresAt = Date.now() - 1;
        else a.peer.socket.terminate();
        b.send('pair-device-join', {pairKey});
        assert.equal(b.messages.at(-1).type, 'pair-device-join-key-invalid');
        assert.equal(server._roomSecrets.size, 0);
        assert.equal(server._rooms.get('secret').size, 0);
    }
    assert.equal(b.last('pair-device-joined'), undefined);
});

test('redemption confirms both devices before discovery and keeps existing pairings', t => {
    const {server, connect} = setup(t);
    const a = connect(), b = connect();
    const saved = Array.from({length: 101}, (_, i) => String(i).padStart(64, '0'));
    a.send('room-secrets', {roomSecrets: saved});
    b.send('pair-device-initiate');
    a.send('pair-device-initiate');
    const {pairKey, roomSecret} = a.last('pair-device-initiated');
    a.messages.length = b.messages.length = 0;
    b.send('pair-device-join', {pairKey});

    assert.equal(a.messages[0].type, 'pair-device-joined');
    assert.equal(b.messages[0].type, 'pair-device-joined');
    assert.equal(a.last('peer-joined').peer.id, b.peer.id);
    assert.equal(b.last('peers').peers[0].id, a.peer.id);
    assert.equal(a.peer.roomSecrets.size, 102);
    assert.equal(server._roomSecrets.size, 0);
    b.send('pair-device-join', {pairKey});
    assert.equal(b.messages.at(-1).type, 'pair-device-join-key-invalid');

    a.send('regenerate-room-secret', {roomSecret});
    const rotated = a.last('room-secret-regenerated').newRoomSecret;
    assert.equal(b.last('room-secret-regenerated').newRoomSecret, rotated);
    for (const client of [a, b]) client.send('room-secrets', {roomSecrets: [rotated]});
    b.send('room-secrets-deleted', {roomSecrets: [rotated]});
    assert.equal(a.last('secret-room-deleted').roomSecret, rotated);
    assert.equal(b.last('secret-room-deleted').roomSecret, rotated);
    assert.equal(a.peer.roomSecrets.size, 101);
});

test('accepts numeric six-digit codes and preserves leading zeros in string codes', t => {
    const {connect} = setup(t);
    const a = connect(), b = connect();
    const random = t.mock.method(crypto, 'randomInt');
    for (const [digits, wireCode] of [['654321', 654321], ['000123', '000123']]) {
        random.mock.mockImplementation(() => Number('1' + digits));
        a.send('pair-device-initiate');
        assert.equal(a.last('pair-device-initiated').pairKey, digits);
        b.send('pair-device-join', {pairKey: wireCode});
        assert.equal(b.messages.at(-1).type, 'peers');
        assert.equal(b.last('pair-device-joined').peerId, a.peer.id);
    }
});

test('late disconnect preserves the replacement socket in each kind of room', t => {
    const {server, connect} = setup(t);
    const old = connect(), partner = connect();
    old.send('pair-device-initiate');
    const {pairKey, roomSecret} = old.last('pair-device-initiated');
    partner.send('pair-device-join', {pairKey});
    old.send('create-public-room');
    const publicRoomId = old.last('public-room-created').roomId;
    partner.send('join-public-room', {publicRoomId});
    old.send('join-ip-room');
    partner.send('join-ip-room');

    const current = connect(old.last('display-name'));
    assert.equal(current.peer.id, old.peer.id);
    current.send('join-ip-room');
    current.send('join-public-room', {publicRoomId});
    current.send('room-secrets', {roomSecrets: [roomSecret]});
    for (const [type, id] of [['ip', current.peer.ip], ['secret', roomSecret], ['public-id', publicRoomId]]) {
        assert.equal(server._memberRoom(old.peer, type, id), undefined);
        assert.ok(server._memberRoom(current.peer, type, id));
    }
    assert.equal(server._memberRoom(current.peer, 'public-id', roomSecret), undefined);
    partner.messages.length = 0;
    old.send('disconnect');
    assert.equal(partner.last('peer-left'), undefined);

    for (const [roomType, roomId] of [['ip', current.peer.ip], ['public-id', publicRoomId], ['secret', roomSecret]]) {
        current.send('text', {to: partner.peer.id, roomType, roomId, text: 'aGk='});
        assert.equal(partner.last('text').roomType, roomType);
        assert.equal(partner.last('text').sender.id, current.peer.id);
    }
    current.send('disconnect');
    assert.equal(partner.messages.filter(msg => msg.type === 'peer-left' && msg.disconnect).length, 3);
});
