import assert from 'node:assert/strict';
import {test} from 'node:test';
import {chromium} from 'playwright-core';
import {startLocalServer} from './local-server.mjs';

test('returning to a visible page reconnects a disconnected signaling socket once', async t => {
    const origin = await startLocalServer(t);

    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const page = await browser.newPage();
    let connections = 0;
    page.on('websocket', () => connections++);
    await page.goto(origin);
    await page.waitForFunction(() => sessionStorage.getItem('peer_id'));

    const states = await page.evaluate(() => {
        const connection = pairDrop.server;
        connection._disconnect();

        // Headless tabs stay visible, so drive both states through the document event.
        const notify = () => document.dispatchEvent(new Event('visibilitychange', {bubbles: true}));
        Object.defineProperty(document, 'hidden', {configurable: true, value: true});
        notify();
        const stayedDisconnected = !connection._socket;
        delete document.hidden;
        notify();
        const socket = connection._socket;
        const startedConnecting = socket?.readyState === WebSocket.CONNECTING;
        notify();
        return {stayedDisconnected, startedConnecting, sameSocket: connection._socket === socket};
    });
    assert.deepEqual(states, {stayedDisconnected: true, startedConnecting: true, sameSocket: true});
    await page.waitForFunction(() => pairDrop.server._isConnected());
    assert.ok(await page.evaluate(() => {
        const socket = pairDrop.server._socket;
        document.dispatchEvent(new Event('visibilitychange', {bubbles: true}));
        return pairDrop.server._socket === socket;
    }));
    assert.equal(connections, 2); // The initial connection and one foreground reconnect.
});

test('a hidden client replaces a silent OPEN socket once and rejoins the default room', async t => {
    const origin = await startLocalServer(t);
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const observer = await browser.newPage();
    await observer.goto(origin);
    await observer.waitForFunction(() => sessionStorage.getItem('peer_id'));
    const client = await browser.newPage();
    let connections = 0;
    let silent = false;
    await client.routeWebSocket(/\/server\?/, route => {
        const server = route.connectToServer();
        if (++connections !== 1) return;
        server.onMessage(data => { if (!silent) route.send(data); });
        route.onMessage(data => { if (!silent) server.send(data); });
        server.onClose(() => {}); // The client end can stay OPEN across sleep.
    });
    await client.goto(origin);
    await client.waitForFunction(() => document.querySelector('x-peer'));
    const id = await client.evaluate(() => sessionStorage.getItem('peer_id'));
    await observer.evaluate(id => {
        window.rediscovered = false;
        Events.on('peer-joined', ({detail}) => {
            if (detail.roomType === 'ip' && detail.peer.id === id) window.rediscovered = true;
        });
    }, id);
    await client.evaluate(() => {
        Object.defineProperty(document, 'hidden', {configurable: true, value: true});
        window.rejoined = false;
        Events.on('peers', ({detail}) => {
            if (detail.roomType === 'ip' && detail.peers.length === 1) window.rejoined = true;
        });
    });
    silent = true;
    const wasOpen = await client.evaluate(() => {
        const connection = pairDrop.server;
        connection._lastActivity -= 60000;
        document.dispatchEvent(new Event('resume'));
        document.dispatchEvent(new Event('resume'));
        return connection._socket.readyState === WebSocket.OPEN;
    });
    assert.ok(wasOpen);
    await client.waitForFunction(() => window.rejoined);
    await observer.waitForFunction(() => window.rediscovered);
    assert.equal(connections, 2);
});

test('a heartbeat during the wake grace period preserves the connected peer', async t => {
    const origin = await startLocalServer(t);
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const peer = await browser.newPage();
    const client = await browser.newPage();
    await Promise.all([peer.goto(origin), client.goto(origin)]);
    await client.waitForFunction(() => Object.values(pairDrop.peers.peers).some(p => p._channel?.readyState === 'open'));
    await client.evaluate(() => {
        const connection = pairDrop.server;
        window.originalSocket = connection._socket;
        window.originalChannel = Object.values(pairDrop.peers.peers)[0]._channel;
        connection._lastActivity -= 60000;
        document.dispatchEvent(new Event('resume'));
        window.dispatchEvent(new Event('focus'));
    });
    await client.waitForFunction(() => pairDrop.server._probeTimer === null);
    assert.ok(await client.evaluate(() => pairDrop.server._socket === window.originalSocket &&
        Object.values(pairDrop.peers.peers)[0]._channel === window.originalChannel &&
        window.originalChannel.readyState === 'open'));
});
