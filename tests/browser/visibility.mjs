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
