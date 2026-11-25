import assert from 'node:assert/strict';
import {once} from 'node:events';
import {test} from 'node:test';
import {chromium} from 'playwright-core';
import PairDropServer from '../../server/server.js';
import PairDropWsServer from '../../server/ws-server.js';

test('returning to a visible page reconnects a disconnected signaling socket once', async t => {
    const buttons = {
        donation_button: {}, twitter_button: {}, mastodon_button: {},
        bluesky_button: {}, custom_button: {}, privacypolicy_button: {}
    };
    const {server} = new PairDropServer({port: 0, localhostOnly: true, buttons});
    t.after(() => server.close());
    new PairDropWsServer(server, {rtcConfig: {iceServers: []}});
    await once(server, 'listening');

    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const page = await browser.newPage();
    let connections = 0;
    page.on('websocket', () => connections++);
    await page.goto('http://127.0.0.1:' + server.address().port);
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
