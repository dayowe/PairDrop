import assert from 'node:assert/strict';
import {fork} from 'node:child_process';
import {createHash} from 'node:crypto';
import {once} from 'node:events';
import {test} from 'node:test';
import {chromium} from 'playwright-core';

// Run with npm test on Node 20+ with Chrome installed, or set BROWSER_CHANNEL=msedge.
test('replace old discovery sessions while finishing existing transfers', {timeout: 90000}, async t => {
    let server;
    let port = 0;
    async function stopServer() {
        if (!server || server.exitCode !== null || server.signalCode !== null) return;
        const stopped = once(server, 'exit');
        server.kill();
        await stopped;
    }
    async function restartServer() {
        await stopServer();
        server = fork(new URL('fixtures/signaling-server.mjs', import.meta.url), [String(port)], {
            stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
        });
        const [address] = await once(server, 'message');
        port = address;
    }
    t.after(stopServer);
    await restartServer();
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());

    async function openClient() {
        const page = await browser.newPage();
        await page.goto('http://127.0.0.1:' + port);
        await page.waitForFunction(() => sessionStorage.getItem('peer_id'));
        return page;
    }
    const id = page => page.evaluate(() => sessionStorage.getItem('peer_id'));
    async function expectTiles(page, ids, includeHidden = false) {
        await page.waitForFunction(({ids, includeHidden}) => {
            const tiles = [...document.querySelectorAll(includeHidden ? 'x-peer' : 'x-peer:not([hidden])')];
            return tiles.length === ids.length && ids.every(id => tiles.some(tile => tile.id === id));
        }, {ids, includeHidden});
    }
    const a = await openClient();
    const b = await openClient();
    await expectTiles(a, [await id(b)]);
    await expectTiles(b, [await id(a)]);

    // A signaling interruption that preserves our ID must keep the original channel.
    const firstId = await id(a);
    const sameChannel = await a.evaluate(remoteId => {
        const originalChannel = pairDrop.peers.peers[remoteId]._channel;
        return new Promise(resolve => {
            Events.on('peers', () => {
                resolve(originalChannel === pairDrop.peers.peers[remoteId]?._channel && originalChannel.readyState === 'open');
            }, {once: true});
            pairDrop.server._socket.close();
        });
    }, await id(b));
    assert.equal(await id(a), firstId);
    assert.ok(sameChannel);

    // Equal display names still identify separate, selectable peers.
    const c = await openClient();
    await expectTiles(a, [await id(b), await id(c)]);
    await expectTiles(c, [await id(a), await id(b)]);
    for (const page of [b, c]) {
        await page.evaluate(() => Events.fire('self-display-name-changed', 'Shared name'));
    }
    await a.waitForFunction(() => [...document.querySelectorAll('x-peer .name')]
        .filter(name => name.textContent === 'Shared name').length === 2);
    await c.close();
    await expectTiles(a, [await id(b)], true);

    async function reconnect(includeHidden = true) {
        const previous = await Promise.all([a, b].map(id));
        await restartServer();
        await Promise.all([a, b].map((page, i) => page.waitForFunction(
            old => sessionStorage.getItem('peer_id') !== old, previous[i])));
        await expectTiles(a, [await id(b)], includeHidden);
        await expectTiles(b, [await id(a)], includeHidden);
    }
    await reconnect();
    await reconnect();

    const size = 3 * 1024 * 1024;
    const oldB = await id(b);
    await b.evaluate(() => {
        Events.on('files-received', async e => {
            const bytes = await e.detail.files[0].arrayBuffer();
            window.receivedHash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
                .map(byte => byte.toString(16).padStart(2, '0')).join('');
        });
    });
    await a.evaluate(({oldB, size}) => {
        const peer = pairDrop.peers.peers[oldB];
        // Hold the sender at the first real partition acknowledgement.
        const resume = peer._sendNextPartition.bind(peer);
        peer._sendNextPartition = () => {
            peer._sendNextPartition = resume;
            window.resumeTransfer = resume;
        };
        Events.on('files-sent', () => { window.fileAcknowledged = true; });
        const bytes = Uint8Array.from({length: size}, (_, i) => i % 251);
        Events.fire('files-selected', {to: oldB, files: [new File([bytes], 'sample.bin')]});
    }, {oldB, size});
    await b.locator('#accept-request').waitFor({state: 'visible'});
    await reconnect(false);
    await b.locator('#accept-request').click();
    await a.waitForFunction(() => window.resumeTransfer !== undefined);
    await reconnect(false);
    await a.evaluate(() => window.resumeTransfer());
    await a.waitForFunction(() => window.fileAcknowledged);
    await b.waitForFunction(() => window.receivedHash);
    const bytes = Buffer.alloc(size);
    for (let i = 0; i < bytes.length; i++) bytes[i] = i % 251;
    assert.equal(await b.evaluate(() => window.receivedHash), createHash('sha256').update(bytes).digest('hex'));
    await expectTiles(a, [await id(b)], true);
    await expectTiles(b, [await id(a)], true);

    // Keep the old channel until a text already delivered has been acknowledged.
    await b.evaluate(remoteId => {
        const peer = pairDrop.peers.peers[remoteId];
        const send = peer.sendJSON.bind(peer);
        peer.sendJSON = message => {
            if (message.type === 'message-transfer-complete') {
                window.acknowledgeText = () => send(message);
            } else {
                send(message);
            }
        };
        Events.on('text-received', e => { window.receivedText = e.detail.text; }, {once: true});
    }, await id(a));
    await a.evaluate(remoteId => {
        window.textPeer = pairDrop.peers.peers[remoteId];
        Events.fire('send-text', {to: remoteId, text: 'Hello across a restart'});
    }, await id(b));
    await b.waitForFunction(() => window.acknowledgeText !== undefined);
    assert.equal(await b.evaluate(() => window.receivedText), 'Hello across a restart');
    await reconnect(false);
    await b.evaluate(() => window.acknowledgeText());
    await a.waitForFunction(() => window.textPeer._pendingTexts === 0);
    await expectTiles(a, [await id(b)], true);
    await expectTiles(b, [await id(a)], true);

    // One client can receive a new ID while its transfer partner keeps the old ID.
    const senderId = await id(b);
    const recipientId = await id(a);
    await b.evaluate(to => Events.fire('files-selected', {to, files: [new File(['a'], 'offer.txt')]}), recipientId);
    await a.locator('#decline-request').waitFor({state: 'visible'});
    await b.evaluate(() => {
        sessionStorage.removeItem('peer_id_hash');
        pairDrop.server._socket.close();
    });
    await b.waitForFunction(old => sessionStorage.getItem('peer_id') !== old, senderId);
    await a.waitForFunction(old => document.getElementById(old)?.hidden, senderId);
    await a.locator('#decline-request').click();
    await expectTiles(b, [recipientId], true);
    await expectTiles(a, [await id(b)], true);
});
