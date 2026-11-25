import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {createHash, randomBytes} from 'node:crypto';
import {once} from 'node:events';
import {createServer} from 'node:net';
import {test} from 'node:test';
import {setTimeout as delay} from 'node:timers/promises';
import {promisify} from 'node:util';
import {chromium} from 'playwright-core';
import {holdFirstWriteAcknowledgement} from './transfer-control.mjs';

const exec = promisify(execFile);
const root = new URL('../../', import.meta.url);
const fileSize = 12 * 1024 * 1024;

async function docker(args, env = process.env) {
    const {stdout} = await exec('docker', args, {cwd: root, env, timeout: 120_000});
    return stdout.trim();
}

async function freePort() {
    const listener = createServer().listen(0, '127.0.0.1');
    await once(listener, 'listening');
    const {port} = listener.address();
    await new Promise(resolve => listener.close(resolve));
    return port;
}

async function waitForServer(url) {
    for (let attempt = 0; attempt < 100; attempt++) {
        try {
            const response = await fetch(url + '/config', {signal: AbortSignal.timeout(1000)});
            if (response.ok) return;
        } catch {
            // The port can be published before Node starts accepting requests.
        }
        await delay(100);
    }
    throw new Error('PairDrop did not start on the loopback test port.');
}

async function startPausedTransfer(t) {
    const image = process.env.PAIRDROP_TEST_IMAGE || await docker(['build', '--quiet', '.']);
    const port = await freePort();
    const url = 'http://127.0.0.1:' + port;
    const container = await docker(['run', '--detach', '--publish', '127.0.0.1:' + port + ':3000',
        '--env', 'PEER_ID_SECRET', image], {...process.env, PEER_ID_SECRET: randomBytes(32).toString('hex')});
    t.after(() => docker(['rm', '--force', container]));
    await waitForServer(url);

    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    // newPage() gives each client its own browser context and storage.
    const pages = [await browser.newPage(), await browser.newPage()];
    const ids = [];
    for (const page of pages) {
        await page.goto(url);
        await page.waitForFunction(() => sessionStorage.getItem('peer_id'));
        ids.push(await page.evaluate(() => sessionStorage.getItem('peer_id')));
    }
    assert.notEqual(ids[0], ids[1]);
    for (const [i, page] of pages.entries()) {
        const remote = ids[1 - i];
        await page.waitForFunction(id => pairDrop.peers.peers[id]?._channel?.readyState === 'open' &&
            document.getElementById(id), remote);
        await page.evaluate(id => {
            window.originalChannel = pairDrop.peers.peers[id]._channel;
            window.discoveries = 0;
            Events.on('peer-joined', e => { if (e.detail.roomType === 'ip') window.discoveries++; });
            Events.on('peers', e => { if (e.detail.roomType === 'ip' && e.detail.peers.length) window.discoveries++; });
        }, remote);
    }

    const [sender, receiver] = pages;
    await holdFirstWriteAcknowledgement(receiver, ids[0]);
    await receiver.evaluate(() => Events.on('files-received', async e => {
        const bytes = await e.detail.files[0].arrayBuffer();
        window.receivedHash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
            .map(byte => byte.toString(16).padStart(2, '0')).join('');
    }, {once: true}));
    await sender.evaluate(({remote, size}) => {
        Events.on('files-sent', () => { window.acknowledged = true; }, {once: true});
        const bytes = Uint8Array.from({length: size}, (_, i) => i % 251);
        Events.fire('files-selected', {to: remote, files: [new File([bytes], 'transfer.bin')]});
    }, {remote: ids[1], size: fileSize});
    await receiver.locator('#accept-request').click();
    // Later blocks can arrive while the first disk acknowledgement is held.
    await receiver.waitForFunction(id => window.resumeTransfer &&
        pairDrop.peers.peers[id]._digester?._bytesReceived >= RTCFileSender.MAX_UNACKNOWLEDGED, ids[0]);
    return {container, pages, ids};
}

async function finishTransfer([sender, receiver], senderId) {
    assert.ok(await receiver.evaluate(id =>
        pairDrop.peers.peers[id]._digester?._bytesReceived === RTCFileSender.MAX_UNACKNOWLEDGED, senderId),
        'The sender exceeded the write limit while the acknowledgement was held');
    await receiver.evaluate(() => window.resumeTransfer());
    await sender.waitForFunction(() => window.acknowledged || window.originalChannel.readyState !== 'open');
    assert.ok(await sender.evaluate(() => window.acknowledged), 'WebRTC channel closed before the file was acknowledged');
    await receiver.waitForFunction(() => window.receivedHash);
    const expected = Buffer.alloc(fileSize);
    for (let i = 0; i < expected.length; i++) expected[i] = i % 251;
    assert.equal(await receiver.evaluate(() => window.receivedHash), createHash('sha256').update(expected).digest('hex'));
}

test('both sessions keep one peer and finish their transfer across two signaling server restarts', {timeout: 180_000}, async t => {
    const {container, pages, ids} = await startPausedTransfer(t);
    for (let restart = 0; restart < 2; restart++) {
        const before = await Promise.all(pages.map(page => page.evaluate(() => window.discoveries)));
        await docker(['restart', '--time', '0', container]);
        for (const [i, page] of pages.entries()) {
            await page.waitForFunction(count => window.discoveries > count, before[i]);
            const state = await page.evaluate(id => ({
                local: sessionStorage.getItem('peer_id'),
                tiles: [...document.querySelectorAll('x-peer')].map(tile => tile.id),
                originalChannelOpen: pairDrop.peers.peers[id]?._channel === window.originalChannel &&
                    window.originalChannel.readyState === 'open'
            }), ids[1 - i]);
            assert.deepEqual(state, {local: ids[i], tiles: [ids[1 - i]], originalChannelOpen: true});
        }
    }
    await finishTransfer(pages, ids[0]);
});

test('a network quality change preserves the signaling socket and the active transfer', {timeout: 180_000}, async t => {
    const {pages, ids} = await startPausedTransfer(t);
    const [sender] = pages;
    const socket = await sender.evaluateHandle(() => {
        navigator.connection.addEventListener('change', () => { window.qualityChanged = true; }, {once: true});
        return pairDrop.server._socket;
    });
    const cdp = await sender.context().newCDPSession(sender);
    // Change Chromium's reported network quality while leaving the transport online.
    await cdp.send('Network.overrideNetworkState', {
        offline: false, latency: 350, downloadThroughput: 1024 * 1024, uploadThroughput: 1024 * 1024
    });
    await sender.waitForFunction(() => window.qualityChanged, null, {timeout: 5000});
    await finishTransfer(pages, ids[0]);
    assert.ok(await sender.evaluate(original => pairDrop.server._socket === original && original.readyState === WebSocket.OPEN, socket));
});
