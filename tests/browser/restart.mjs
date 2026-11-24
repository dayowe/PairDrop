import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {createHash, randomBytes} from 'node:crypto';
import {once} from 'node:events';
import {createServer} from 'node:net';
import {test} from 'node:test';
import {setTimeout as delay} from 'node:timers/promises';
import {promisify} from 'node:util';
import {chromium} from 'playwright-core';

const exec = promisify(execFile);
const root = new URL('../../', import.meta.url);
const fileSize = 3 * 1024 * 1024;

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

test('both sessions keep one peer and finish their transfer across two signaling server restarts', {timeout: 180_000}, async t => {
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
    await receiver.evaluate(() => Events.on('files-received', async e => {
        const bytes = await e.detail.files[0].arrayBuffer();
        window.receivedHash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
            .map(byte => byte.toString(16).padStart(2, '0')).join('');
    }, {once: true}));
    await sender.evaluate(({remote, size}) => {
        // Pause at a real partition acknowledgement so both restarts occur mid-transfer.
        const peer = pairDrop.peers.peers[remote];
        const nextPartition = peer._sendNextPartition.bind(peer);
        peer._sendNextPartition = () => {
            peer._sendNextPartition = nextPartition;
            window.resumeTransfer = nextPartition;
        };
        Events.on('files-sent', () => { window.acknowledged = true; }, {once: true});
        const bytes = Uint8Array.from({length: size}, (_, i) => i % 251);
        Events.fire('files-selected', {to: remote, files: [new File([bytes], 'restart.bin')]});
    }, {remote: ids[1], size: fileSize});
    await receiver.locator('#accept-request').click();
    await sender.waitForFunction(() => !!window.resumeTransfer);

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
    await sender.evaluate(() => window.resumeTransfer());
    await sender.waitForFunction(() => window.acknowledged);
    await receiver.waitForFunction(() => window.receivedHash);
    const expected = Buffer.alloc(fileSize);
    for (let i = 0; i < expected.length; i++) expected[i] = i % 251;
    assert.equal(await receiver.evaluate(() => window.receivedHash), createHash('sha256').update(expected).digest('hex'));
});
