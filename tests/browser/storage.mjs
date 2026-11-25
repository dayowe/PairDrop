import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {test} from 'node:test';
import {chromium} from 'playwright-core';
import {startLocalServer} from './local-server.mjs';
import {holdFirstWriteAcknowledgement} from './transfer-control.mjs';

async function openPage(context, origin, diskBacked = true) {
    const page = await context.newPage();
    await page.goto(origin);
    await page.waitForFunction(() => sessionStorage.getItem('peer_id'));
    assert.equal(await page.evaluate(() => FileDigester.init()), diskBacked);
    return page;
}

async function sendFiles(sender, receiver, sizes) {
    const id = await receiver.evaluate(() => sessionStorage.getItem('peer_id'));
    await sender.waitForFunction(id => document.getElementById(id), id);
    await sender.evaluate(({id, sizes}) => {
        const files = sizes.map((size, i) => new File([new Uint8Array(size).fill(i + 1)], 'file-' + i + '.bin'));
        Events.fire('files-selected', {to: id, files});
    }, {id, sizes});
    await receiver.locator('#accept-request').click();
}

// Inspect the real OPFS, including files already flushed and waiting for Save.
async function storedFiles(page) {
    return page.evaluate(async () => {
        const root = await navigator.storage.getDirectory();
        const transfers = await root.getDirectoryHandle('pairdrop-transfers');
        const names = [];
        for await (const dir of transfers.values()) {
            for await (const name of dir.keys()) names.push(name);
        }
        return names.sort();
    });
}

test('another tab cannot reclaim a pending download or an active receive', {timeout: 45_000}, async t => {
    const origin = await startLocalServer(t);
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const context = await browser.newContext({serviceWorkers: 'block'});
    const first = await openPage(context, origin);
    const sender = await openPage(await browser.newContext(), origin);
    await sendFiles(sender, first, [128]);
    await first.waitForFunction(() => pairDrop.receiveFileDialog._currentFiles?.length);
    const owned = await first.evaluate(async () => {
        const root = await navigator.storage.getDirectory();
        await root.getFileHandle('unrelated', {create: true});
        // Hold an unfinished write alongside the completed download.
        window.partial = new FileDigester({size: 2, name: 'partial'}, 2, 0, file => { window.finished = file; });
        partial.unchunk(Uint8Array.of(5).buffer);
        await new Promise(resolve => partial.onPartition(resolve));
        return [pairDrop.receiveFileDialog._currentFiles[0].id, partial._id].sort();
    });
    const second = await openPage(context, origin);
    assert.deepEqual(await storedFiles(second), owned);
    await sendFiles(sender, second, [64]);
    await second.waitForFunction(() => pairDrop.receiveFileDialog._currentFiles?.length);
    // Leave the second download undismissed to exercise abandoned session cleanup.
    await second.close();
    const third = await openPage(context, origin);
    assert.deepEqual(await storedFiles(third), owned);
    const bytes = await first.evaluate(async () => {
        const root = await navigator.storage.getDirectory();
        await root.getFileHandle('unrelated');
        partial.unchunk(Uint8Array.of(6).buffer);
        const file = pairDrop.receiveFileDialog._currentFiles[0];
        return [...new Uint8Array(await file.arrayBuffer())];
    });
    assert.deepEqual(bytes, Array(128).fill(1));
    await first.waitForFunction(() => !!window.finished);
    assert.deepEqual(await first.evaluate(async () => [...new Uint8Array(await finished.arrayBuffer())]), [5, 6]);
    await first.evaluate(() => {
        const id = pairDrop.receiveFileDialog._currentFiles[0].id;
        FileDigester._worker.addEventListener('message', ({data}) => {
            if (data.type === 'file-deleted' && data.id === id) window.dismissedFileDeleted = true;
        });
    });
    await first.locator('#receive-file-dialog [close]').last().click();
    await first.waitForFunction(() => window.dismissedFileDeleted, null, {timeout: 5000});
    assert.deepEqual(await storedFiles(third), [await first.evaluate(() => finished.id)]);
});

test('quota exhaustion fails both ends, removes the partial batch and permits a retry', {timeout: 45_000}, async t => {
    const origin = await startLocalServer(t);
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const sender = await openPage(await browser.newContext({serviceWorkers: 'block'}), origin);
    const receiver = await openPage(await browser.newContext({serviceWorkers: 'block'}), origin);
    for (const page of [sender, receiver]) {
        await page.evaluate(() => {
            window.messages = [];
            Events.on('notify-user', e => messages.push(e.detail));
            Events.on('files-sent', () => { window.sent = true; });
            Events.on('files-received', () => { window.received = true; });
        });
    }
    const cdp = await receiver.context().newCDPSession(receiver);
    await receiver.evaluate(() => FileDigester._worker.addEventListener('message', ({data}) => {
        if (data.type === 'file-deleted') window.discardedFile = true;
    }));
    const senderId = await sender.evaluate(() => sessionStorage.getItem('peer_id'));
    await holdFirstWriteAcknowledgement(receiver, senderId);
    await sendFiles(sender, receiver, [128, 16 * 1024 * 1024]);
    await receiver.waitForFunction(() => !!window.resumeTransfer);
    // The offer fit when accepted. Take space away after the first partition was written.
    const {usage} = await receiver.evaluate(() => navigator.storage.estimate());
    await cdp.send('Storage.overrideQuotaForOrigin', {origin, quotaSize: usage + 512 * 1024});
    await receiver.evaluate(() => window.resumeTransfer());
    await receiver.waitForFunction(() => messages.includes(Localization.getTranslation('notifications.storage-full')), null, {timeout: 10000});
    await sender.waitForFunction(() => messages.includes(Localization.getTranslation('notifications.peer-storage-full')), null, {timeout: 10000});
    assert.equal(await sender.evaluate(() => !!window.sent), false);
    assert.equal(await receiver.evaluate(() => !!window.received), false);
    await receiver.waitForFunction(() => window.discardedFile);
    assert.deepEqual(await storedFiles(receiver), []);
    // Chromium can mask the actual quota in estimate(). Exercise an explicit low estimate too.
    await receiver.evaluate(() => Object.defineProperty(navigator.storage, 'estimate', {
        configurable: true, value: async () => ({quota: 1024, usage: 512})
    }));
    await sender.evaluate(() => {
        window.messages = [];
        Events.on('file-transfer-accepted', () => { window.acceptedAgain = true; }, {once: true});
    });
    await sendFiles(sender, receiver, [8 * 1024 * 1024]);
    await sender.waitForFunction(() => messages.includes(Localization.getTranslation('notifications.peer-storage-full')));
    assert.equal(await sender.evaluate(() => !!window.acceptedAgain), false);
    await receiver.evaluate(() => { delete navigator.storage.estimate; });
    await cdp.send('Storage.overrideQuotaForOrigin', {origin});
    await sendFiles(sender, receiver, [256]);
    await sender.waitForFunction(() => window.sent);
    await receiver.waitForFunction(() => window.received);
    assert.equal(await receiver.evaluate(() => pairDrop.receiveFileDialog._currentFiles[0].size), 256);
});

test('a receiver without disk support can receive an empty file and a nonempty file', {timeout: 30_000}, async t => {
    const origin = await startLocalServer(t);
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const context = await browser.newContext({serviceWorkers: 'block'});
    await context.addInitScript(() => Object.defineProperty(navigator, 'locks', {value: undefined}));
    const receiver = await openPage(context, origin, false);
    const sender = await openPage(await browser.newContext(), origin);
    await sendFiles(sender, receiver, [0, 32]);
    await receiver.waitForFunction(() => pairDrop.receiveFileDialog._currentFiles?.length === 2);
    assert.deepEqual(await receiver.evaluate(async () => {
        const files = pairDrop.receiveFileDialog._currentFiles;
        return Promise.all(files.map(async file => [...new Uint8Array(await file.arrayBuffer())]));
    }), [[], Array(32).fill(2)]);
});

for (const transport of ['WebSocket', 'WebRTC without buffer thresholds']) {
    test(transport + ' still sends files with the partition handshake', {timeout: 30_000}, async t => {
        const origin = await startLocalServer(t, {wsFallback: true});
        const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
        t.after(() => browser.close());
        const context = await browser.newContext({serviceWorkers: 'block'});
        await context.addInitScript(transport => {
            if (transport === 'WebSocket') {
                for (const api of ['RTCPeerConnection', 'webkitRTCPeerConnection', 'mozRTCPeerConnection']) {
                    delete window[api];
                }
            } else {
                delete RTCDataChannel.prototype.bufferedAmountLowThreshold;
            }
        }, transport);
        const sender = await openPage(context, origin);
        const receiver = await openPage(await browser.newContext(), origin);
        const id = await sender.evaluate(() => sessionStorage.getItem('peer_id'));
        await receiver.waitForFunction(id => document.getElementById(id), id);
        await receiver.evaluate(id => {
            const peer = pairDrop.peers.peers[id];
            const sendJSON = peer.sendJSON.bind(peer);
            window.writes = 0;
            peer.sendJSON = message => {
                if (message.type === 'partition-received') window.writes++;
                sendJSON(message);
            };
        }, id);
        const size = 3 * 1024 * 1024;
        await sender.evaluate(() => Events.on('files-sent', () => { window.sent = true; }, {once: true}));
        await sendFiles(sender, receiver, [size]);
        await sender.waitForFunction(() => window.sent);
        const result = await receiver.evaluate(async () => {
            const [file] = pairDrop.receiveFileDialog._currentFiles;
            const bytes = new Uint8Array(await file.arrayBuffer());
            return {writes, size: bytes.length, intact: bytes.every(byte => byte === 1)};
        });
        assert.deepEqual(result, {writes: 3, size, intact: true});
    });
}

test('a download over the iOS memory limit keeps its filename and contents', {timeout: 120_000}, async t => {
    const origin = await startLocalServer(t);
    const browser = await chromium.launch({channel: process.env.BROWSER_CHANNEL || 'chrome'});
    t.after(() => browser.close());
    const sender = await openPage(await browser.newContext(), origin);
    const receiver = await openPage(await browser.newContext(), origin);
    const id = await receiver.evaluate(() => {
        window.iOS = true; // Exercise the size guard in Chromium; device testing covers WebKit.
        return sessionStorage.getItem('peer_id');
    });
    await sender.waitForFunction(id => pairDrop.peers.peers[id]?._channel?.readyState === 'open', id);
    const sizeMiB = 251;
    await sender.evaluate(({id, sizeMiB}) => {
        const block = new Blob([new Uint8Array(1024 * 1024).fill(42)]);
        const file = new File(Array(sizeMiB).fill(block), 'large.bin');
        Events.fire('files-selected', {to: id, files: [file]});
    }, {id, sizeMiB});
    const downloadPromise = receiver.waitForEvent('download', {timeout: 90000});
    await receiver.locator('#accept-request').click();
    const download = await downloadPromise;
    assert.equal(download.suggestedFilename(), 'large.bin');
    const hash = createHash('sha256');
    for await (const bytes of await download.createReadStream()) hash.update(bytes);
    const expected = createHash('sha256');
    const block = Buffer.alloc(1024 * 1024, 42);
    for (let i = 0; i < sizeMiB; i++) expected.update(block);
    assert.equal(hash.digest('hex'), expected.digest('hex'));
});
