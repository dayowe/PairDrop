// A dedicated worker owns one directory for the lifetime of its page.
let session;
const files = new Map();
let directory;
let messageQueue = Promise.resolve();

self.addEventListener('message', ({data}) => {
    if (data.type === 'check-support') {
        session = data.session;
        checkSupport();
        return;
    }
    // Writes, finalization and cancellation must run in message order.
    messageQueue = messageQueue.then(() => processMessage(data)).catch(async error => {
        await deleteFile(data.id).catch(console.error);
        self.postMessage({type: 'error', id: data.id, name: error.name, error: error.message});
    });
});

async function sessionDirectory() {
    if (!directory) {
        const root = await navigator.storage.getDirectory();
        const transfers = await root.getDirectoryHandle('pairdrop-transfers', {create: true});
        directory = await transfers.getDirectoryHandle(session, {create: true});
    }
    return directory;
}

async function removeAbandonedSessions() {
    const root = await navigator.storage.getDirectory();
    let transfers;
    try {
        transfers = await root.getDirectoryHandle('pairdrop-transfers');
    } catch (error) {
        if (error.name === 'NotFoundError') return;
        throw error;
    }
    for await (const [name, entry] of transfers.entries()) {
        if (name === session || entry.kind !== 'directory' || !name.startsWith('tab-')) continue;
        await navigator.locks.request('pairdrop-transfers/' + name, {ifAvailable: true}, async lock => {
            if (lock) await transfers.removeEntry(name, {recursive: true});
        });
    }
}

async function checkSupport() {
    try {
        if (!self.FileSystemFileHandle?.prototype.createSyncAccessHandle || !navigator.locks) {
            self.postMessage({type: 'support', supported: false});
            return;
        }
        await removeAbandonedSessions().catch(console.error);
        try {
            const dir = await sessionDirectory();
            const probe = await dir.getFileHandle('probe', {create: true});
            (await probe.createSyncAccessHandle()).close();
            await dir.removeEntry('probe');
        } catch (error) {
            // A full disk is a transfer error, not a reason to receive a large file in RAM.
            if (error.name !== 'QuotaExceededError') throw error;
        }
        self.postMessage({type: 'support', supported: true});
    } catch (error) {
        self.postMessage({type: 'support', supported: false});
    }
}

async function processMessage({type, id, chunk, offset}) {
    if (type === 'init-file') {
        const dir = await sessionDirectory();
        const handle = await dir.getFileHandle(id, {create: true});
        const file = {handle, access: null};
        files.set(id, file);
        file.access = await handle.createSyncAccessHandle();
        return;
    }
    const file = files.get(id);
    // Chunks already in flight after an error or cancellation cannot reopen a file.
    if (!file) return;
    switch (type) {
        case 'chunk': {
            const bytes = new Uint8Array(chunk instanceof Blob ? await chunk.arrayBuffer() : chunk);
            let written = 0;
            while (written < bytes.byteLength) {
                const count = file.access.write(bytes.subarray(written), {at: offset + written});
                if (!count) throw new DOMException('No space to finish the write', 'QuotaExceededError');
                written += count;
            }
            break;
        }
        case 'partition':
            self.postMessage({type: 'partition-written', id});
            break;
        case 'get-file':
            file.access.flush();
            file.access.close();
            file.access = null;
            self.postMessage({type: 'file', id, file: await file.handle.getFile()});
            break;
        case 'delete-file':
            await deleteFile(id);
            self.postMessage({type: 'file-deleted', id});
            break;
    }
}

async function deleteFile(id) {
    const file = files.get(id);
    if (!file) return;
    files.delete(id);
    try {
        file.access?.close();
    } finally {
        await directory.removeEntry(id);
    }
}
