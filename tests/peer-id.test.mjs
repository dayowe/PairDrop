import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {randomBytes, randomUUID} from 'node:crypto';
import {test} from 'node:test';

const root = new URL('../', import.meta.url);
const peerId = randomUUID();

function runWithSecret(secret, args) {
    const env = {...process.env};
    delete env.PEER_ID_SECRET;
    if (secret !== undefined) env.PEER_ID_SECRET = secret;
    return spawnSync(process.execPath, args, {cwd: root, env, encoding: 'utf8', timeout: 5000});
}

function proofFromFreshProcess(secret) {
    const result = runWithSecret(secret, ['--input-type=module', '-e',
        "import {hasher} from './server/helper.js'; console.log(hasher.hashCodeSalted(process.argv[1]));", peerId]);
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
}

test('a stable secret preserves the peer proof across processes and rotation replaces it', () => {
    const secret = randomBytes(32).toString('hex');
    const proof = proofFromFreshProcess(secret);
    assert.match(proof, /^[0-9a-f]{128}$/);
    assert.equal(proofFromFreshProcess(secret), proof);
    assert.notEqual(proofFromFreshProcess(randomBytes(32).toString('hex')), proof);
});

test('without a configured secret each process issues its own peer proof', () => {
    assert.notEqual(proofFromFreshProcess(), proofFromFreshProcess());
});

test('an explicitly empty or malformed secret fails startup without echoing the value', () => {
    for (const value of ['', 'invalid-secret-for-test']) {
        const result = runWithSecret(value, ['server/index.js', '--localhost-only']);
        assert.equal(result.status, 1);
        assert.match(result.stderr, /PEER_ID_SECRET must contain 64 hexadecimal characters/);
        if (value) assert.ok(!(result.stdout + result.stderr).includes(value));
    }
});
