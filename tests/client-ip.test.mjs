import assert from 'node:assert/strict';
import {test} from 'node:test';
import {compileTrustedProxies, getDiscoveryAddress} from '../server/client-ip.js';

const proxy = '192.0.2.10';
const visitor = '198.51.100.12';
const edge = '2001:db8:10::1';
const trust = compileTrustedProxies(proxy + ', 2001:db8:10::/64');

function address(remoteAddress, headers = {}, trustProxy) {
    return getDiscoveryAddress({socket: {remoteAddress}, headers}, trustProxy);
}

test('direct discovery uses the socket address without proxy configuration', () => {
    assert.equal(address(visitor), visitor);
    assert.equal(address(visitor, {}, trust), visitor);
    assert.equal(address('::1'), '::1');
    assert.equal(address(undefined), null);
});

test('a configured proxy can forward a client IP across IPv4 and IPv6 hops', () => {
    assert.equal(address(proxy, {'x-forwarded-for': visitor}, trust), visitor);
    assert.equal(address('::ffff:' + proxy, {'x-forwarded-for': edge + ', ' + visitor}, trust), visitor);
    assert.equal(address(proxy, {'x-forwarded-for': visitor + ', ' + edge}, trust), visitor);
    assert.equal(address(edge, {'x-forwarded-for': '2001:db8:20::7'}, trust), '2001:db8:20::7');
});

test('only the verified X-Forwarded-For chain supplies a proxied discovery address', () => {
    const forwarded = {'x-forwarded-for': visitor, 'cf-connecting-ip': '203.0.113.8'};
    assert.equal(address(proxy, forwarded, trust), visitor);
    assert.equal(address(proxy, forwarded), null);
    assert.equal(address('192.0.2.11', forwarded, trust), null);
    assert.equal(address(proxy, {'cf-connecting-ip': visitor}, trust), null);
});

test('a missing, malformed or proxy-only client address has no IP room', () => {
    for (const value of [undefined, '', 'unknown', edge, visitor + ', unknown']) {
        const headers = value === undefined ? {} : {'x-forwarded-for': value};
        assert.equal(address(proxy, headers, trust), null);
    }
});

test('proxy settings accept explicit addresses and ranges and reject invalid entries', () => {
    assert.equal(compileTrustedProxies(undefined), undefined);
    assert.equal(compileTrustedProxies(''), undefined);
    assert.ok(compileTrustedProxies('192.0.2.0/28')('192.0.2.3'));
    for (const invalid of ['true', '1', 'proxy.example', '192.0.2.0/33', proxy + ',']) {
        assert.throws(() => compileTrustedProxies(invalid));
    }
});
