import {isIP} from 'node:net';
import proxyaddr from 'proxy-addr';

export function compileTrustedProxies(setting) {
    if (!setting?.trim()) return undefined;
    const ranges = setting.split(',').map(range => range.trim());
    if (!ranges.every(range => isIP(range.split('/')[0]))) {
        throw new Error('Expected a comma-separated list of proxy IP addresses or CIDRs.');
    }
    return proxyaddr.compile(ranges);
}

export function getDiscoveryAddress(request, trustProxy) {
    const remote = request.socket.remoteAddress;
    if (!isIP(remote)) return null;

    const forwarded = request.headers['x-forwarded-for'];
    if (trustProxy?.(remote)) {
        if (typeof forwarded !== 'string' || !forwarded.trim()) return null;
        // Walk outwards from the socket and stop at the first untrusted hop.
        const client = proxyaddr(request, trustProxy);
        return isIP(client) && !trustProxy(client) ? client : null;
    }

    // An unconfigured intermediary must not put all its visitors in its own IP room.
    if (forwarded !== undefined || request.headers['cf-connecting-ip'] !== undefined ||
        request.headers.forwarded !== undefined || request.headers['x-real-ip'] !== undefined) return null;
    return remote;
}
