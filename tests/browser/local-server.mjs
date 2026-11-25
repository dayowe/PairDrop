import {once} from 'node:events';
import PairDropServer from '../../server/server.js';
import PairDropWsServer from '../../server/ws-server.js';

export async function startLocalServer(t) {
    const buttons = {
        donation_button: {}, twitter_button: {}, mastodon_button: {},
        bluesky_button: {}, custom_button: {}, privacypolicy_button: {}
    };
    const {server} = new PairDropServer({port: 0, localhostOnly: true, buttons});
    t.after(() => server.close());
    new PairDropWsServer(server, {rtcConfig: {iceServers: []}});
    await once(server, 'listening');
    return 'http://127.0.0.1:' + server.address().port;
}
