import PairDropServer from '../../server/server.js';
import PairDropWsServer from '../../server/ws-server.js';

const config = {
    port: Number(process.argv[2]),
    localhostOnly: true,
    buttons: {},
    rtcConfig: {iceServers: []},
};
const {server} = new PairDropServer(config);
new PairDropWsServer(server, config);
server.on('listening', () => process.send(server.address().port));
process.on('disconnect', () => process.exit());
