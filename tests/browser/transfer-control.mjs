export async function holdFirstWriteAcknowledgement(receiver, senderId) {
    await receiver.waitForFunction(id => pairDrop.peers.peers[id]?._channel?.readyState === 'open', senderId);
    await receiver.evaluate(id => {
        const peer = pairDrop.peers.peers[id];
        const sendJSON = peer.sendJSON.bind(peer);
        peer.sendJSON = message => {
            if (message.type !== 'partition-received') return sendJSON(message);
            peer.sendJSON = sendJSON;
            window.resumeTransfer = () => sendJSON(message);
        };
    }, senderId);
}
