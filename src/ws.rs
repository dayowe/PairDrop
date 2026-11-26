//! Signaling WebSocket, ported from upstream's `server/ws-server.js`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha512};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::config::Config;
use crate::ip;
use crate::peer::{Peer, PeerName};
use crate::random;

/// Upstream pings every second and drops a peer that has not ponged for five.
const PING_INTERVAL: Duration = Duration::from_secs(1);
const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// Transfer traffic that normally rides the WebRTC data channel. Relayed only under WS_FALLBACK.
const RELAYED_ON_FALLBACK: [&str; 11] = [
    "request",
    "header",
    "partition",
    "partition-received",
    "progress",
    "files-transfer-response",
    "file-transfer-complete",
    "message-transfer-complete",
    "text",
    "display-name-changed",
    "ws-chunk",
];

#[derive(Default)]
struct HubState {
    peers: HashMap<Uuid, Peer>,
    rooms: HashMap<&'static str, HashMap<String, Vec<Uuid>>>,
    /// Pair key -> the secret it unlocks and the peer offering it.
    pair_keys: HashMap<String, PairOffer>,
}

const PAIR_KEY_LIFETIME: Duration = Duration::from_secs(600);

struct PairOffer {
    expires: Instant,
    room_secret: String,
    creator: Uuid,
}

pub struct Hub {
    pub conf: Arc<Config>,
    secret: [u8; 32],
    state: Mutex<HubState>,
}

impl Hub {
    pub fn new(conf: Arc<Config>, secret: [u8; 32]) -> Self {
        Hub {
            conf,
            secret,
            state: Mutex::new(HubState::default()),
        }
    }

    /// Lets a reconnecting client keep its peer id without letting anyone claim an arbitrary one.
    fn peer_id_hash(&self, peer_id: &Uuid) -> String {
        let mut hasher = Sha512::new();
        hasher.update(self.secret);
        hasher.update(peer_id.as_bytes());
        hex::encode(hasher.finalize())
    }
}

impl HubState {
    fn join_ip_room(&mut self, peer_id: &Uuid) {
        let Some(room_id) = self.peers.get(peer_id).and_then(|p| p.ip.clone()) else {
            self.send(
                peer_id,
                &json!({"type": "peers", "roomType": "ip", "roomId": null, "peers": []}),
            );
            return;
        };
        self.join_room(peer_id, "ip", &room_id);
    }

    fn on_room_secrets(&mut self, peer_id: &Uuid, message: &serde_json::Value) {
        let Some(secrets) = message.get("roomSecrets").and_then(|s| s.as_array()) else {
            return;
        };

        let mut seen = std::collections::HashSet::new();
        let secrets = secrets
            .iter()
            .filter_map(|s| s.as_str())
            .filter(|s| s.is_ascii() && (64..=256).contains(&s.len()))
            .filter(|secret| seen.insert(*secret));

        for secret in secrets {
            self.join_secret_room(peer_id, secret);
        }
    }

    fn on_room_secrets_deleted(&mut self, peer_id: &Uuid, message: &serde_json::Value) {
        let Some(secrets) = message.get("roomSecrets").and_then(|s| s.as_array()) else {
            return;
        };

        for secret in secrets.iter().filter_map(|s| s.as_str()) {
            if self.is_member(peer_id, "secret", secret) {
                self.delete_secret_room(secret);
            }
        }
    }

    fn on_pair_device_initiate(&mut self, peer_id: &Uuid) {
        if let Some(previous) = self.peers.get(peer_id).and_then(|p| p.pair_key.clone()) {
            self.remove_pair_key(&previous);
        }
        let room_secret = random::room_secret();
        let pair_key = self.create_pair_key(peer_id, &room_secret);
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.pair_key = Some(pair_key.clone());
            peer.send(&json!({
                "type": "pair-device-initiated",
                "roomSecret": room_secret,
                "pairKey": pair_key,
            }));
        }
    }

    fn on_pair_device_join(&mut self, peer_id: &Uuid, message: &serde_json::Value) {
        let rate_limited = self
            .peers
            .get_mut(peer_id)
            .is_some_and(|peer| peer.rate_limit_reached());
        if rate_limited {
            self.send(peer_id, &json!({ "type": "join-key-rate-limit" }));
            return;
        }

        let key = match &message["pairKey"] {
            serde_json::Value::String(key) => key.clone(),
            serde_json::Value::Number(key) => key.to_string(),
            _ => String::new(),
        };
        let Some((room_secret, creator)) = self
            .pair_keys
            .get(&key)
            .filter(|offer| offer.creator != *peer_id && offer.expires > Instant::now())
            .filter(|offer| {
                self.peers
                    .get(&offer.creator)
                    .is_some_and(|p| !p.tx.is_closed() && p.pair_key.as_ref() == Some(&key))
            })
            .map(|offer| (offer.room_secret.clone(), offer.creator))
        else {
            self.send(peer_id, &json!({ "type": "pair-device-join-key-invalid" }));
            return;
        };

        self.remove_pair_key(&key);

        self.send(
            peer_id,
            &json!({ "type": "pair-device-joined", "roomSecret": room_secret, "peerId": creator }),
        );
        self.send(
            &creator,
            &json!({ "type": "pair-device-joined", "roomSecret": room_secret, "peerId": peer_id }),
        );

        self.join_secret_room(&creator, &room_secret);
        self.join_secret_room(peer_id, &room_secret);

        // A device that was itself offering a key stops offering it once it pairs.
        if let Some(own_key) = self.peers.get(peer_id).and_then(|p| p.pair_key.clone()) {
            self.remove_pair_key(&own_key);
        }
    }

    fn on_pair_device_cancel(&mut self, peer_id: &Uuid) {
        let Some(pair_key) = self.peers.get(peer_id).and_then(|p| p.pair_key.clone()) else {
            return;
        };

        self.remove_pair_key(&pair_key);
        self.send(
            peer_id,
            &json!({ "type": "pair-device-canceled", "pairKey": pair_key }),
        );
    }

    fn on_regenerate_room_secret(&mut self, peer_id: &Uuid, message: &serde_json::Value) {
        let Some(old_secret) = message.get("roomSecret").and_then(|s| s.as_str()) else {
            return;
        };
        if !self.is_member(peer_id, "secret", old_secret) {
            return;
        }
        let new_secret = random::room_secret();

        let Some(members) = self
            .rooms
            .get_mut("secret")
            .and_then(|rooms| rooms.remove(old_secret))
        else {
            return;
        };

        let notice = json!({
            "type": "room-secret-regenerated",
            "oldRoomSecret": old_secret,
            "newRoomSecret": new_secret,
        });

        for member_id in members {
            if let Some(peer) = self.peers.get_mut(&member_id) {
                peer.send(&notice);
                peer.remove_room_secret(old_secret);
            }
        }
    }

    fn on_create_public_room(&mut self, peer_id: &Uuid) {
        let mut room_id = random::public_room_id();
        while self.members("public-id", &room_id).is_some() {
            room_id = random::public_room_id();
        }

        self.send(
            peer_id,
            &json!({ "type": "public-room-created", "roomId": room_id }),
        );
        self.join_public_room(peer_id, &room_id);
    }

    fn on_join_public_room(&mut self, peer_id: &Uuid, message: &serde_json::Value) {
        let rate_limited = self
            .peers
            .get_mut(peer_id)
            .is_some_and(|peer| peer.rate_limit_reached());
        if rate_limited {
            self.send(peer_id, &json!({ "type": "join-key-rate-limit" }));
            return;
        }

        let Some(room_id) = message
            .get("publicRoomId")
            .and_then(|id| id.as_str())
            .map(str::to_owned)
        else {
            return;
        };
        let create_if_invalid = message
            .get("createIfInvalid")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if room_id.len() != 5
            || !room_id.bytes().all(|c| c.is_ascii_lowercase())
            || (self.members("public-id", &room_id).is_none() && !create_if_invalid)
        {
            self.send(
                peer_id,
                &json!({ "type": "public-room-id-invalid", "publicRoomId": room_id }),
            );
            return;
        }

        self.join_public_room(peer_id, &room_id);
    }

    fn on_leave_public_room(&mut self, peer_id: &Uuid) {
        self.leave_public_room(peer_id, true);
        self.send(peer_id, &json!({ "type": "public-room-left" }));
    }

    /// Everything a peer is part of falls away together when the socket closes.
    fn disconnect(&mut self, peer_id: &Uuid, disconnect: bool) {
        if let Some(pair_key) = self.peers.get(peer_id).and_then(|p| p.pair_key.clone()) {
            self.remove_pair_key(&pair_key);
        }

        if let Some(ip) = self.peers.get(peer_id).and_then(|p| p.ip.clone()) {
            self.leave_room(peer_id, "ip", &ip, disconnect);
        }

        for secret in self
            .peers
            .get(peer_id)
            .map(|p| p.room_secrets.clone())
            .unwrap_or_default()
        {
            self.leave_secret_room(peer_id, &secret, disconnect);
        }

        self.leave_public_room(peer_id, disconnect);
        self.peers.remove(peer_id);
    }

    /// `_signalAndRelay`: hand the message to `message.to` within the sender's room, stamped with
    /// the sender's identity. Only peers that share a room can reach each other.
    fn signal_and_relay(&self, sender_id: &Uuid, mut message: serde_json::Value) {
        let Some(sender) = self.peers.get(sender_id) else {
            return;
        };

        let room_type = message["roomType"].as_str().unwrap_or_default();
        let room_id = match room_type {
            "ip" => sender.ip.as_deref(),
            "secret" | "public-id" => message["roomId"].as_str(),
            _ => None,
        };
        let Some(room_id) = room_id else {
            return;
        };

        let Some(recipient_id) = message
            .get("to")
            .and_then(|to| to.as_str())
            .and_then(|to| Uuid::parse_str(to).ok())
        else {
            return;
        };

        if !self
            .members(room_type, room_id)
            .is_some_and(|m| m.contains(sender_id) && m.contains(&recipient_id))
        {
            return;
        }

        let sender_info = json!({ "id": sender.id, "rtcSupported": sender.rtc_supported });
        if let Some(object) = message.as_object_mut() {
            object.remove("to");
            object.insert("sender".into(), sender_info);
        }

        if let Some(recipient) = self.peers.get(&recipient_id) {
            recipient.send(&message);
        }
    }
}

impl HubState {
    fn members(&self, kind: &str, id: &str) -> Option<&[Uuid]> {
        self.rooms.get(kind)?.get(id).map(Vec::as_slice)
    }

    fn is_member(&self, peer_id: &Uuid, kind: &str, id: &str) -> bool {
        self.members(kind, id)
            .is_some_and(|members| members.contains(peer_id))
    }

    fn owns_socket(&self, id: &Uuid, tx: &mpsc::UnboundedSender<String>) -> bool {
        self.peers
            .get(id)
            .is_some_and(|peer| peer.tx.same_channel(tx))
    }

    fn expire_offer(&mut self, id: &Uuid) {
        let offer = self
            .peers
            .get(id)
            .and_then(|peer| peer.pair_key.as_ref())
            .and_then(|key| self.pair_keys.get(key));
        if offer.is_some_and(|o| o.expires <= Instant::now()) {
            self.on_pair_device_cancel(id);
        }
    }

    fn send(&self, peer_id: &Uuid, message: &serde_json::Value) {
        if let Some(peer) = self.peers.get(peer_id) {
            peer.send(message);
        }
    }

    fn join_secret_room(&mut self, peer_id: &Uuid, room_secret: &str) {
        self.join_room(peer_id, "secret", room_secret);
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.add_room_secret(room_secret);
        }
    }

    fn leave_secret_room(&mut self, peer_id: &Uuid, room_secret: &str, disconnect: bool) {
        self.leave_room(peer_id, "secret", room_secret, disconnect);
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.remove_room_secret(room_secret);
        }
    }

    fn delete_secret_room(&mut self, room_secret: &str) {
        let Some(members) = self.members("secret", room_secret).map(<[_]>::to_vec) else {
            return;
        };

        let notice = json!({ "type": "secret-room-deleted", "roomSecret": room_secret });
        for member_id in members {
            self.leave_secret_room(&member_id, room_secret, true);
            self.send(&member_id, &notice);
        }
    }

    /// A peer is only ever in one public room.
    fn join_public_room(&mut self, peer_id: &Uuid, room_id: &str) {
        self.leave_public_room(peer_id, false);
        self.join_room(peer_id, "public-id", room_id);
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.public_room_id = Some(room_id.to_string());
        }
    }

    fn leave_public_room(&mut self, peer_id: &Uuid, disconnect: bool) {
        let Some(room_id) = self
            .peers
            .get(peer_id)
            .and_then(|p| p.public_room_id.clone())
        else {
            return;
        };

        self.leave_room(peer_id, "public-id", &room_id, disconnect);
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.public_room_id = None;
        }
    }

    fn create_pair_key(&mut self, creator: &Uuid, room_secret: &str) -> String {
        let mut key = random::pair_key();
        while self.pair_keys.contains_key(&key) {
            key = random::pair_key();
        }

        self.pair_keys.insert(
            key.clone(),
            PairOffer {
                room_secret: room_secret.to_string(),
                creator: *creator,
                expires: Instant::now() + PAIR_KEY_LIFETIME,
            },
        );
        key
    }

    fn remove_pair_key(&mut self, pair_key: &str) {
        if let Some(offer) = self.pair_keys.remove(pair_key) {
            if let Some(creator) = self.peers.get_mut(&offer.creator) {
                creator.pair_key = None;
            }
        }
    }

    fn join_room(&mut self, peer_id: &Uuid, room_type: &'static str, room_id: &str) {
        // Re-joining leaves first, so other peers never see `peer-left` after `peer-joined`.
        if self.is_member(peer_id, room_type, room_id) {
            self.leave_room(peer_id, room_type, room_id, false);
        }

        // The room has to exist before notifying, so the first peer in still gets `peers: []`.
        self.rooms
            .entry(room_type)
            .or_default()
            .entry(room_id.to_string())
            .or_default();
        self.notify_peers(peer_id, room_type, room_id);
        self.rooms
            .get_mut(room_type)
            .and_then(|r| r.get_mut(room_id))
            .expect("room was just created")
            .push(*peer_id);
    }

    fn leave_room(
        &mut self,
        peer_id: &Uuid,
        room_type: &'static str,
        room_id: &str,
        disconnect: bool,
    ) {
        let Some(rooms) = self.rooms.get_mut(room_type) else {
            return;
        };
        let Some(members) = rooms.get_mut(room_id) else {
            return;
        };
        let Some(index) = members.iter().position(|id| id == peer_id) else {
            return;
        };
        members.remove(index);

        if members.is_empty() {
            rooms.remove(room_id);
            return;
        }

        let message = json!({
            "type": "peer-left",
            "peerId": peer_id,
            "roomType": room_type,
            "roomId": room_id,
            "disconnect": disconnect,
        });

        for other_id in members.iter() {
            if let Some(other) = self.peers.get(other_id) {
                other.send(&message);
            }
        }
    }

    /// Tells the room about the newcomer, then tells the newcomer about the room.
    fn notify_peers(&self, peer_id: &Uuid, room_type: &'static str, room_id: &str) {
        let Some(members) = self.members(room_type, room_id) else {
            return;
        };
        let Some(peer) = self.peers.get(peer_id) else {
            return;
        };

        let joined = json!({
            "type": "peer-joined",
            "peer": peer.info(),
            "roomType": room_type,
            "roomId": room_id,
        });

        let mut others = Vec::new();
        for other_id in members.iter().filter(|id| *id != peer_id) {
            if let Some(other) = self.peers.get(other_id) {
                other.send(&joined);
                others.push(other.info());
            }
        }

        peer.send(&json!({
            "type": "peers",
            "peers": others,
            "roomType": room_type,
            "roomId": room_id,
        }));
    }
}

#[derive(Debug, Deserialize)]
pub struct WsQuery {
    peer_id: Option<String>,
    peer_id_hash: Option<String>,
    webrtc_supported: Option<String>,
}

pub async fn handler(
    State(hub): State<Arc<Hub>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(query): Query<WsQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let ip = ip::resolve(&headers, addr.ip(), &hub.conf);
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // Reuse the client's peer id only when it can prove the server issued it.
    let id = query
        .peer_id
        .as_deref()
        .and_then(|id| Uuid::parse_str(id).ok())
        .filter(|id| query.peer_id_hash.as_deref() == Some(hub.peer_id_hash(id).as_str()))
        .unwrap_or_else(Uuid::new_v4);

    let rtc_supported = query.webrtc_supported.as_deref() == Some("true");

    upgrade.on_upgrade(move |socket| connection(hub, socket, id, ip, rtc_supported, user_agent))
}

async fn connection(
    hub: Arc<Hub>,
    socket: WebSocket,
    id: Uuid,
    ip: Option<String>,
    rtc_supported: bool,
    user_agent: Option<String>,
) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    let name = PeerName::new(&id, user_agent.as_deref());
    tracing::debug!(%id, ?ip, display_name = %name.display_name, "peer connected");

    let peer = Peer::new(id, ip, rtc_supported, name.clone(), tx.clone());
    // One task owns the sink; everything else queues through `tx`.
    let writer = tokio::spawn(async move {
        while let Some(text) = rx.recv().await {
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    peer.send(&json!({
        "type": "ws-config",
        "wsConfig": {
            "rtcConfig": hub.conf.rtc_config,
            "wsFallback": hub.conf.ws_fallback,
        }
    }));

    peer.send(&json!({
        "type": "display-name",
        "displayName": name.display_name,
        "deviceName": name.device_name,
        "peerId": id,
        "peerIdHash": hub.peer_id_hash(&id),
    }));

    {
        let mut state = hub.state.lock().unwrap();
        if state.peers.contains_key(&id) {
            state.disconnect(&id, false);
        }
        state.peers.insert(id, peer);
    }

    let mut ping = tokio::time::interval(PING_INTERVAL);
    let mut last_pong = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = ping.tick() => {
                let mut state = hub.state.lock().unwrap();
                if !state.owns_socket(&id, &tx) { break; }
                state.expire_offer(&id);
                if last_pong.elapsed() > PING_TIMEOUT {
                    tracing::debug!(%id, "peer unresponsive, disconnecting");
                    break;
                }
                if tx.send(json!({ "type": "ping" }).to_string()).is_err() {
                    break;
                }
            }
            incoming = stream.next() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) => {
                        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                            tracing::warn!("WS: received JSON is malformed");
                            continue;
                        };
                        let mut state = hub.state.lock().unwrap();
                        if !state.owns_socket(&id, &tx) { break; }
                        match value.get("type").and_then(|t| t.as_str()) {
                            Some("pong") => last_pong = tokio::time::Instant::now(),
                            Some("disconnect") => break,
                            Some("join-ip-room") => state.join_ip_room(&id),
                            Some("room-secrets") => state.on_room_secrets(&id, &value),
                            Some("room-secrets-deleted") => state.on_room_secrets_deleted(&id, &value),
                            Some("pair-device-initiate") => state.on_pair_device_initiate(&id),
                            Some("pair-device-join") => state.on_pair_device_join(&id, &value),
                            Some("pair-device-cancel") => state.on_pair_device_cancel(&id),
                            Some("regenerate-room-secret") => state.on_regenerate_room_secret(&id, &value),
                            Some("create-public-room") => state.on_create_public_room(&id),
                            Some("join-public-room") => state.on_join_public_room(&id, &value),
                            Some("leave-public-room") => state.on_leave_public_room(&id),
                            Some("signal") => state.signal_and_relay(&id, value),
                            // Everything else is peer-to-peer payload that only travels through
                            // the server when the WebSocket fallback is switched on.
                            Some(kind) if RELAYED_ON_FALLBACK.contains(&kind) => {
                                if hub.conf.ws_fallback {
                                    state.signal_and_relay(&id, value);
                                } else {
                                    tracing::debug!(kind, "websocket fallback is not activated on this instance");
                                }
                            }
                            Some(other) => tracing::trace!(%id, kind = other, "unhandled message"),
                            None => tracing::warn!("WS: message without a type"),
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }

    {
        let mut state = hub.state.lock().unwrap();
        if state.owns_socket(&id, &tx) {
            state.disconnect(&id, true);
        }
    }
    drop(tx);
    writer.abort();
    tracing::debug!(%id, "peer disconnected");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(state: &mut HubState) -> (Uuid, mpsc::UnboundedReceiver<String>) {
        let id = Uuid::new_v4();
        let (tx, rx) = mpsc::unbounded_channel();
        state
            .peers
            .insert(id, Peer::new(id, None, true, PeerName::new(&id, None), tx));
        (id, rx)
    }

    #[test]
    fn an_invitation_creates_shared_membership_only_when_accepted() {
        let mut state = HubState::default();
        let (creator, _creator_messages) = peer(&mut state);
        let (joiner, _joiner_messages) = peer(&mut state);
        state.on_pair_device_initiate(&creator);
        let key = state.peers[&creator].pair_key.clone().unwrap();
        let secret = state.pair_keys[&key].room_secret.clone();
        assert!(state.members("secret", &secret).is_none());
        state.on_pair_device_join(&joiner, &json!({"pairKey": key}));
        assert_eq!(
            state.members("secret", &secret),
            Some([creator, joiner].as_slice())
        );
        assert!(state.pair_keys.is_empty());
        assert!(state.members("public-id", &secret).is_none());
        state.on_room_secrets_deleted(&joiner, &json!({"roomSecrets": [secret]}));
        assert!(state.members("secret", &secret).is_none());
    }

    #[test]
    fn expired_offers_are_released_and_the_owner_is_notified() {
        let mut state = HubState::default();
        let (id, mut messages) = peer(&mut state);
        state.on_pair_device_initiate(&id);
        messages.try_recv().unwrap();
        let key = state.peers[&id].pair_key.clone().unwrap();
        state.pair_keys.get_mut(&key).unwrap().expires = Instant::now();
        state.expire_offer(&id);
        assert!(state.pair_keys.is_empty());
        assert!(state.peers[&id].pair_key.is_none());
        let notice: serde_json::Value =
            serde_json::from_str(&messages.try_recv().unwrap()).unwrap();
        assert_eq!(
            notice,
            json!({"type":"pair-device-canceled", "pairKey":key})
        );
    }

    #[test]
    fn only_the_current_socket_owns_a_reconnected_peer() {
        let mut state = HubState::default();
        let (id, _rx) = peer(&mut state);
        let first = state.peers[&id].tx.clone();
        let (replacement, _rx) = mpsc::unbounded_channel();
        state.peers.get_mut(&id).unwrap().tx = replacement.clone();
        assert!(state.owns_socket(&id, &replacement));
        assert!(!state.owns_socket(&id, &first));
    }

    #[test]
    fn a_new_hub_accepts_the_same_peer_proof_after_restart() {
        let config = Arc::new(Config::from_env().unwrap());
        let id = Uuid::new_v4();
        let start = |secret| Hub::new(config.clone(), secret);
        let original = start([42; 32]).peer_id_hash(&id);
        assert_eq!(start([42; 32]).peer_id_hash(&id), original);
        assert_ne!(start([43; 32]).peer_id_hash(&id), original);
    }
}
