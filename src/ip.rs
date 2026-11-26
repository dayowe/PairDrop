//! Client IP resolution, ported from `_setIP` / `ipIsPrivate` in upstream's `server/peer.js`.
//!
//! PairDrop groups peers into rooms by IP, and deliberately collapses every private address to
//! `127.0.0.1` so that everyone on the server's own network lands in the same room.

use std::net::IpAddr;

use axum::http::HeaderMap;

use crate::config::Config;

pub fn trusted_proxies(setting: Option<&str>) -> Result<Vec<ipnet::IpNet>, String> {
    let Some(setting) = setting.filter(|s| !s.trim().is_empty()) else {
        return Ok(vec![]);
    };
    setting
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            entry
                .parse::<ipnet::IpNet>()
                .or_else(|_| entry.parse::<IpAddr>().map(ipnet::IpNet::from))
                .map_err(|_| {
                    "TRUSTED_PROXIES expects a comma-separated list of IP addresses or CIDRs".into()
                })
        })
        .collect()
}

pub fn client_address(
    headers: &HeaderMap,
    socket: IpAddr,
    trusted: &[ipnet::IpNet],
) -> Option<IpAddr> {
    let socket = socket.to_canonical();
    let is_trusted = |ip: &IpAddr| trusted.iter().any(|range| range.contains(ip));
    if !is_trusted(&socket) {
        let behind_proxy = [
            "x-forwarded-for",
            "x-real-ip",
            "forwarded",
            "cf-connecting-ip",
        ]
        .iter()
        .any(|name| headers.contains_key(*name));
        return (!behind_proxy).then_some(socket);
    }
    let fields: Vec<_> = headers.get_all("x-forwarded-for").iter().collect();
    for field in fields.into_iter().rev() {
        for hop in field.to_str().ok()?.rsplit(',') {
            let ip = hop.trim().parse::<IpAddr>().ok()?.to_canonical();
            if !is_trusted(&ip) {
                return Some(ip);
            }
        }
    }
    None
}

pub fn resolve(headers: &HeaderMap, socket: IpAddr, conf: &Config) -> Option<String> {
    let ip = client_address(headers, socket, &conf.trusted_proxies)?;
    if ip.is_loopback() || is_private(&ip.to_string()) {
        return Some("127.0.0.1".into());
    }
    if let (IpAddr::V6(ip), Some(count)) = (ip, conf.ipv6_localize) {
        return Some(
            ip.segments()[..count]
                .iter()
                .map(|segment| format!("{segment:x}"))
                .collect::<Vec<_>>()
                .join(":"),
        );
    }
    Some(ip.to_string())
}

fn is_private(ip: &str) -> bool {
    if !ip.contains(':') {
        let octets: Vec<&str> = ip.split('.').collect();
        if octets.len() != 4 {
            return false;
        }
        return match (octets[0], octets[1].parse::<u8>()) {
            ("10", _) => true,
            ("172", Ok(second)) => (16..=31).contains(&second),
            ("192", _) => octets[1] == "168",
            _ => false,
        };
    }

    // First non-empty hextet decides; `::`-compressed addresses may start with empty ones.
    let Some(first) = ip.split(':').find(|segment| !segment.is_empty()) else {
        return false;
    };
    let first = first.to_ascii_lowercase();

    // Deprecated site-local (fec0::/10), unique local (fc00::/7), link local (fe80::/10),
    // and the discard prefix.
    let hex = |s: &str| s.chars().all(|c| c.is_ascii_hexdigit());
    match first.as_str() {
        "fe80" | "100" => true,
        s if s.len() == 4 && hex(s) => {
            let prefix = &s[..2];
            let third = s.as_bytes()[2] as char;
            (prefix == "fe" && ('c'..='f').contains(&third)) || prefix == "fc" || prefix == "fd"
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_the_first_client_beyond_the_trusted_proxy_chain() {
        let proxies = trusted_proxies(Some("192.0.2.9, 2001:db8::/64")).unwrap();
        let proxy = "::ffff:192.0.2.9".parse().unwrap();
        let client = "198.51.100.5".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(client_address(&headers, client, &proxies), Some(client));
        assert_eq!(client_address(&headers, proxy, &proxies), None);
        headers.append("x-forwarded-for", "198.51.100.5".parse().unwrap());
        headers.append("x-forwarded-for", "2001:db8::7".parse().unwrap());
        assert_eq!(client_address(&headers, proxy, &proxies), Some(client));
        assert_eq!(client_address(&headers, proxy, &[]), None);
        assert!(trusted_proxies(Some("192.0.2.9/56")).is_err());
    }

    #[test]
    fn ipv4_private_ranges() {
        assert!(is_private("10.0.0.1"));
        assert!(is_private("192.168.1.83"));
        assert!(is_private("172.16.0.1"));
        assert!(is_private("172.31.255.255"));
        assert!(!is_private("172.15.0.1"));
        assert!(!is_private("172.32.0.1"));
        assert!(!is_private("8.8.8.8"));
    }

    #[test]
    fn ipv6_local_ranges() {
        assert!(is_private("fe80::1"));
        assert!(is_private("fd00::1"));
        assert!(is_private("fc00::1"));
        assert!(is_private("fec0::1"));
        assert!(is_private("100::1"));
        assert!(!is_private("2001:db8::1"));
    }
}
