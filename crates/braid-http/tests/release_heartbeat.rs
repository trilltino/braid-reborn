#![allow(deprecated)] // The compatibility wrapper itself is under test.
use braid_http::protocol::{parse_heartbeat, parse_heartbeat_duration};
use std::time::Duration;

#[test]
#[allow(deprecated)]
fn durations_are_precise_and_legacy_seconds_round_up() {
    for (wire, duration) in [
        ("500ms", Duration::from_millis(500)),
        ("0.5", Duration::from_millis(500)),
        ("1.25s", Duration::from_millis(1250)),
        ("86400", Duration::from_secs(86400)),
    ] {
        assert_eq!(parse_heartbeat_duration(wire).unwrap(), duration);
        assert_eq!(braid_http::client::parse_heartbeat(wire).unwrap(), duration);
        assert_eq!(
            parse_heartbeat(wire).unwrap(),
            duration.as_secs() + u64::from(duration.subsec_nanos() != 0)
        );
    }
    for invalid in [
        "0",
        "0ms",
        "-1",
        "NaN",
        "inf",
        "1e999",
        "86401",
        "",
        "garbage",
        "0.00000000001ms",
    ] {
        assert!(parse_heartbeat_duration(invalid).is_err(), "{invalid}");
        assert!(
            braid_http::client::HeartbeatConfig::from_header(invalid).is_none(),
            "{invalid}"
        );
    }
}
