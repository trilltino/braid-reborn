use crate::client::MessageParser;
use rand::prelude::*;
use rand::rngs::SmallRng;
use rand::SeedableRng;

/// Fuzz the message parser with arbitrary random bytes.
fn fuzz_parser_with_random_bytes(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut parser = MessageParser::new();

    let len = rng.gen_range(1..4096);
    let mut data = vec![0u8; len];
    rng.fill_bytes(&mut data);

    let mut pos = 0;
    while pos < data.len() {
        let remaining = data.len() - pos;
        let chunk_size = rng.gen_range(1..=remaining);
        let _ = parser.feed(&data[pos..pos + chunk_size]);
        pos += chunk_size;
    }
}

/// Helper to generate a semi-valid Braid message with mutations.
fn generate_semi_valid_message(rng: &mut SmallRng) -> Vec<u8> {
    let mut msg = Vec::new();

    if rng.gen_bool(0.1) {
        msg.extend_from_slice(b"HTTP/1.1 209 Subscription\r\n");
    } else if rng.gen_bool(0.1) {
        msg.extend_from_slice(b"PATCH /foo HTTP/1.1\r\n");
    }

    for _ in 0..rng.gen_range(1..5) {
        let key = if rng.gen_bool(0.5) {
            "Content-Length"
        } else {
            "Braid-Version"
        };
        let val = rng.gen_range(0..1000).to_string();
        msg.extend_from_slice(format!("{}: {}\r\n", key, val).as_bytes());
    }

    msg.extend_from_slice(b"\r\n");

    let body_len = rng.gen_range(0..100);
    let mut body = vec![0u8; body_len];
    rng.fill_bytes(&mut body);
    msg.extend_from_slice(&body);

    msg
}

/// Fuzz the parser with semi-valid messages.
fn fuzz_parser_with_mutated_messages(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut parser = MessageParser::new();

    let mut stream = Vec::new();
    for _ in 0..rng.gen_range(1..10) {
        stream.extend_from_slice(&generate_semi_valid_message(&mut rng));
    }

    for _ in 0..rng.gen_range(0..10) {
        if stream.is_empty() {
            break;
        }
        let idx = rng.gen_range(0..stream.len());
        match rng.gen_range(0..3) {
            0 => stream[idx] ^= 0xFF,
            1 => stream.insert(idx, rng.gen()),
            2 => {
                stream.remove(idx);
            }
            _ => unreachable!(),
        }
    }

    let mut pos = 0;
    while pos < stream.len() {
        let remaining = stream.len() - pos;
        let chunk_size = rng.gen_range(1..=remaining);
        let _ = parser.feed(&stream[pos..pos + chunk_size]);
        pos += chunk_size;
    }
}

#[test]
fn test_parser_fuzz_random_once() {
    fuzz_parser_with_random_bytes(42);
}

#[test]
fn test_parser_fuzz_mutated_once() {
    fuzz_parser_with_mutated_messages(42);
}

#[test]
#[ignore]
fn test_parser_fuzz_forever() {
    for seed in 0.. {
        if seed % 1000 == 0 {
            println!("Fuzzing seed {}", seed);
        }
        fuzz_parser_with_random_bytes(seed);
        fuzz_parser_with_mutated_messages(seed);
    }
}
