#![no_main]
use braid_http::{protocol::text::apply_text_patches, types::Patch};
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: (&str, usize, usize, &str)| {
    let (base, a, b, insert) = data;
    if base.len() > 32768 || insert.len() > 16384 {
        return;
    }
    let mut chars: Vec<char> = base.chars().collect();
    let a = a % (chars.len() + 1);
    let b = b % (chars.len() + 1);
    let (start, end) = (a.min(b), a.max(b));
    chars.splice(start..end, insert.chars());
    let expected: String = chars.into_iter().collect();
    let patch = Patch::text(format!("[{start}:{end}]"), insert.to_owned());
    assert_eq!(apply_text_patches(base, &[patch]).unwrap(), expected);
});
