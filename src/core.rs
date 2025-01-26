use std::collections::HashSet;
use std::path::PathBuf;

pub(crate) fn count(
    corpus_path: &PathBuf,
    output_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    extra_chars: HashSet<char>,
) {
    let is_invalid_char = |c: char| {
        if extra_chars.is_empty() {
            c < '一' || c > '鿿'
        } else {
            c < '一' || c > '鿿' && !extra_chars.contains(&c)
        }
    };
}
