use dashmap::DashMap;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) fn count(
    corpus_path: &PathBuf,
    output_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    extra_chars: HashSet<char>,
) {
    let last_word_freq;
    if extra_chars.is_empty() {
        let is_invalid_char = |c| c < '一' || c > '鿿';
        let base_word_freq = traverse_corpus(corpus_path, word_len, is_invalid_char);
        last_word_freq = local_pick(
            &base_word_freq,
            corpus_path,
            word_len,
            threshold,
            is_invalid_char,
        );
    } else {
        let is_invalid_char = |c| (c < '一' || c > '鿿') && !extra_chars.contains(&c);
        let base_word_freq = traverse_corpus(corpus_path, word_len, is_invalid_char);
        last_word_freq = local_pick(
            &base_word_freq,
            corpus_path,
            word_len,
            threshold,
            is_invalid_char,
        );
    }
    filter_sort_save(&last_word_freq, threshold, output_path);
}

fn traverse_corpus(
    corpus_path: &PathBuf,
    word_len: usize,
    is_invalid_char: impl Fn(char) -> bool + Sync,
) -> DashMap<String, AtomicUsize> {
    let word_freq = DashMap::with_capacity(8192);
    let file = File::open(corpus_path).expect("无法打开语料文件");
    BufReader::new(file).lines().par_bridge().for_each(|line| {
        let mut word_buffer = VecDeque::with_capacity(word_len);
        for c in line.expect("无法读取语料文件的一行").chars() {
            if is_invalid_char(c) {
                word_buffer.clear();
            } else {
                word_buffer.push_back(c);
            }
            if word_buffer.len() == word_len {
                word_freq
                    .entry(word_buffer.iter().collect())
                    .or_insert(AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                word_buffer.pop_front();
            }
        }
    });
    word_freq
}

fn local_pick(
    base_word_freq: &DashMap<String, AtomicUsize>,
    corpus_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    is_invalid_char: impl Fn(char) -> bool + Sync,
) -> DashMap<String, AtomicUsize> {
    let word_freq = DashMap::with_capacity(8192);
    let window_size = word_len * 2 - 1;
    let file = File::open(corpus_path).expect("无法打开语料文件");
    BufReader::new(file).lines().par_bridge().for_each(|line| {
        let mut window = VecDeque::with_capacity(window_size);
        for c in line.expect("无法读取语料文件的一行").chars() {
            if is_invalid_char(c) {
                window.clear();
            } else {
                window.push_back(c);
            }
            if window.len() == window_size {
                let mut max_word = "".to_string();
                let mut max_freq = 0;
                for i in 0..word_len {
                    let word: String = window.iter().skip(i).take(word_len).collect();
                    let freq = base_word_freq
                        .get(&word)
                        .map(|f| f.load(Ordering::Relaxed))
                        .expect("第一轮统计结果中缺少词。这是不该出现的错误。请联系开发者。");
                    if freq > max_freq {
                        max_word = word;
                        max_freq = freq;
                    }
                    window.pop_front();
                }
                if max_freq > threshold {
                    word_freq
                        .entry(max_word)
                        .or_insert(AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    word_freq
}

fn filter_sort_save(
    word_freq: &DashMap<String, AtomicUsize>,
    threshold: usize,
    output_path: &PathBuf,
) {
    let mut entries: Vec<_> = word_freq
        .iter()
        .filter_map(|entry| {
            let word = entry.key().clone();
            let freq = entry.value().load(Ordering::Relaxed);
            if freq > threshold {
                Some((word, freq))
            } else {
                None
            }
        })
        .collect();

    entries.sort_by(|(_, a_val), (_, b_val)| b_val.cmp(a_val));

    let content = entries
        .into_iter()
        .map(|(word, freq)| format!("{word}\t{freq}"))
        .collect::<Vec<_>>()
        .join("\n");
    let file = File::create(output_path).expect("无法创建结果文件");
    let mut writer = BufWriter::new(file);
    writer
        .write_all(content.as_bytes())
        .expect("无法写入结果文件");
}
