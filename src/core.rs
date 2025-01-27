use dashmap::DashMap;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) fn count(
    corpus_path: &PathBuf,
    output_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    extra_chars: HashSet<char>,
) {
    let is_invalid_char = |c| {
        if extra_chars.is_empty() {
            c < '一' || c > '鿿'
        } else {
            (c < '一' || c > '鿿') && !extra_chars.contains(&c)
        }
    };
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
