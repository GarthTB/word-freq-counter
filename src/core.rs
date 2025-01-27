use dashmap::DashMap;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) fn run(
    corpus_path: &PathBuf,
    output_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    extra_chars: HashSet<char>,
) {
    let word_freq = if extra_chars.is_empty() {
        count(corpus_path, word_len, threshold, |c| c < '一' || c > '鿿')
    } else {
        count(corpus_path, word_len, threshold, |c| {
            (c < '一' || c > '鿿') && !extra_chars.contains(&c)
        })
    };
    filter_sort_save(&word_freq, threshold, output_path);
}

fn count(
    corpus_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    is_invalid_char: impl Fn(char) -> bool + Sync,
) -> DashMap<String, AtomicUsize> {
    println!("第一轮统计中...");
    let base_word_freq: DashMap<String, AtomicUsize> = DashMap::with_capacity(8192);
    let bare_word_len = word_len - 1;
    let file = File::open(corpus_path).expect("无法打开语料文件");
    BufReader::new(file).lines().par_bridge().for_each(|line| {
        let chars: Vec<_> = line.expect("无法读取语料文件的一行").chars().collect();
        let mut head = 0;
        for tail in 0..chars.len() {
            if is_invalid_char(chars[tail]) {
                head = tail + 1;
            } else if tail - head == bare_word_len {
                base_word_freq
                    .entry(chars[head..tail + 1].iter().collect())
                    .or_insert(AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                head += 1;
            }
        }
    });
    println!("统计完成，初步过滤中...");
    if threshold > 0 {
        base_word_freq.retain(|_, freq| freq.load(Ordering::Relaxed) > threshold);
    }
    println!("过滤完成，第二轮统计中...");
    let word_freq: DashMap<String, AtomicUsize> = DashMap::with_capacity(8192);
    let bare_window_size = bare_word_len * 2;
    let file = File::open(corpus_path).expect("无法打开语料文件");
    BufReader::new(file).lines().par_bridge().for_each(|line| {
        let chars: Vec<_> = line.expect("无法读取语料文件的一行").chars().collect();
        let mut head = 0;
        for tail in 0..chars.len() {
            if is_invalid_char(chars[tail]) {
                head = tail + 1;
            } else if tail - head == bare_window_size {
                let window = &chars[head..tail + 1];
                let mut max_word = "".to_string();
                let mut max_freq = 0;
                for i in 0..word_len {
                    let word = window[i..i + word_len].iter().collect();
                    if let Some(freq) = base_word_freq.get(&word).map(|f| f.load(Ordering::Relaxed)) {
                        if freq > max_freq {
                            max_word = word;
                            max_freq = freq;
                        }
                    }
                }
                head += word_len;
                if max_freq > threshold {
                    word_freq
                        .entry(max_word)
                        .or_insert(AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    println!("统计完成。");
    word_freq
}

fn filter_sort_save(
    word_freq: &DashMap<String, AtomicUsize>,
    threshold: usize,
    output_path: &PathBuf,
) {
    println!("过滤条目中...");
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
    println!("过滤完成，排序中...");
    entries.sort_by(|(_, a_val), (_, b_val)| b_val.cmp(a_val));
    println!("排序完成，输出中...");
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
    println!("结果保存成功。统计结束。");
}
