use dashmap::DashMap;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) fn run(
    corpus_path: &PathBuf,
    output_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    extra_chars: HashSet<char>,
) {
    let result = if extra_chars.is_empty() {
        count(corpus_path, word_len, threshold, |c| c < '一' || c > '鿿')
    } else {
        count(corpus_path, word_len, threshold, |c| {
            (c < '一' || c > '鿿') && !extra_chars.contains(&c)
        })
    };
    sort_and_save(result, output_path);
}

fn count(
    corpus_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    invalid_char: impl Fn(char) -> bool + Sync,
) -> Vec<(String, usize)> {
    println!("第一轮统计中...");
    let raw1 = count1(corpus_path, word_len, &invalid_char);
    println!("统计完成。初步处理中...");
    let base_map = Arc::new(convert_dashmap(raw1, threshold));
    println!("处理完成。第二轮统计中...");
    let raw2 = count2(corpus_path, word_len, threshold, &invalid_char, base_map);
    println!("统计完成。处理结果中...");
    let result = convert_dashmap(raw2, threshold);
    println!("处理完成。");
    result
}

fn count1(
    corpus_path: &PathBuf,
    word_len: usize,
    invalid_char: impl Fn(char) -> bool + Sync,
) -> DashMap<String, AtomicUsize> {
    let map: DashMap<String, AtomicUsize> = DashMap::with_capacity(8192);
    let bare_word_len = word_len - 1;
    let file = File::open(corpus_path).expect("无法打开语料文件");
    BufReader::new(file).lines().par_bridge().for_each(|line| {
        let chars: Vec<_> = line.expect("无法读取语料文件的一行").chars().collect();
        let mut head = 0;
        for tail in 0..chars.len() {
            if invalid_char(chars[tail]) {
                head = tail + 1;
            } else if tail - head == bare_word_len {
                map.entry(chars[head..tail + 1].into_iter().collect())
                    .or_insert(AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                head += 1;
            }
        }
    });
    map
}

fn count2(
    corpus_path: &PathBuf,
    word_len: usize,
    threshold: usize,
    invalid_char: impl Fn(char) -> bool + Sync,
    base_map: Arc<HashMap<String, usize>>,
) -> DashMap<String, AtomicUsize> {
    let map: DashMap<String, AtomicUsize> = DashMap::with_capacity(8192);
    let bare_window_size = 2 * word_len - 2;
    let file = File::open(corpus_path).expect("无法打开语料文件");
    BufReader::new(file).lines().par_bridge().for_each(|line| {
        let chars: Vec<_> = line.expect("无法读取语料文件的一行").chars().collect();
        let mut head = 0;
        for tail in 0..chars.len() {
            if invalid_char(chars[tail]) {
                head = tail + 1;
            } else if tail - head == bare_window_size {
                let mut max_word = "".to_string();
                let mut max_freq = 0;
                for _ in 0..word_len {
                    let word = chars[head..head + word_len].into_iter().collect();
                    if let Some(freq) = base_map.get(&word) {
                        if *freq > max_freq {
                            max_word = word;
                            max_freq = *freq;
                        }
                    }
                    head += 1;
                }
                if max_freq > threshold {
                    map.entry(max_word)
                        .or_insert(AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    map
}

fn sort_and_save(mut result: Vec<(String, usize)>, output_path: &PathBuf) {
    println!("排序中...");
    result.sort_by(|(_, a_val), (_, b_val)| b_val.cmp(a_val));
    println!("排序完成。输出中...");
    let file = File::create(output_path).expect("无法创建结果文件");
    let mut writer = BufWriter::new(file);
    let content = result
        .into_iter()
        .map(|(word, freq)| format!("{word}\t{freq}"))
        .collect::<Vec<_>>()
        .join("\n");
    writer
        .write_all(content.as_bytes())
        .expect("无法写入结果文件");
    println!("输出成功。统计结束。");
}

fn convert_dashmap<T>(map: DashMap<String, AtomicUsize>, threshold: usize) -> T
where
    T: FromIterator<(String, usize)>,
{
    if threshold > 0 {
        map.into_iter()
            .filter_map(|(k, v)| {
                let count = v.load(Ordering::Relaxed);
                (count > threshold).then(|| (k, count))
            })
            .collect()
    } else {
        map.into_iter()
            .map(|(k, v)| (k, v.load(Ordering::Relaxed)))
            .collect()
    }
}
