use std::collections::HashSet;
use std::io::stdin;
use std::path::{Path, PathBuf};

pub(crate) fn get_input() -> String {
    let mut input = String::new();
    loop {
        if let Ok(_) = stdin().read_line(&mut input) {
            return input.trim().to_string();
        } else {
            println!("错误：无法读取输入，请重新输入。");
        }
    }
}

pub(crate) fn get_corpus_path() -> PathBuf {
    println!("请输入语料文件路径：");
    loop {
        let path = PathBuf::from(get_input());
        if path.exists() {
            return path;
        } else {
            println!("错误：文件不存在，请重新输入。");
        }
    }
}

pub(crate) fn get_word_len() -> usize {
    println!("请输入要统计的词长：");
    loop {
        if let Ok(word_len) = get_input().parse() {
            if word_len > 0 {
                return word_len;
            } else {
                println!("错误：词长必须大于0，请重新输入。");
            }
        } else {
            println!("错误：无法解析输入的数字，请重新输入。");
        }
    }
}

pub(crate) fn get_output_path(corpus_path: &Path, word_len: usize) -> PathBuf {
    let dir = corpus_path.parent().expect("无法获取语料文件所在的目录");
    let name = corpus_path
        .file_stem()
        .expect("无法获取语料文件名")
        .to_str()
        .expect("无法转换文件名为字符串");
    let mut output_path = dir.join(format!("{name}_{word_len}字统计结果.txt"));
    let mut i: usize = 2;
    while output_path.exists() {
        output_path = dir.join(format!("{name}_{word_len}字统计结果_{i}.txt"));
        i += 1;
    }
    output_path
}

pub(crate) fn get_threshold() -> usize {
    println!("请输入词频阈值，此次数及以下的词将被忽略，留空则默认为1：");
    loop {
        let input = get_input();
        if input.is_empty() {
            return 1;
        } else if let Ok(threshold) = input.parse() {
            return threshold;
        } else {
            println!("错误：无法解析输入的数字，请重新输入。");
        }
    }
}

pub(crate) fn get_extra_chars() -> HashSet<char> {
    println!("请输入要纳入统计的除中文外的其他字符，回车结束。若无请留空：");
    let mut extra_chars = HashSet::new();
    for c in get_input().chars() {
        if c < '一' || c > '鿿' {
            extra_chars.insert(c);
        }
    }
    extra_chars
}
