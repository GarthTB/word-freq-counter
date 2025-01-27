mod core;
mod init;

fn main() {
    loop {
        let corpus_path = init::get_corpus_path();
        let word_len = init::get_word_len();
        let output_path = init::get_output_path(&corpus_path, word_len);
        let threshold = init::get_threshold();
        let extra_chars = init::get_extra_chars();

        println!("---");
        println!("使用参数：");
        println!("语料文件路径：{}", corpus_path.display());
        println!("输出文件路径：{}", output_path.display());
        println!("词长：{word_len}");
        println!("词频阈值：{threshold}");
        println!("额外字符（共{}个）：{:?}", extra_chars.len(), extra_chars);
        println!("---");

        core::run(&corpus_path, &output_path, word_len, threshold, extra_chars);

        println!("输入y以进行新一轮统计...");
        if init::get_input(true) != "y" {
            break;
        }
    }
}
