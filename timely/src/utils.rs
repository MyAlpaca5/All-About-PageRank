use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

const DATA_DIR_PATH: &str = "../dataset/incremental";

pub fn get_citations_from_file(year: u64) -> Vec<(usize, usize)> {
    let path = Path::new(DATA_DIR_PATH).join(format!("{}-edges.txt", year));
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    let mut citations = Vec::new();
    for line in reader.lines() {
        if let Ok(line) = line {
            let fields = line
                .split_whitespace()
                .map(|s| s.parse::<usize>().unwrap())
                .take(2)
                .collect::<Vec<usize>>();
            let src_id = fields[0];
            let dst_id = fields[1];

            citations.push((src_id, dst_id));
        }
    }

    citations
}
