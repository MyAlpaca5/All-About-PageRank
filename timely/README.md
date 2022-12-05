# Timely Dataflow PageRank
Calculate the PageRank using [Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/introduction.html). Provided both iteration approach and tolerance approach.

## Prerequisites
- Rust 1.65.0

## How to run
Choose one of the following command to get the yearly result from 1992 to `end_year`:
- single-worker version

    `<end_year> <iteration>`
    ``` bash
    cargo run --release --example pagerank_iteration -- 2002 40
    ```
    or

    `<end_year> <tolerance>`
    ``` bash
    cargo run --release --example pagerank_tolerance -- 2002 1e-10
    ```
- multiple-worker version

    `<end_year> <iteration> -w<num_worker>`
    ``` bash
    cargo run --release --example pagerank_iteration -- 2002 40 -w4
    ```
    or

    `<end_year> <tolerance> -w<num_worker>`
    ``` bash
    cargo run --release --example pagerank_tolerance -- 2002 1e-10 -w4
    ```