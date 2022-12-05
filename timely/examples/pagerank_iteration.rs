use std::collections::{HashMap, HashSet};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::{operators::*, Scope};
use timely::dataflow::{InputHandle, ProbeHandle};

use pagerank::utils;

const START_YEAR: u64 = 1992;

// this is the iteration approach, it will run pagerank algorithm recursively untill reach the maximum iteration.
fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let end_year: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
        let iteration: u64 = std::env::args().nth(2).unwrap().parse().unwrap();

        // input is a stream of edges in the format of (from, to)
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow::<u64, _, _>(|scope| {
            // create a new input, into which we can push edge changes
            let edge_stream = input.to_stream(scope);

            let single_year = scope.iterative::<u64, _, _>(|subscope| {
                // create a new loop stream
                let (handle, contribution_stream) = subscope.loop_variable(1);

                let contribs = edge_stream.enter(subscope).binary_frontier(
                    &contribution_stream,
                    Exchange::new(|x: &(usize, usize)| x.0 as u64),
                    Exchange::new(|x: &(usize, usize, f64)| x.1 as u64),
                    "PageRank",
                    |_capability, _info| {
                        // where we stash out-of-order data
                        let mut edge_stash = HashMap::new();
                        let mut contribution_stash = HashMap::new();

                        // accumulative edges and ranks
                        let mut edges = HashMap::new();
                        let mut ranks = HashMap::new();

                        // empty list for swapping
                        let mut edge_vec = Vec::new();
                        let mut contribution_vec = Vec::new();

                        move |input1, input2, output| {
                            // hold on to edge changes until it is time
                            input1.for_each(|time, data| {
                                data.swap(&mut edge_vec);
                                edge_stash
                                    .entry(time.retain())
                                    .or_insert(Vec::new())
                                    .extend(edge_vec.drain(..));
                            });

                            // hold on to incoming contributions until it is time
                            input2.for_each(|time, data| {
                                data.swap(&mut contribution_vec);
                                contribution_stash
                                    .entry(time.retain())
                                    .or_insert(Vec::new())
                                    .extend(contribution_vec.drain(..));
                            });

                            let frontiers = &[input1.frontier(), input2.frontier()];

                            for (time, edge_changes) in edge_stash.iter_mut() {
                                if frontiers.iter().all(|f| !f.less_equal(time)) {
                                    let mut session = output.session(time);

                                    for (src, dst) in edge_changes.drain(..) {
                                        // populate all map using received values
                                        edges.entry(src).or_insert(Vec::new()).push(dst);
                                        ranks.entry(src).or_insert(1.0f64);
                                        ranks.entry(dst).or_insert(1.0f64);
                                    }

                                    // distribute contributions for next iteration
                                    for (src, dsts) in edges.iter_mut() {
                                        let contribution = ranks[src] / (dsts.len() as f64);
                                        for dst in dsts {
                                            session.give((*src, *dst, contribution));
                                        }
                                    }
                                }
                            }
                            edge_stash.retain(|_key, val| !val.is_empty());

                            for (time, contributions) in contribution_stash.iter_mut() {
                                if frontiers.iter().all(|f| !f.less_equal(time)) {
                                    let mut session = output.session(time);

                                    let mut contribution_sum = HashMap::new();
                                    for (_, dst, contribution) in contributions.drain(..) {
                                        *contribution_sum.entry(dst).or_insert(0.0f64) +=
                                            contribution;
                                    }

                                    // calculate the new rank for this iteration
                                    let mut new_ranks = HashMap::new();
                                    for vert in ranks.keys() {
                                        let contribution =
                                            contribution_sum.get(vert).unwrap_or(&0.0f64);
                                        new_ranks.insert(*vert, 0.15 + 0.85 * contribution);
                                    }

                                    // distribute contributions for next iteration
                                    ranks = new_ranks;
                                    for (src, rank) in &ranks {
                                        if let Some(dsts) = edges.get(src) {
                                            let contribution = *rank / (dsts.len() as f64);
                                            for dst in dsts {
                                                session.give((*src, *dst, contribution));
                                            }
                                        }
                                    }
                                }
                            }
                            contribution_stash.retain(|_key, val| !val.is_empty());
                        }
                    },
                );

                let branches = contribs.branch_when(move |t| t.inner < iteration);
                // continue the loop if we don't reach the maximum iteration
                branches.1.connect_loop(handle);
                // branch out if we reach the maximum iteration
                branches.0.leave()
            });

            // once we break out of the inner iterative scope, meaning we have reached the maximum iteration
            // for this year's citation, then we can calculate the result for this year
            single_year.probe_with(&mut probe).unary_frontier(
                Exchange::new(|_| 0),
                "Result",
                |_capability, _info| {
                    let mut contribution_stash = HashMap::new();
                    let mut contribution_vec = Vec::new();

                    move |input, _output: &mut OutputHandle<u64, Option<u64>, _>| {
                        // hold on to final contribution until it is time.
                        input.for_each(|time, data| {
                            data.swap(&mut contribution_vec);
                            contribution_stash
                                .entry(time.retain())
                                .or_insert(Vec::new())
                                .extend(contribution_vec.drain(..));
                        });

                        for (time, contributions) in contribution_stash.iter_mut() {
                            if !input.frontier.less_equal(time) {
                                // get total contribution
                                let mut contribution_sum = HashMap::new();
                                let mut verts = HashSet::new();
                                for (src, dst, contribution) in contributions.drain(..) {
                                    *contribution_sum.entry(dst).or_insert(0.0f64) += contribution;
                                    verts.insert(src);
                                    verts.insert(dst);
                                }

                                // calculate ranks based on incoming contributions
                                let mut ranks = HashMap::new();
                                for vert in &verts {
                                    let contribution =
                                        contribution_sum.get(vert).unwrap_or(&0.0f64);
                                    ranks.insert(*vert, 0.15 + 0.85 * contribution);
                                }

                                // get normalize factor
                                let sum: f64 = ranks.values().sum();
                                let normalize_factor = verts.len() as f64 / sum;

                                // get top ranks
                                let mut rank_vec = Vec::from_iter(ranks);
                                rank_vec.sort_by(|&(_, a), &(_, b)| b.partial_cmp(&a).unwrap());
                                println!("--- year {:?} top 5 ---", time.time());
                                for (vert, rank) in &rank_vec[0..5] {
                                    println!(
                                        "{:?} has rank score: {:?}",
                                        vert,
                                        rank * normalize_factor
                                    );
                                }

                                // no need to output anything in this point
                                // output.session(&time).give(None::<usize>);
                            }
                        }
                        contribution_stash.retain(|_key, val| !val.is_empty());
                    }
                },
            );
        });

        // feeding edges
        input.advance_to(START_YEAR);
        if worker.index() == 0 {
            for year in START_YEAR..=end_year {
                for e in utils::get_citations_from_file(year) {
                    input.send(e);
                }
                input.advance_to(year + 1);

                worker.step_while(|| probe.less_than(input.time()));
            }
        }
    })
    .unwrap();
}
