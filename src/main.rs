extern crate differential_dataflow;
extern crate rand;
extern crate stopwatch;
extern crate timely;

use std::cmp::min;
use std::hash::Hash;
use std::mem;

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;
use differential_dataflow::operators::group::Count;
use rand::{Rng, SeedableRng, StdRng};
use stopwatch::Stopwatch;
use timely::dataflow::*;
use timely::dataflow::operators::*;

type Node = u64;
type Edge = (Node, Node);

fn main() {
    let node_count: u64 = 1000000;
    let edge_count: u64 = 750000;

    timely::execute_from_args(std::env::args(), move |computation| {
        // Generate a random graph.
        let seed: &[_] = &[0];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        let mut graph = Vec::with_capacity(edge_count as usize);
        for _ in 0..edge_count {
            let node1: Node = rng.gen_range(0u64, node_count);
            let node2: Node = rng.gen_range(0u64, node_count);
            let edge: Edge = (node1, node2);
            graph.push((edge, 1));
        }
        let graph_iter = graph.clone();

        println!("Running connected components on a random graph ({} nodes, {} edges)", node_count, edge_count);
        println!("For each size, the number of components of that size (may take a moment):");

        let mut stopwatch = Stopwatch::start_new();

        let (mut input, probe) = computation.scoped::<u64, _, _>(|scope| {
            let (handle, updates) = scope.new_input::<(Edge, i32)>();

            let edges = updates.concat(&graph.to_stream(scope));
            let probe = connected_components(&Collection::new(edges))
                .map(|node| node.1)
                .count()
                .map(|count| count.1)
                .consolidate()
                .inspect(|x| {
                    // Printing the elapsed time here would require the stopwatch to be moved
                    // inside and cloned into this closure, which then wouldn't allow us to reset
                    // the stopwatch when introducing the changes.
                    println!("{:?}", x)
                })
                .probe().0;

            (handle, probe)
        });

        // Wait until the initial graph has been processed.
        let mut next = input.epoch() + 1;
        input.advance_to(next);
        while probe.lt(input.time()) {
            computation.step();
        }
        println!("Time to process: {}", stopwatch);

        println!();
        println!("Next: sequentially rewiring random edges (press [enter] each time):");

        // Introduce changes to the graph.
        for edge in graph_iter {
            // Wait for any user input.
            let mut user_input = String::new();
            std::io::stdin().read_line(&mut user_input).unwrap();

            // Delete the current edge and introduce a new one.
            let edge: Edge = edge.0;
            let node1: Node = rng.gen_range(0u64, node_count);
            let node2: Node = rng.gen_range(0u64, node_count);
            let new_edge: Edge = (node1, node2);
            println!("Rewiring edge: {:?} -> {:?}", edge, new_edge);
            stopwatch.restart();
            input.send((edge, -1));
            input.send((new_edge, 1));

            // Wait until the changes have been processed.
            next = input.epoch() + 1;
            input.advance_to(next);
            while probe.lt(input.time()) {
                computation.step();
            }
            println!("Time to process: {}", stopwatch);
        }
    }).unwrap();
}

fn connected_components<G: Scope>(edges: &Collection<G, Edge>) -> Collection<G, (Node, u64)>
where G::Timestamp: Lattice+Hash {
    let nodes = edges.map_in_place(|edge| {
            let min = min(edge.0, edge.1);
            *edge = (min, min);
        })
        .consolidate_by(|node| node.0);

    let edges = edges.map_in_place(|edge| mem::swap(&mut edge.0, &mut edge.1))
        .concat(&edges);

    nodes.filter(|_| false)
        .iterate(|inner| {
            let edges = edges.enter(&inner.scope());
            let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - (r.0).1.leading_zeros() as u64));

            inner.join_map_u(&edges, |_k, l, d| (*d, *l))
                .concat(&nodes)
                .group_u(|_, mut s, t| {
                    t.push((*s.peek().unwrap().0, 1));
                })
        })
}
