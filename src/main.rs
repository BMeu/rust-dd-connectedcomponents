extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;

fn main() {
    timely::execute_from_args(std::env::args(), |computation| {
       let mut input = computation.scoped(move |scope| {
           let (input, stream) = scope.new_input();
           stream.inspect(|x| println!("hello {}", x));
           input
       });

        for round in 0..10 {
            input.send(round);
            input.advance_to(round + 1);
            computation.step();
        }
    }).unwrap();
}
