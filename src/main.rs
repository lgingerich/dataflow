use differential_dataflow::input::InputSession;
use differential_dataflow::operators::reduce::Reduce;

fn main() {

    // Set the number of workers
    let workers = 1;

    // Create timely execution config
    let config = timely::execute::Config::process(workers);

    timely::execute(config, |worker| {

        // Get the worker index
        let index = worker.index();

        // input session: lets us feed ints
        let mut input = InputSession::<usize, i64, isize>::new(); // <time, value, diff>

        // Build probe to track computation progress
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        worker.dataflow::<usize, _, _>(|scope| {
            // make a differential collection
            let numbers = input.to_collection(scope);

            // map all elements to a single key, then reduce to sum
            let sum = numbers
                .map(|x| ((), x))
                .reduce_named("Sum", |_key, input, output| {
                    let mut total = 0i64;
                    for (val, diff) in input.iter() {
                        total += *val * (*diff as i64);
                    }
                    if total != 0 {
                        output.push((total, 1));
                    }
                });

            // print the updates
            sum.inspect(move |x| println!("Worker {} SUM UPDATE: {:?}", index, x))
               .probe_with(&mut probe);
        });

        // Feed in some numbers
        input.insert(1);
        input.insert(2);
        input.insert(3);
        input.flush();

        input.advance_to(1);
        
        // Step a reasonable number of times to process the data
        // The probe check doesn't work reliably with InputSession, but stepping works
        for _ in 0..10 {
            worker.step();
        }

        input.insert(5);
        input.flush();
        input.advance_to(2);
        
        // Step again to process the second batch
        for _ in 0..10 {
            worker.step();
        }
    }).unwrap();
}
