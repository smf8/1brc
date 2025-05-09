use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;

fn main() -> io::Result<()> {
    let file = File::open("../measurements.txt")?;
    let chunk_size = 1024 * 1024 * 400; // 40MB

    let mut reader = BufReader::with_capacity(chunk_size, file);
    let mut final_result: HashMap<String, Vec<f64>> = HashMap::new();
    let mut parallelism_number = thread::available_parallelism()?.get() - 1;
    let (chunk_tx, chunk_rx) = channel::<Vec<u8>>();
    let (result_tx, result_rx) = channel::<HashMap<String, Vec<f64>>>();

    parallelism_number -= 1;

    let merge_handle = thread::spawn(move || {
        while let Ok(res) = result_rx.recv() {
            merge_results(&mut final_result, res);
        }

        final_result
    });

    parallelism_number -= 1;
    let compute_handle = thread::spawn(move || {
        parallel_compute_chunk(chunk_rx, result_tx, parallelism_number);
    });

    loop {
        let mut buffer = vec![0_u8; chunk_size];
        let bytes_read = reader.read(&mut buffer)?;

        if bytes_read == 0 {
            println!("All done");
            break;
        }

        // Read until the end of the current line to avoid splitting lines
        let mut tail = buffer.split_off(bytes_read);
        let tail_bytes = reader.read_until(b'\n', &mut tail)?;
        if tail_bytes != 0 {
            buffer.extend_from_slice(&tail[..tail_bytes]);
        }

        chunk_tx.send(buffer).unwrap();
    }

    drop(chunk_tx);

    final_result = merge_handle.join().unwrap();
    compute_handle.join().unwrap();

    print_result(&final_result);
    Ok(())
}

/// Merges chunk results into the final result map.
fn merge_results(
    final_result: &mut HashMap<String, Vec<f64>>,
    chunk_result: HashMap<String, Vec<f64>>,
) {
    for (city, temps) in chunk_result {
        final_result
            .entry(city)
            .and_modify(|e| {
                e[0] = f64::min(e[0], temps[0]);
                e[1] = f64::max(e[1], temps[1]);
                e[2] += temps[2];
                e[3] += temps[3];
            })
            .or_insert(temps);
    }
}

/// Prints the final result in sorted order.
fn print_result(res: &HashMap<String, Vec<f64>>) {
    let mut str_result = String::from("{");
    let mut cities: Vec<&String> = res.keys().collect();
    cities.sort();

    for city in cities {
        let data = &res[city];
        let mean = data[2] / data[3];
        let _ = std::fmt::Write::write_fmt(
            &mut str_result,
            format_args!("{}={:.1}/{:.1}/{:.1}, ", city, data[0], mean, data[1]),
        );
    }

    str_result.push('}');
    println!("{str_result}");
}

/// Processes a chunk of bytes and returns a map of city statistics.
/// # Safety
/// Assumes the input is valid UTF-8 and lines are separated by `\n`.
unsafe fn compute_chunk(data: &[u8]) -> HashMap<String, Vec<f64>> {
    let mut result: HashMap<String, Vec<f64>> = HashMap::new();
    let mut line_start = 0;

    for (i, &byte) in data.iter().enumerate() {
        if byte == b'\n' {
            let line = std::str::from_utf8_unchecked(&data[line_start..=i]);
            let (city, temp) = parse_line(line);
            result
                .entry(city)
                .and_modify(|e| {
                    e[0] = f64::min(e[0], temp);
                    e[1] = f64::max(e[1], temp);
                    e[2] += temp;
                    e[3] += 1.0;
                })
                .or_insert(vec![temp, temp, temp, 1.0]);
            line_start = i + 1;
        }
    }

    result
}

fn parallel_compute_chunk(
    r: Receiver<Vec<u8>>,
    result_channel: Sender<HashMap<String, Vec<f64>>>,
    num_threads: usize,
) {
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_threads);
    // let num_threads = Arc::new(Mutex::new(num_threads));

    for data in r {
        // Wait for available thread slot if needed
        loop {
            // Clean up finished threads
            handles.retain(|h: &_| !h.is_finished());

            let current_count = handles.len();

            if current_count < num_threads {
                break;
            }

            // Wait a bit before checking again
            thread::sleep(std::time::Duration::from_millis(1));
        }

        // Spawn new thread to process data
        let tx = result_channel.clone();
        handles.push(thread::spawn(move || {
            let result = unsafe { compute_chunk(&data) };
            tx.send(result).unwrap();
        }));
    }

    handles.into_iter().for_each(|h| h.join().unwrap());
    drop(result_channel);
}

/// Parses a line into a city and temperature.
fn parse_line(line: &str) -> (String, f64) {
    let (city, temp_str) = line.split_once(';').unwrap();
    let temp = temp_str.trim_end().parse::<f64>().unwrap();
    (city.to_string(), temp)
}
