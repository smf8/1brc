use rustc_hash::FxHashMap;
use std::time;
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[tokio::main]
async fn main() -> io::Result<()> {
    let exec_start_time = time::Instant::now();

    let file = File::open("../measurements.txt").await?;
    let chunk_size = 1024 * 1024 * 2; // 40MB

    let mut reader = io::BufReader::with_capacity(chunk_size, file);
    let mut final_result: FxHashMap<&'static str, Vec<f64>> = FxHashMap::default();

    let (chunk_tx, chunk_rx) = channel::<Vec<u8>>(20000);
    let (result_tx, mut result_rx) = channel::<FxHashMap<&'static str, Vec<f64>>>(20000);

    let merge_handle = tokio::spawn(async move {
        while let Some(res) = result_rx.recv().await {
            merge_results(&mut final_result, res);
        }
        final_result
    });

    let compute_handle = tokio::spawn(async move {
        async_compute_chunk(chunk_rx, result_tx).await;
    });
    loop {
        let start_time = time::Instant::now();

        let mut buffer = Vec::with_capacity(chunk_size);

        // Fill the buffer as much as possible
        let mut total_bytes_read = 0;
        loop {
            let buf = reader.fill_buf().await?;
            if buf.is_empty() {
                break;
            }

            let bytes_read = buf.len();
            buffer.extend_from_slice(buf);
            reader.consume(bytes_read);
            total_bytes_read += bytes_read;

            // Stop if we've read enough or filled the buffer
            if total_bytes_read > chunk_size {
                break;
            }
        }

        if total_bytes_read == 0 {
            println!("All done");
            break;
        }

        // Read until the end of the current line to avoid splitting lines
        let mut tail = buffer.split_off(total_bytes_read);
        let tail_bytes = reader.read_until(b'\n', &mut tail).await?;
        if tail_bytes != 0 {
            buffer.extend_from_slice(&tail[..tail_bytes]);
        }

        println!("time spent reading data: {}ms", start_time.elapsed().as_millis());

        chunk_tx.send(buffer).await.unwrap();
    }

    drop(chunk_tx);

    let final_result = merge_handle.await?;
    compute_handle.await?;

    print_result(&final_result);

    println!(
        "total execution time: {}ms",
        exec_start_time.elapsed().as_millis()
    );

    Ok(())
}

fn merge_results(
    final_result: &mut FxHashMap<&'static str, Vec<f64>>,
    chunk_result: FxHashMap<&'static str, Vec<f64>>,
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

fn print_result(res: &FxHashMap<&'static str, Vec<f64>>) {
    let mut str_result = String::from("{");
    let mut cities: Vec<&&'static str> = res.keys().collect();
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

unsafe fn compute_chunk(data: &[u8]) -> FxHashMap<&'static str, Vec<f64>> {
    let mut result: FxHashMap<&'static str, Vec<f64>> = FxHashMap::default();
    let mut line_start = 0;

    let start_time = time::Instant::now();

    for (i, &byte) in data.iter().enumerate() {
        if byte == b'\n' {
            let line_bytes = &data[line_start..=i];
            let delim = line_bytes.iter().rposition(|&b| b == b';').unwrap();

            let city = std::str::from_utf8_unchecked(&line_bytes[..delim]);
            let temp = fast_parse_f64_u8(&line_bytes[delim + 1..]);

            if let Some(entry) = result.get_mut(city) {
                entry[0] = f64::min(entry[0], temp);
                entry[1] = f64::max(entry[1], temp);
                entry[2] += temp;
                entry[3] += 1.0;
            } else {
                // Leak the city string to get a &'static str
                let city_static: &'static str = Box::leak(city.to_owned().into_boxed_str());

                result.insert(city_static, vec![temp, temp, temp, 1.0]);
            }

            line_start = i + 1;
        }
    }

    println!("time spent computing chunk: {}ms", start_time.elapsed().as_millis());

    result
}

async fn async_compute_chunk(
    mut r: Receiver<Vec<u8>>,
    result_channel: Sender<FxHashMap<&'static str, Vec<f64>>>,
) {
    let mut handles = Vec::new();
    while let Some(data) = r.recv().await {
        let tx = result_channel.clone();

        handles.push(tokio::spawn(async move {
            let result = unsafe { compute_chunk(&data) };
            tx.send(result).await.unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    drop(result_channel);
}

#[inline]
fn fast_parse_f64_u8(s: &[u8]) -> f64 {
    let negative = s[0] == b'-';
    let s = if negative { &s[1..] } else { s };

    let mut i = 0;
    while i < s.len() && s[i] != b'.' {
        i += 1;
    }

    let int_part = &s[..i];
    let frac_part = &s[i + 1..];

    let mut int_val: i64 = 0;
    for &byte in int_part {
        int_val = int_val * 10 + (byte - b'0') as i64;
    }

    let frac_val = (frac_part[0] - b'0') as i64;
    let mut result = int_val as f64 + (frac_val as f64) / 10.0;

    if negative {
        result = -result;
    }
    result
}
