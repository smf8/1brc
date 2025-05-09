# 1brc

## Go
This is my implementation of the 1brc in Go.

The algorithm is fairly simple and [explained here](https://github.com/gunnarmorling/1brc#1%EF%B8%8F%E2%83%A3%EF%B8%8F-the-one-billion-row-challenge).

After writing the solution, to generate the input you can follow [these steps](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge).

My development considerations:
* Use buffered I/O
  * using `bufio.NewReader` on top of `os.File` and using `io.ReadFull` for reading chunks of data at a time.
* Introduce concurrency
  * Reading `chunks` and computing per city results can be done in parallel.
  * Using a channel to send `chunks` to `ComputeChunk` and another channel to send partial results an aggregator goroutine (inside `s2` function).
* **At this step I introduced pprof to optimize sections with the highest CPU time**
* Write manual string parsers instead of `bytes.Split` to avoid allocations.
* Use `unsafe.String` method for `[]byte <-> string` conversion.
* Write manual float64 parser instead of `strconv.ParseFloat`.
* Use manual buffer char range instead of `bytes.Buffer.ReadBytes('\n')` for per line calculations.'

###  Result

The result on my Macbook Air M2 16GB using `go 1.24.2` is:
```text
Total execution time: 8.589658583s
```

## Rust
The first solution is std Rust without any additional crate. It was also a synchronous solution.

It took `1:24.97 total` time to produce the result. **Not so blazingly fast ** :D

The first clear optimization is to run it in parallel. By leveraging `std::thread`. We've reduced execution time to `31s`.
Still **not blazingly fast**.

### Time to visit crates.io 
Rust doesn't have the best and richest standard library, but it's got many great crates including:
* Tokio (async runtime)
  * Using async i/o
  * Increasing number of worker threads (green threads)
* rustc-hash
  * based on profiles, the time spent on hashing `String`s is noticeable.
  * Using `FxHashMap` resulted in `6s` improvement without using a runtime. Totally `24s`.


**In our go implementation**, we used `unsafe.String()` to bypass allocation for city names, the initial rust implementation, used `.to_string()` for insertion since HashMap requires ownership of string.
By using `&'static str` instead of `String` we can use the same idea used in our Go implementation. **This change reduced execution time by `14s`** Best execution time until now is `10s`.

### Result

The result on my Macbook Air M2 16GB using `rust nightly 1.88.0` is:

`8.1s` which unfortunately isn't blazingly fast.