package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func main() {
	{
		f, err := os.Create("profile.out")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	startTime := time.Now()
	s2()

	fmt.Printf("Total execution time: %s\n", time.Since(startTime))
}

func ChunkQueueWorker(chunkQueue chan []byte, resultQueue chan map[string][]float64) {
	wg := &sync.WaitGroup{}

	for chunk := range chunkQueue {
		wg.Add(1)
		go func(resultChan chan map[string][]float64) {
			result := ComputeChunk(chunk)
			resultChan <- result
			wg.Done()
		}(resultQueue)
	}

	wg.Wait()
	close(resultQueue)
}

func PrintResult(res map[string][]float64) {
	strResult := &strings.Builder{}

	strResult.WriteString("{")
	// Print sorted result: city:min,mean,max
	cities := make([]string, 0, len(res))
	for city := range res {
		cities = append(cities, city)
	}

	sort.Strings(cities)
	for _, city := range cities {
		data := res[city]
		mean := data[2] / data[3]
		_, err := fmt.Fprintf(strResult, "%s=%.1f/%.1f/%.1f, ", city, data[0], mean, data[1])
		if err != nil {
			panic(err)
		}
	}

	strResult.WriteString("}")

	fmt.Println(strResult.String())
}

// ComputeChunk result map content:
// [0] -> min
// [1] -> max
// [2] -> sum
// [3] -> count
func ComputeChunk(chunk []byte) map[string][]float64 {
	var (
		lineStart, lineEnd int
		line               []byte
	)

	result := make(map[string][]float64)

	for i := range chunk {
		if chunk[i] == '\n' {
			lineEnd = i
			line = chunk[lineStart : lineEnd+1]
			lineStart = lineEnd + 1

			city, temp := ParseLine(line)

			if cityData, ok := result[city]; ok {
				cityData[0] = min(cityData[0], temp)
				cityData[1] = max(cityData[1], temp)
				cityData[2] += temp
				cityData[3]++
			} else {
				result[city] = []float64{temp, temp, temp, 1}
			}
		}
	}

	return result
}

func ParseLine(line []byte) (string, float64) {
	index := 0
	var (
		city string
		temp float64
	)

	for i, char := range line {
		if char == ';' {
			index = i
			city = unsafe.String(&line[0], i) // Convert byte slice to string using unsafe
		}

		if char == '\n' {
			temp = ParseFloatOneDecimal(line[index+1 : i])
			break
		}
	}

	return city, temp
}

// ParseFloatOneDecimal will manually parse a float from a byte slice
// we know that we have only one decimal point
func ParseFloatOneDecimal(input []byte) float64 {
	var intPart, fracPart int
	var isNegative bool

	// Handle negative numbers
	if input[0] == '-' {
		isNegative = true
		input = input[1:]
	}

	// Parse the integer part
	i := 0
	for ; i < len(input) && input[i] != '.'; i++ {
		intPart = intPart*10 + int(input[i]-'0')
	}

	// Parse the fractional part (assumes exactly one digit after '.')
	if i+1 < len(input) {
		fracPart = int(input[i+1] - '0')
	}

	// Combine integer and fractional parts
	result := float64(intPart) + float64(fracPart)/10.0
	if isNegative {
		result = -result
	}

	return result
}

func MergeResult(r1, r2 map[string][]float64) {
	for city, data := range r2 {
		if existingData, ok := r1[city]; ok {
			existingData[0] = min(existingData[0], data[0])
			existingData[1] = max(existingData[1], data[1])
			existingData[2] += data[2]
			existingData[3] += data[3]
		} else {
			r1[city] = data
		}
	}
}

func s2() {
	PrintTime("start program")

	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	fileReader := bufio.NewReader(file)
	chunkSize := 1024 * 1024 * 30 // 30MB chunk size

	chunkQueue := make(chan []byte, 1024)
	resultQueue := make(chan map[string][]float64, 1024)
	finalResult := make(map[string][]float64)
	doneSignal := make(chan struct{})

	go ChunkQueueWorker(chunkQueue, resultQueue)

	go func(queue chan map[string][]float64) {
		for res := range queue {
			MergeResult(finalResult, res)
		}

		close(doneSignal)
	}(resultQueue)

	PrintTime("start read loop")
	for {
		tempChunk := make([]byte, chunkSize)
		readBytes, err := io.ReadFull(fileReader, tempChunk)

		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			if errors.Is(err, io.EOF) {
				close(chunkQueue)
				break
			}
			panic(err)
		}

		tempChunk = tempChunk[:readBytes]

		// Continue reading until a newline is found
		for {
			line, err := fileReader.ReadBytes('\n')
			tempChunk = append(tempChunk, line...)
			if err != nil || len(line) > 0 {
				break
			}
		}

		chunkQueue <- tempChunk
		// Process the chunk
		// Example: fmt.Println(len(chunk))
	}

	PrintTime("finish read loop")

	<-doneSignal

	PrintTime("finish result")

	PrintResult(finalResult)

	PrintTime("final app time")
}

func PrintTime(s string) {
	fmt.Printf("time: %s: %s\n", time.Now(), s)
}
