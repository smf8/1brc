package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	s2()
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
	buff := bytes.NewBuffer(chunk)

	result := make(map[string][]float64)
	for {
		line, err := buff.ReadBytes('\n')
		if err != nil {
			break
		}

		if len(line) == 0 {
			continue
		}

		lineData := bytes.Split(bytes.TrimSpace(line), []byte(";"))
		city, tempStr := string(lineData[0]), string(lineData[1])
		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		if cityData, ok := result[city]; ok {
			cityData[0] = min(cityData[0], temp)
			cityData[1] = max(cityData[1], temp)
			cityData[2] += temp
			cityData[3]++
		} else {
			result[city] = []float64{temp, temp, temp, 1}
		}
	}

	return result
}

func ParseLine(line []byte) (string, float64) {
	index := 0
	var (
		city string
		temp float64
		err  error
	)

	for i, char := range line {
		if char == ';' {
			index = i
			city = string(line[:i])
		}

		if char == '\n' {
			temp, err = strconv.ParseFloat(string(line[index+1:i]), 64)
			if err != nil {
				panic(err)
			}

			break
		}
	}

	return city, temp
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

func s1() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	fileReader := bufio.NewReader(file)
	chunkSize := 1024 * 1024 * 1024 // 50MB chunk size

	result := make(map[string][]float64)

	i := 0
	for {
		fmt.Println(i)
		i++

		tempChunk := make([]byte, chunkSize)
		readBytes, err := io.ReadFull(fileReader, tempChunk)

		fmt.Println(readBytes)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			if errors.Is(err, io.EOF) {
				break
			} else {
				panic(err)
			}
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

		MergeResult(result, ComputeChunk(tempChunk))
		// Process the chunk
		// Example: fmt.Println(len(chunk))
	}

	PrintResult(result)
}

func s2() {
	f, err := os.Create("profile.out")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	PrintTime("start program")

	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	fileReader := bufio.NewReader(file)
	chunkSize := 1024 * 1024 * 20 // 50MB chunk size

	chunkQueue := make(chan []byte, 500)
	resultQueue := make(chan map[string][]float64, 500)
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
