package gomr

import (
	"bufio"
	"io"
	"log"
	"math"
	"os"
)

// TextFileSerial Open a file and read lines from the file
// into a single input channel
func TextFileSerial(fn string, inMap chan interface{}) {
	file, _ := os.Open(fn)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		inMap <- scanner.Text()
	}

	close(inMap)
}

// TextFileMultiplex Read lines from file and multiplex between
// multiple intput channels
func TextFileMultiplex(fn string, inMap []chan interface{}) {
	par := len(inMap)

	file, _ := os.Open(fn)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for d := 0; scanner.Scan(); d = (d + 1) % par {
		inMap[d] <- scanner.Text()
	}

	for _, ch := range inMap {
		close(ch)
	}
}

// TextFileParallel Read from file in parallel. Split file into
// chunks based on the number of inMap channels and read input
// by lines.
func TextFileParallel(fn string, inMap []chan interface{}) {
	file, err := os.Open(fn)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	size := stat.Size()
	nChunks := len(inMap)
	chunkSize := int64(math.Ceil(float64(size) / float64(nChunks)))

	for i := 0; i < nChunks; i++ {
		go func(i int) {
			buffer := make([]byte, FILEBUF)
			atEOF := false
			skippedFirst := false

			start := chunkSize * int64(i)
			end := start + chunkSize
			bufstart, bufend := 0, 0
			log.Println(i, start, end)

			file, _ := os.Open(fn)
			defer file.Close()

			pos, err := file.Seek(start, 0)
			if err != nil || pos != start {
				log.Fatal(pos, err)
			}

			for start <= end && !atEOF {
				copy(buffer, buffer[bufstart:bufend])
				bufend -= bufstart

				n, err := file.Read(buffer[bufend:])
				if err != nil {
					if err == io.EOF {
						atEOF = true
					} else {
						log.Fatal(err)
					}
				}

				bufstart = 0
				bufend += n

				for start <= end {
					advance, token, err := bufio.ScanLines(buffer[bufstart:bufend], atEOF)
					if err != nil {
						log.Fatal(err)
					}

					if advance == 0 {
						break
					}

					bufstart += advance
					start += int64(advance)

					if i == 0 || skippedFirst {
						inMap[i] <- string(token)
					}
					skippedFirst = true
				}
			}

			close(inMap[i])
		}(i)
	}
}
