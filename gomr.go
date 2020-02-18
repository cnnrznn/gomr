package gomr

import (
	"bufio"
	"io"
	"log"
	"math"
	"os"
	"sync"
)

const (
	CHANBUF = 4096
	FILEBUF = 1024 * 1024 * 32
)

type Mapper interface {
	Map(in <-chan interface{}, out chan<- interface{})
}

type Partitioner interface {
	Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup)
}

type Reducer interface {
	Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup)
}

func TextFileSerial(fn string, inMap chan interface{}) {
	file, _ := os.Open(fn)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		inMap <- scanner.Text()
	}

	close(inMap)
}

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
						log.Fatal(err) // ScanLines doesn't throw an error ever in the source code
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

/*
 * Architect and MapReduce Job with the following number of mappers and
 * reducers. Return to the user a channel for intputing their data
 */
func Run(nMap, nRed int, m Mapper, p Partitioner, r Reducer) (inMap []chan interface{},
	outRed chan interface{}) {
	log.Println("Architecting...")

	inMap = make([]chan interface{}, nMap)
	inPar := make([]chan interface{}, nMap)
	inRed := make([]chan interface{}, nRed)
	outRed = make(chan interface{}, CHANBUF)

	var wgMap, wgRed sync.WaitGroup
	wgMap.Add(nMap)
	wgRed.Add(nRed)

	for i := 0; i < nRed; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go r.Reduce(inRed[i], outRed, &wgRed)
	}

	for i := 0; i < nMap; i++ {
		inMap[i] = make(chan interface{}, CHANBUF)
		inPar[i] = make(chan interface{}, CHANBUF)
		go m.Map(inMap[i], inPar[i])
		go p.Partition(inPar[i], inRed, &wgMap)
	}

	go func() {
		wgMap.Wait()
		for i := 0; i < nRed; i++ {
			close(inRed[i])
		}
		log.Println("Map done.")

		wgRed.Wait()
		close(outRed)
		log.Println("Reduce done.")
	}()

	return
}
