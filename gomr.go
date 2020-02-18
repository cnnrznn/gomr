package gomr

import (
	"bufio"
	"log"
	"math"
	"os"
	"sync"
)

const (
	CHANBUF = 4096
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
	chunkSize := int(math.Ceil(float64(size) / float64(nChunks)))

	for i := 0; i < nChunks; i++ {
		go func(i int) {
			start := chunkSize * i
			var end int64 = int64(start + chunkSize)
			var pos int64 = int64(start)
			log.Println(i, start, end)

			file, _ := os.Open(fn)
			defer file.Close()
			_, err := file.Seek(int64(chunkSize*i), 0)
			if err != nil {
				log.Println(err)
			}

			scanner := bufio.NewScanner(file)
			if i > 0 {
				scanner.Scan()
				pos, _ = file.Seek(0, 1)
			}

			for pos <= end && scanner.Scan() {
				pos, _ = file.Seek(0, 1)

				inMap[i] <- scanner.Text()
				log.Println(i, scanner.Text())
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
