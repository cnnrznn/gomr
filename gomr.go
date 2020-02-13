package gomr

import (
	"bufio"
	"log"
	//"math"
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

func TextFile(fn string, inMap []chan interface{}) {
	par := len(inMap)

	file, _ := os.Open(fn)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for dest := 0; scanner.Scan(); dest = (dest + 1) % par {
		inMap[dest] <- scanner.Text()
	}

	for _, ch := range inMap {
		close(ch)
	}

	//stat, _ := file.Stat()
	//size := stat.Size()
	//nChunks := len(inMap)
	//chunkSize := int(math.Ceil(float64(size) / float64(nChunks)))

	//for i := 0; i < nChunks; i++ {
	//	go func(i int) {
	//		file, _ := os.Open(fn)
	//		defer file.Close()
	//		_, err := file.Seek(int64(chunkSize*i), 0)
	//		if err != nil {
	//			log.Println(err)
	//		}

	//		scanner := bufio.NewScanner(file)
	//		if i > 0 {
	//			scanner.Scan()
	//		}

	//		nRead := 0
	//		for scanner.Scan() && nRead < chunkSize {
	//			inMap[i] <- scanner.Text()
	//			nRead += len(scanner.Text())
	//		}

	//		close(inMap[i])
	//		log.Printf("File scan %v done.\n", i)
	//	}(i)
	//}
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
	outRed = make(chan interface{})

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
