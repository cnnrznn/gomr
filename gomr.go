package gomr

import (
	"bytes"
	"log"
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

func TextFile(fn string, inMaps []chan interface{}) {
	file, _ := os.Open(fn)
	defer file.Close()
	size, _ := f.Stat().Size()
	nChunks := len(inMaps)
	chunkSize := math.Ceil(float64(size) / float64(nChunks))

	for i := 0; i < nChunks; i++ {
		go func() {
			file.Seek(chunkSize * i)
			buff := make([]byte, chunkSize)
			count, _ := file.Read(buff)
		}()
	}
}

/*
 * Architect and MapReduce Job with the following number of mappers and
 * reducers. Return to the user a channel for intputing their data
 */
func Run(nMap, nRed int, m Mapper, p Partitioner, r Reducer) (inMaps []chan interface{},
	outRed chan interface{}) {
	log.Println("Architecting...")

	inMaps = make([]chan interface{}, nMap)
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
		go m.Map(inMap, inPar[i])
		go p.Partition(inPar[i], inRed, &wgMap)
	}

	go func() {
		wgMap.Wait()
		for i := 0; i < nRed; i++ {
			close(inRed[i])
		}

		wgRed.Wait()
		close(outRed)
	}()

	return
}
