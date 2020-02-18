package gomr

import (
	"log"
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
