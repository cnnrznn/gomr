package gomr

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"runtime"
	"sync"
)

const (
	CHANBUF = 4096
	FILEBUF = 1024 * 1024 * 32

	MAPPER  = 0
	REDUCER = 1
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
func RunLocal(nMap, nRed int, m Mapper, p Partitioner, r Reducer) (inMap []chan interface{},
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

type Worker struct {
	config      map[string]interface{}
	role        int
	ncpu        int
	input       string
	mapper      Mapper
	partitioner Partitioner
	reducer     Reducer
}

func (w *Worker) RunMap() {
	inMap := make([]chan interface{}, w.ncpu)
	inPar := make([]chan interface{}, w.ncpu)
	inRed := make([]chan interface{}, w.ncpu)

	var wgMap, wgPar sync.WaitGroup
	wgMap.Add(w.ncpu)
	wgPar.Add(w.ncpu)
	defer wgPar.Wait()

	for i := 0; i < w.ncpu; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go w.Shuffle(inRed[i], &wgPar)
	}

	for i := 0; i < w.ncpu; i++ {
		inMap[i] = make(chan interface{}, CHANBUF)
		inPar[i] = make(chan interface{}, CHANBUF)
		go w.mapper.Map(inMap[i], inPar[i])
		go w.partitioner.Partition(inPar[i], inRed, &wgMap)
	}

	go func() {
		wgMap.Wait()
		for i := 0; i < w.ncpu; i++ {
			close(inRed[i])
		}
		log.Println("Map done.")
	}()

	TextFileParallel(w.input, inMap)
}

func (w *Worker) Shuffle(inPar chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for _ = range inPar {
		// do nothing!
	}
}

func (w *Worker) RunRed() {

}

func Run(m Mapper, p Partitioner, r Reducer) {
	// 1. Parse command-line
	// 2. Am I running as a Mapper or Reducer?
	// 3. Mapper: read file parallel and partition,
	// 4. Reducer:

	w := Worker{}

	role := flag.Int("role", MAPPER, "What is the role of this worker")
	input := flag.String("input", "input.txt", "Path to input file")
	configFn := flag.String("conf", "config.json", "Path to a config file for GoMR")
	flag.Parse()

	ncpu := runtime.NumCPU()

	// open and parse configFn
	config := make(map[string]interface{})
	data, _ := ioutil.ReadFile(*configFn)
	json.Unmarshal(data, &config)

	w.config = config
	w.ncpu = ncpu
	w.role = *role
	w.input = *input
	w.mapper = m
	w.partitioner = p
	w.reducer = r

	switch *role {
	case MAPPER:
		w.RunMap()
	case REDUCER:
		w.RunRed()
	}
}
