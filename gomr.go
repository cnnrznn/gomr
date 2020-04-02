package gomr

import (
	"encoding/json"
	"flag"
	"fmt"
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
	id          int
	role        int
	ncpu        int
	input       string
	mapper      Mapper
	partitioner Partitioner
	reducer     Reducer
}

func (w *Worker) RunMap() {
	npeers := len(w.config["workers"].([]interface{}))
	inMap := make([]chan interface{}, w.ncpu)
	inPar := make([]chan interface{}, w.ncpu)
	inRed := make([]chan interface{}, npeers)

	var wgMap, wgPar sync.WaitGroup
	wgMap.Add(w.ncpu)
	wgPar.Add(npeers)
	defer wgPar.Wait()

	for i := 0; i < npeers; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go w.Shuffle(i, inRed[i], &wgPar)
	}

	for i := 0; i < w.ncpu; i++ {
		inMap[i] = make(chan interface{}, CHANBUF)
		inPar[i] = make(chan interface{}, CHANBUF)
		go w.partitioner.Partition(inPar[i], inRed, &wgMap)
		go w.mapper.Map(inMap[i], inPar[i])
	}

	go func() {
		wgMap.Wait()
		for i := 0; i < npeers; i++ {
			close(inRed[i])
		}
		log.Println("Map done.")
	}()

	TextFileParallel(w.input, inMap)
}

func (w *Worker) Shuffle(i int, inRed chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	dst := w.config["workers"].([]interface{})[i].(string)
	pipe := NewPipe(dst)
	defer pipe.Close()

	for item := range inRed {
		pipe.Transmit(item)
	}
}

func (w *Worker) RunRed() {
	server := NewServer(
		w.config["workers"].([]interface{})[w.id].(string),
		len(w.config["workers"].([]interface{})),
	)
	fromNet := server.Serve()
	//inRed := make(map[interface{}]chan interface{})
	//var wg sync.WaitGroup

	for bs := range fromNet {
		fmt.Println(bs)
		//m := make(map[string]interface{})
		//err := json.Unmarshal(bs, &m)
		//if err != nil {
		//	log.Println("Error unmarshal:", err)
		//}

		//if _, ok := inRed[m["key"]]; !ok {
		//	// create new goroutine
		//}
		//inRed[m["key"]] <- bs
	}
}

func Run(m Mapper, p Partitioner, r Reducer) {
	w := Worker{}
	ncpu := runtime.NumCPU()
	id := flag.Int("id", 0, "What is the reducer id of the worker?")
	role := flag.Int("role", MAPPER, "What is the role of this worker")
	input := flag.String("input", "input.txt", "Path to input file")
	configFn := flag.String("conf", "config.json", "Path to a config file for GoMR")
	flag.Parse()

	// open and parse configFn
	config := make(map[string]interface{})
	data, _ := ioutil.ReadFile(*configFn)
	json.Unmarshal(data, &config)

	w.config = config
	w.ncpu = ncpu
	w.id = *id
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
