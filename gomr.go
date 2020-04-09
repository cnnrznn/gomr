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

type Keyer interface {
	Key() interface{}
}

type LocalShuffle struct {
	reducers map[interface{}]chan interface{}
	mux      sync.Mutex
	wg       sync.WaitGroup
	reducer  Reducer
}

func (ls *LocalShuffle) Shuffle(inRed, outRed chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for item := range inRed {
		key := item.(Keyer).Key()
		ls.mux.Lock()
		if _, ok := ls.reducers[key]; !ok {
			ls.wg.Add(1)
			ls.reducers[key] = make(chan interface{}, CHANBUF)
			go ls.reducer.Reduce(ls.reducers[key], outRed, &ls.wg)

		}
		ls.mux.Unlock()
		ls.reducers[key] <- item
	}
}

/*
 * Architect and MapReduce Job with the following number of mappers and
 * reducers. Return to the user a channel for intputing their data
 */
func RunLocalDynamic(m Mapper, p Partitioner, r Reducer) (inMap []chan interface{},
	outRed chan interface{}) {
	log.Println("Architecting...")

	nCpu := runtime.NumCPU()

	inMap = make([]chan interface{}, nCpu)   // number of mappers == cpus
	inPar := make([]chan interface{}, nCpu)  // number of partitioners == nMap
	inRed := make([]chan interface{}, nCpu)  // number of reducer input == cpus
	outRed = make(chan interface{}, CHANBUF) // single output channel

	localShuffle := LocalShuffle{ // reducer-generator
		reducers: make(map[interface{}]chan interface{}),
		reducer:  r,
	}

	var wgPar, wgShuf sync.WaitGroup
	wgPar.Add(nCpu)
	wgShuf.Add(nCpu)

	for i := 0; i < nCpu; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go localShuffle.Shuffle(inRed[i], outRed, &wgShuf)
	}

	for i := 0; i < nCpu; i++ {
		inMap[i] = make(chan interface{}, CHANBUF)
		inPar[i] = make(chan interface{}, CHANBUF)
		go p.Partition(inPar[i], inRed, &wgPar)
		go m.Map(inMap[i], inPar[i])
	}

	go func() {
		wgPar.Wait()
		log.Println("Map and Partition done.")

		for i := 0; i < nCpu; i++ {
			close(inRed[i])
		}

		wgShuf.Wait()
		log.Println("Shuffle done.")

		for _, v := range localShuffle.reducers {
			close(v)
		}

		localShuffle.wg.Wait()
		log.Println("Reduce done.")

		close(outRed)
	}()

	return
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
	inRed := make(map[interface{}]chan interface{})
	outRed := make(chan interface{}, CHANBUF)
	var wg, wgRed sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()
		for out := range outRed {
			fmt.Println(out)
		}
	}()

	for bs := range fromNet {
		m := make(map[string]interface{})
		err := json.Unmarshal(bs, &m)
		if err != nil {
			log.Println("Error unmarshal:", err)
		}

		if _, ok := inRed[m["key"]]; !ok {
			ch := make(chan interface{}, CHANBUF)
			inRed[m["key"]] = ch
			wgRed.Add(1)
			go w.reducer.Reduce(ch, outRed, &wgRed)
		}
		inRed[m["key"]] <- bs
	}

	for _, v := range inRed {
		close(v)
	}
	wgRed.Wait()
	close(outRed)
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
