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

	MAPPER = iota
	REDUCER
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

type Job interface {
	Mapper
	Partitioner
	Reducer
}

type Keyer interface {
	Key() interface{}
}

type localShuffle struct {
	mux     sync.Mutex
	wg      sync.WaitGroup
	reducer Reducer
}

func (ls *localShuffle) shuffle(inRed, outRed chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	reducers := make(map[interface{}]chan interface{})

	for item := range inRed {
		key := item.(Keyer).Key()
		if _, ok := reducers[key]; !ok {
			ls.wg.Add(1)
			reducers[key] = make(chan interface{}, CHANBUF)
			go ls.reducer.Reduce(reducers[key], outRed, &ls.wg)
		}
		reducers[key] <- item
	}

	for _, v := range reducers {
		close(v)
	}

}

/*
Architect and MapReduce Job a dynamic number of mappers and reducers. Return
to the user a channel for intputing their data
*/
func RunLocalDynamic(m Mapper, p Partitioner, r Reducer) (inMap []chan interface{},
	outRed chan interface{}) {
	nCpu := runtime.NumCPU()

	inMap = make([]chan interface{}, nCpu)   // number of mappers == cpus
	inPar := make([]chan interface{}, nCpu)  // number of partitioners == nMap
	inRed := make([]chan interface{}, nCpu)  // number of reducer input == cpus
	outRed = make(chan interface{}, CHANBUF) // single output channel

	ls := localShuffle{ // reducer-generator
		reducer: r,
	}

	var wgPar, wgShuf sync.WaitGroup
	wgPar.Add(nCpu)
	wgShuf.Add(nCpu)

	for i := 0; i < nCpu; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go ls.shuffle(inRed[i], outRed, &wgShuf)
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

		ls.wg.Wait()
		log.Println("Reduce done.")

		close(outRed)
	}()

	return
}

/*
Architect and MapReduce Job with the following number of mappers and
reducers. Return to the user a channel for intputing their data
*/
func RunLocal(nMap, nRed int, j Job) (inMap []chan interface{},
	outRed chan interface{}) {

	inMap = make([]chan interface{}, nMap)
	inPar := make([]chan interface{}, nMap)
	inRed := make([]chan interface{}, nRed)
	outRed = make(chan interface{}, CHANBUF)

	var wgPar, wgRed sync.WaitGroup
	wgPar.Add(nMap)
	wgRed.Add(nRed)

	for i := 0; i < nRed; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go j.Reduce(inRed[i], outRed, &wgRed)
	}

	for i := 0; i < nMap; i++ {
		inMap[i] = make(chan interface{}, CHANBUF)
		inPar[i] = make(chan interface{}, CHANBUF)
		go j.Map(inMap[i], inPar[i])
		go j.Partition(inPar[i], inRed, &wgPar)
	}

	go func() {
		wgPar.Wait()
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

type worker struct {
	config      map[string]interface{}
	id          int
	role        int
	ncpu        int
	input       string
	mapper      Mapper
	partitioner Partitioner
	reducer     Reducer
}

func (w *worker) runMapper() {
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
		go w.shuffle(i, inRed[i], &wgPar)
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

func (w *worker) shuffle(i int, inRed chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	dst := w.config["workers"].([]interface{})[i].(string)
	pipe := NewPipe(dst)
	defer pipe.Close()

	for item := range inRed {
		pipe.Transmit(item)
	}
}

func (w *worker) runReducer() {
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

/*
Run a Mapper or Reducer process in a distributed environment.
*/
func RunDistributed(m Mapper, p Partitioner, r Reducer) {
	w := worker{}
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
		w.runMapper()
	case REDUCER:
		w.runReducer()
	}
}
