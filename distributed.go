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

type worker struct {
	config map[string]interface{}
	id     int
	role   int
	ncpu   int
	input  string
	job    Job
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
		go w.job.Partition(inPar[i], inRed, &wgMap)
		go w.job.Map(inMap[i], inPar[i])
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
			go w.job.Reduce(ch, outRed, &wgRed)
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
func RunDistributed(job Job) {
	w := worker{}
	ncpu := runtime.NumCPU()
	id := flag.Int("id", 0, "What is the reducer id of the worker?")
	role := flag.Int("role", MAPPER, "What is the role of this worker")
	input := flag.String("input", "input.txt", "Path to input file")
	configFn := flag.String("conf", "config.json", "Path to a config file for GoMR")
	flag.Parse()

	// open and parse configFn
	config := make(map[string]interface{})
	data, err := ioutil.ReadFile(*configFn)
	if err != nil {
		log.Fatal("Unable to read config file: ", err)
	}
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatal(err)
	}

	w.config = config
	w.ncpu = ncpu
	w.id = *id
	w.role = *role
	w.input = *input
	w.job = job

	switch *role {
	case MAPPER:
		w.runMapper()
	case REDUCER:
		w.runReducer()
	}
}
