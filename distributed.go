package gomr

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
)

type worker struct {
	id            int
	role          int
	ncpu          int
	input, output string
	job           Job
	nmappers      int
	reducers      []string
}

func (w *worker) runMapper() {
	nRed := len(w.reducers)
	inMap := make([]chan interface{}, w.ncpu)
	inPar := make([]chan interface{}, w.ncpu)
	inRed := make([]chan interface{}, nRed)

	var wgPar, wgShuf sync.WaitGroup
	wgPar.Add(w.ncpu)
	wgShuf.Add(nRed)
	defer wgShuf.Wait()

	for i := 0; i < nRed; i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go w.shuffle(i, inRed[i], &wgShuf)
	}

	for i := 0; i < w.ncpu; i++ {
		inMap[i] = make(chan interface{}, CHANBUF)
		inPar[i] = make(chan interface{}, CHANBUF)
		go w.job.Partition(inPar[i], inRed, &wgPar)
		go w.job.Map(inMap[i], inPar[i])
	}

	go func() {
		wgPar.Wait()
		for i := 0; i < nRed; i++ {
			close(inRed[i])
		}
		log.Println("Map and partition done.")
	}()

	TextFileParallel(w.input, inMap)
}

func (w *worker) shuffle(i int, inRed chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	dst := w.reducers[i]
	client := newClient(dst)
	defer client.close()

	for item := range inRed {
		client.transmit(item.([]byte))
	}
}

func (w *worker) runReducer() {
	server := newServer(
		w.reducers[w.id],
		w.nmappers,
	)

	fromNet := server.serve()
	inRed := make([]chan interface{}, 10*w.ncpu)
	outRed := make(chan interface{}, CHANBUF)

	var wgPar, wgRed sync.WaitGroup
	wgPar.Add(1)
	wgRed.Add(len(inRed))

	for i := 0; i < len(inRed); i++ {
		inRed[i] = make(chan interface{}, CHANBUF)
		go w.job.Reduce(inRed[i], outRed, &wgRed)
	}

	go w.job.Partition(fromNet, inRed, &wgPar)

	go func() {
		wgPar.Wait()
		for _, ch := range inRed {
			close(ch)
		}

		wgRed.Wait()
		close(outRed)
	}()

	f, err := os.Create(w.output)
	if err != nil {
		log.Panic(err)
	}
	defer f.Close()

	wr := bufio.NewWriter(f)
	defer wr.Flush()

	for item := range outRed {
		_, err := fmt.Fprintln(wr, item)
		if err != nil {
			log.Panic(err)
		}
	}
}

// RunDistributed executes a Mapper or Reducer process in a distributed
// environment.
func RunDistributed(job Job) {
	w := worker{}
	ncpu := runtime.NumCPU()
	id := flag.Int("id", 0, "What is the reducer id of the worker?")
	role := flag.Int("role", MAPPER, "What is the role of this worker")
	input := flag.String("input", "input.txt", "Path to input file")
	output := flag.String("output", "output.txt", "Path to output file")
	nmappers := flag.Int("nmappers", 1, "The number of mappers")
	reducers := flag.String("reducers", "localhost:3000", "A comma seperated list of reducer ports")
	flag.Parse()

	w.ncpu = ncpu
	w.id = *id
	w.role = *role
	w.input = *input
	w.output = *output
	w.job = job
	w.nmappers = *nmappers
	w.reducers = []string{}

	for _, red := range strings.Split(*reducers, ",") {
		w.reducers = append(w.reducers, red)
	}

	log.Printf("%+v\n", w)

	switch *role {
	case MAPPER:
		w.runMapper()
	case REDUCER:
		w.runReducer()
	}
}
