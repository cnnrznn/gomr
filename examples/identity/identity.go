package main

import (
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/cnnrznn/gomr"
)

type Identity struct{}

func (i *Identity) Map(in <-chan interface{}, out chan<- interface{}) {
	for e := range in {
		out <- e
	}
	close(out)
}

func (i *Identity) Partition(in <-chan interface{}, out []chan interface{}, wg *sync.WaitGroup) {
	for e := range in {
		s := e.(string)
		if len(s) == 0 {
			s = " "
		}
		key := int(s[0])
		out[key%len(out)] <- e
	}
	wg.Done()
}

func (i *Identity) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	for e := range in {
		out <- e
	}
	wg.Done()
}

func main() {
	id := &Identity{}     // implements interfaces
	p := runtime.NumCPU() // number of mappers and reducers

	ins, out := gomr.RunLocal(p, p, id)    // architect pipeline
	gomr.TextFileParallel(os.Args[1], ins) // feed input to pipeline

	for e := range out {
		fmt.Println(e) // collect results
	}
}
