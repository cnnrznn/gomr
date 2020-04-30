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
	id := &Identity{}
	p := runtime.NumCPU()

	ins, out := gomr.RunLocal(p, p, id)
	gomr.TextFileParallel(os.Args[1], ins)

	for e := range out {
		fmt.Println(e)
	}
}
