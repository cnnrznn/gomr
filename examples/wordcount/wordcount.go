package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/cnnrznn/gomr"
)

type WordCount struct{}

type Count struct {
	Key   string
	Value int
}

func (c Count) String() string {
	return fmt.Sprintf("%v %v", c.Key, c.Value)
}

func (w *WordCount) Map(in <-chan interface{}, out chan<- interface{}) {
	counts := make(map[string]int)

	for elem := range in {
		for _, word := range strings.Fields(elem.(string)) {
			counts[word]++
		}
	}

	for k, v := range counts {
		out <- Count{k, v}
	}

	close(out)
}

func (w *WordCount) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	for elem := range in {
		key := elem.(Count).Key

		h := sha1.New()
		h.Write([]byte(key))
		hash := int(binary.BigEndian.Uint64(h.Sum(nil)))
		if hash < 0 {
			hash = hash * -1
		}

		outs[hash%len(outs)] <- elem
	}

	wg.Done()
}

func (w *WordCount) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	counts := make(map[string]int)

	for elem := range in {
		ct := elem.(Count)
		counts[ct.Key] += ct.Value
	}

	for k, v := range counts {
		out <- Count{k, v}
	}

	wg.Done()
}

func main() {
	wc := &WordCount{}

	nprocs := runtime.NumCPU()
	ins, out := gomr.RunLocal(nprocs, nprocs, wc)

	gomr.TextFileParallel(os.Args[1], ins)

	for count := range out {
		fmt.Println(count)
	}
}
