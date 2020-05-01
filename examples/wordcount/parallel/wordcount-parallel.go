package main

import (
	//"crypto/sha1"
	//"encoding/binary"
	"fmt"
	"github.com/cnnrznn/gomr"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type WordCount struct{}

type Count struct {
	key   string
	Value int
}

func (c Count) Key() interface{} {
	return c.key
}

func (c Count) String() string {
	return fmt.Sprintf("%v %v", c.key, c.Value)
}

func (w *WordCount) Map(in <-chan interface{}, out chan<- interface{}) {
	counts := make(map[string]int)

	for elem := range in {
		for _, word := range strings.Split(elem.(string), " ") {
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
		word := elem.(Count).key
		key := 0
		if len(word) > 0 {
			key = int(word[0])
		}
		outs[key%len(outs)] <- elem
	}

	wg.Done()
}

func (w *WordCount) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	counts := make(map[string]int)

	for elem := range in {
		ct := elem.(Count)
		counts[ct.key] += ct.Value
	}

	for k, v := range counts {
		out <- Count{k, v}
	}

	wg.Done()
}

func main() {
	// Uncomment next line to show debug info
	log.SetOutput(ioutil.Discard)
	wc := &WordCount{}

	// Uncomment next two lines for static
	par, _ := strconv.Atoi(os.Args[2])
	runtime.GOMAXPROCS(par)
	//runtime.GOMAXPROCS(0)
	ins, out := gomr.RunLocal(par, par, wc)

	// Comment next line for static
	//ins, out := gomr.RunLocalDynamic(wc, wc, wc)

	gomr.TextFileParallel(os.Args[1], ins)

	for count := range out {
		fmt.Println(count)
	}
}
