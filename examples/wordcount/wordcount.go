package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/cnnrznn/gomr"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
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
	for elem := range in {
		for _, word := range strings.Split(elem.(string), " ") {
			out <- word
		}
	}

	close(out)
}

func (w *WordCount) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	for elem := range in {
		key := elem.(string)

		h := sha1.New()
		h.Write([]byte(key))
		hash := int(binary.BigEndian.Uint64(h.Sum(nil)))
		if hash < 0 {
			hash = hash * -1
		}

		outs[hash%len(outs)] <- key
	}

	wg.Done()
}

func (w *WordCount) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	counts := make(map[string]int)

	for elem := range in {
		key := elem.(string)
		counts[key]++
	}

	for k, v := range counts {
		out <- Count{k, v}
	}

	wg.Done()
}

func main() {
	log.SetOutput(ioutil.Discard)
	wc := &WordCount{}
	par, _ := strconv.Atoi(os.Args[2])

	ins, out := gomr.Run(par, par, wc, wc, wc)
	gomr.TextFileParallel(os.Args[1], ins)

	for count := range out {
		//for _ = range out {
		fmt.Println(count)
	}

	log.Println("Wordcount done!")
}
