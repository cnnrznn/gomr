package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/cnnrznn/gomr"
)

type WordCount struct{}

type Count struct {
	Key   string
	Value int
}

func (w *WordCount) Map(in <-chan interface{}, out chan<- interface{}) {
	for elem := range in {
		out <- elem
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

		outs[hash%len(outs)] <- count
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
	data := "hello world\nthis is a body of text\ntext world hello a boy"
	wc := &WordCount{}

	fmt.Println(data)

	in, out := gomr.Run(5, 5, wc, wc, wc)

	for _, word := range strings.Split(data, " ") {
		in <- word
	}
	close(in)

	for count := range out {
		fmt.Println(count)
	}
}
