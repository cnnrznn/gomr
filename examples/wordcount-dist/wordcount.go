package main

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/cnnrznn/gomr"
)

type WordCount struct{}

type Count struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
}

func (c Count) String() string {
	return fmt.Sprintf("%v %v", c.Key, c.Value)
}

func (w *WordCount) Map(in <-chan interface{}, out chan<- interface{}) {
	defer close(out)

	counts := make(map[string]int)

	for elem := range in {
		for _, word := range strings.Fields(elem.(string)) {
			counts[word]++
		}
	}

	for k, v := range counts {
		bs, err := json.Marshal(Count{k, v})
		if err != nil {
			log.Println(err)
			continue
		}
		out <- bs
	}
}

func (w *WordCount) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for elem := range in {
		bs := elem.([]byte)
		ct := Count{}
		if err := json.Unmarshal(bs, &ct); err != nil {
			log.Println(err)
			continue
		}

		key := ct.Key

		h := sha1.New()
		h.Write([]byte(key))
		hash := int(binary.BigEndian.Uint64(h.Sum(nil)))
		if hash < 0 {
			hash = hash * -1
		}

		outs[hash%len(outs)] <- elem
	}
}

func (w *WordCount) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	counts := make(map[string]int)

	for elem := range in {
		bs := elem.([]byte)
		ct := Count{}
		json.Unmarshal(bs, &ct)
		counts[ct.Key] += ct.Value
	}

	for k, v := range counts {
		out <- Count{k, v}
	}
}

func main() {
	wc := &WordCount{}
	gomr.RunDistributed(wc)
}
