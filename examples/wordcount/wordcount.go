package main

import (
	"crypto/sha1"
	"encoding/binary"
	//"fmt"
	"log"
	"os"
	"strconv"
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
	wc := &WordCount{}
	par, _ := strconv.Atoi(os.Args[2])

	ins, out := gomr.Run(par, par, wc, wc, wc)
	gomr.TextFile(os.Args[1], ins)

	//file, err := os.Open(os.Args[1])
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer file.Close()

	//scanner := bufio.NewScanner(file)
	//log.Println("Started scanning")
	//for scanner.Scan() {
	//	in <- scanner.Text()
	//}
	//log.Println("Finished scanning")
	//close(in)

	for _ = range out {
		//fmt.Println(count)
	}

	log.Println("Wordcount done!")
}
