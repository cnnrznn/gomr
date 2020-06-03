package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/cnnrznn/gomr"
)

type EdgeToCount struct {
}

type Count struct {
	key       int
	nIn, nOut int
}

func (e *EdgeToCount) Map(in <-chan interface{}, out chan<- interface{}) {
	for elem := range in {
		ls := strings.Split(elem.(string), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])

		out <- Count{v1, 0, 1}
		out <- Count{v2, 1, 0}
	}

	close(out)
}

func (e *EdgeToCount) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	for elem := range in {
		ct := elem.(Count)
		outs[ct.key%len(outs)] <- ct
	}

	wg.Done()
}

func (e *EdgeToCount) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	numTwoPath := 0
	counts := make(map[int]Count)

	for elem := range in {
		ct := elem.(Count)
		newCount := counts[ct.key]
		newCount.nIn += ct.nIn
		newCount.nOut += ct.nOut
		counts[ct.key] = newCount
	}

	for _, ct := range counts {
		numTwoPath += (ct.nIn * ct.nOut)
	}

	out <- numTwoPath
	wg.Done()
}

func main() {
	log.Println("Spinning up...")

	e2t := &EdgeToCount{}

	inMap, outRed := gomr.RunLocal(10, 100, e2t)
	gomr.TextFileParallel(os.Args[1], inMap)

	// Read edges file and populate map
	numTwoPath := 0

	for result := range outRed {
		numTwoPath += result.(int)
	}

	fmt.Println(numTwoPath)
}
