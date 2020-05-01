package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/cnnrznn/gomr"
)

type Pagerank struct {
	g map[int][]int
}

type Contrib struct {
	key int
	val float64
}

func NewPagerank(fn string) *Pagerank {
	g := make(map[int][]int)

	f, err := os.Open(fn)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		ls := strings.Split(scanner.Text(), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])
		g[v1] = append(g[v1], v2)
	}

	return &Pagerank{g: g}
}

func (pr *Pagerank) Map(in <-chan interface{}, out chan<- interface{}) {
	for e := range in {
		ls := strings.Split(e.(string), " ")
		v, _ := strconv.Atoi(ls[0])
		rank, _ := strconv.ParseFloat(ls[1], 64)
		size := len(pr.g[v])
		for _, peer := range pr.g[v] {
			out <- Contrib{
				key: peer,
				val: rank / float64(size),
			}
		}
	}
	close(out)
}

func (pr *Pagerank) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	for e := range in {
		key := e.(Contrib).key
		outs[key%len(outs)] <- e
	}
	wg.Done()
}

func (pr *Pagerank) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	newRanks := make(map[int]float64)
	for e := range in {
		contrib := e.(Contrib)
		newRanks[contrib.key] += contrib.val
	}
	for k, v := range newRanks {
		out <- fmt.Sprintf("%v %v", k, v)
	}
	wg.Done()
}

func main() {
	pr := NewPagerank(os.Args[1])

	p := runtime.NumCPU()
	ins, out := gomr.RunLocal(p, p, pr)
	gomr.TextFileParallel(os.Args[2], ins)

	for e := range out {
		fmt.Println(e)
	}
}
