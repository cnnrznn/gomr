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

/*
Holds an in-memory representation of the graph and implements Job
interface
*/
type Pagerank struct {
	g map[int][]int
}

/*
Represents a contribution to a pages rank
*/
type Contrib struct {
	key int
	val float64
}

/*
Load the graph into memory as a map from a node (int) to a list
of its outgoing links ([]int).
*/
func NewPagerank(fn string) *Pagerank {
	g := make(map[int][]int)

	f, err := os.Open(fn)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		ls := strings.Split(scanner.Text(), "\t")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])
		g[v1] = append(g[v1], v2)
	}

	return &Pagerank{g: g}
}

func (pr *Pagerank) Map(in <-chan interface{}, out chan<- interface{}) {
	defer close(out)

	var sink float64 = 0.0

	for e := range in {
		ls := strings.Fields(e.(string))
		node, _ := strconv.Atoi(ls[0])
		rank, _ := strconv.ParseFloat(ls[1], 64)

		// always preserve this node
		out <- Contrib{key: node, val: 0}

		// if this node has outgoing
		for _, peer := range pr.g[node] {
			out <- Contrib{
				key: peer,
				val: rank / float64(len(pr.g[node])),
			}
		}

		if _, ok := pr.g[node]; !ok {
			sink += rank
		}
	}

	// disseminate "sink" contribution
	contrib := sink / float64(len(pr.g))
	for k := range pr.g {
		out <- Contrib{key: k, val: contrib}
	}
}

func (pr *Pagerank) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for e := range in {
		key := e.(Contrib).key
		outs[key%len(outs)] <- e
	}
}

func (pr *Pagerank) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	newRanks := make(map[int]float64)
	for e := range in {
		contrib := e.(Contrib)
		newRanks[contrib.key] += contrib.val
	}
	for k, v := range newRanks {
		v = (0.85 * v) + 0.15
		out <- fmt.Sprintf("%v %v", k, v)
	}
}

func main() {
	pr := NewPagerank(os.Args[1])

	p := runtime.NumCPU()
	ins, out := gomr.RunLocal(p, p, pr)
	gomr.TextFileParallel(os.Args[2], ins)

	for i := 0; i < 10; i++ {
		insNext, outNext := gomr.RunLocal(p, p, pr)
		gomr.Chain(out, insNext)
		out = outNext
	}

	for e := range out {
		fmt.Println(e)
	}
}
