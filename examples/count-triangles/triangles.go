package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cnnrznn/gomr"
	. "github.com/cnnrznn/gomr/examples/edge"
)

type EdgeToTables struct {
	edges map[Edge]bool
	mux   *sync.Mutex
}

func (e *EdgeToTables) Map(in <-chan interface{}, out chan<- interface{}) {
	localMap := make(map[Edge]bool)

	for elem := range in {
		ls := strings.Split(elem.(string), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1]) // Read edges file and populate map
		edge := Edge{v1, v2}

		m := 20000
		if v1 >= m || v2 >= m {
			continue
		}

		if v1 > v2 {
			localMap[edge] = true
		}

		if edge.Fr < edge.To {
			out <- JoinEdge{edge.To, "e1", edge}
		}
		out <- JoinEdge{edge.Fr, "e2", edge}
	}

	e.mux.Lock()
	for k, v := range localMap {
		e.edges[k] = v
	}
	e.mux.Unlock()

	close(out)
}

func (e *EdgeToTables) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	for elem := range in {
		je := elem.(JoinEdge)
		outs[je.Key%len(outs)] <- je
	}

	wg.Done()
}

func (e *EdgeToTables) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	jes := []JoinEdge{}

	for elem := range in {
		je := elem.(JoinEdge)
		jes = append(jes, je)
	}

	log.Println("Begin sorting")
	sort.Sort(ByKeyThenTable(jes))
	log.Println("End sorting")

	numTriangles := 0
	lastSeen := -1
	arr := []Edge{}

	for _, je := range jes {
		if je.Key != lastSeen {
			arr = nil
			lastSeen = je.Key
		}

		if je.Table == "e1" {
			arr = append(arr, je.Edge)
		} else {
			for _, e1 := range arr {
				if e1.Fr < je.Edge.To {
					if _, ok := e.edges[Edge{je.Edge.To, e1.Fr}]; ok {
						numTriangles++
					}
				}
			}
		}
	}

	out <- numTriangles
	wg.Done()
}

func main() {
	log.Println("Spinning up...")

	nMap, _ := strconv.Atoi(os.Args[2])
	nRed, _ := strconv.Atoi(os.Args[3])
	edges := make(map[Edge]bool)
	e2t := &EdgeToTables{edges, &sync.Mutex{}}
	inMap, outRed := gomr.Run(nMap, nRed, e2t, e2t, e2t)

	gomr.TextFileParallel(os.Args[1], inMap)

	numTriangles := 0

	for result := range outRed {
		numTriangles += result.(int)
	}

	fmt.Println(numTriangles)
}
