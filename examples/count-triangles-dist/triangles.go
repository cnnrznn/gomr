package main

import (
	//"fmt"
	"encoding/json"
	"log"
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
	for elem := range in {
		ls := strings.Split(elem.(string), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])
		edge := Edge{v1, v2}
		if edge.Fr < edge.To {
			out <- JoinEdge{edge.To, "e1", edge}
		}
		out <- JoinEdge{edge.Fr, "e2", edge}
	}
	close(out)
}

func (e *EdgeToTables) Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup) {
	for elem := range in {
		je := elem.(JoinEdge)
		outs[je.JoinKey%len(outs)] <- je
	}

	wg.Done()
}

func (e *EdgeToTables) Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	jes := []JoinEdge{}

	for elem := range in {
		bs := elem.([]byte)
		je := JoinEdge{}
		err := json.Unmarshal(bs, &je)
		if err != nil {
			log.Println("Error unmarshal:", err)
		}
		jes = append(jes, je)
	}

	log.Println("Begin sorting")
	sort.Sort(ByKeyThenTable(jes))
	log.Println("End sorting")
	log.Println(jes)

	numTriangles := 0
	lastSeen := -1
	arr := []Edge{}

	for _, je := range jes {
		if je.JoinKey != lastSeen {
			arr = nil
			lastSeen = je.JoinKey
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
	edges := make(map[Edge]bool)
	e2t := &EdgeToTables{edges, &sync.Mutex{}}
	edgeCh := make(chan interface{}, 4096)
	gomr.TextFileParallel("test.csv", []chan interface{}{edgeCh})
	log.Println("Start Edge map build")
	for elem := range edgeCh {
		ls := strings.Split(elem.(string), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])
		if v1 > v2 {
			e2t.edges[Edge{v1, v2}] = true
		}
	}
	log.Println("Done Edge map build")

	gomr.RunDistributed(e2t)
}
