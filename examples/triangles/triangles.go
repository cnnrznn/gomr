package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	. "github.com/cnnrznn/cs6240/triangles/edge"
	"github.com/cnnrznn/gomr"
)

const (
	nMap = 10
	nRed = 10
)

type EdgeToTables struct {
	edges map[Edge]bool
}

func (e *EdgeToTables) Map(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	for elem := range in {
		edge := elem.(Edge)
		//fmt.Println(edge)
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
		//fmt.Println(je, je.Key%len(outs))
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

	//fmt.Println(jes)

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
				if e1.Fr < je.Edge.To && e1.Fr < je.Edge.Fr {
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

	edges := make(map[Edge]bool)
	e2t := &EdgeToTables{edges}

	inMap, outRed := gomr.Run(10, 100, e2t, e2t, e2t)

	// Read edges file and populate map
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal("Couldn't find input file", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ls := strings.Split(scanner.Text(), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])

		inMap <- Edge{v1, v2}
		if v1 > v2 {
			edges[Edge{v1, v2}] = true
		}
	}

	close(inMap)

	numTriangles := 0

	for result := range outRed {
		numTriangles += result.(int)
	}

	fmt.Println(numTriangles)
}
