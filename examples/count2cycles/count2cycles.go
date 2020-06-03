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

	"github.com/cnnrznn/gomr"
	. "github.com/cnnrznn/gomr/examples/edge"
)

const (
	nMap = 10
	nRed = 10
)

type EdgeToTables struct {
	edges map[Edge]bool
}

func (e *EdgeToTables) Map(in <-chan interface{}, out chan<- interface{}) {
	for elem := range in {
		edge := elem.(Edge)
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
		je := elem.(JoinEdge)
		jes = append(jes, je)
	}

	log.Println("Begin sorting")
	sort.Sort(ByKeyThenTable(jes))
	log.Println("End sorting")

	numTwoCycles := 0
	lastSeen := -1
	arr := []Edge{}

	for _, je := range jes {
		if je.JoinKey != lastSeen {
			arr = nil
			lastSeen = je.JoinKey
		}

		if je.Table == "e1" {
			arr = append(arr, je.Edge)
		} else { // "e2"
			for _, e1 := range arr {
				if e1.Fr == je.Edge.To {
					numTwoCycles++
				}
			}
		}
	}

	out <- numTwoCycles
	wg.Done()
}

func main() {
	log.Println("Spinning up...")

	edges := make(map[Edge]bool)
	e2t := &EdgeToTables{edges}

	inMap, outRed := gomr.RunLocal(10, 100, e2t)

	// Read edges file and populate map
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal("Couldn't find input file", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for i := 0; scanner.Scan(); i++ {
		ls := strings.Split(scanner.Text(), ",")
		v1, _ := strconv.Atoi(ls[0])
		v2, _ := strconv.Atoi(ls[1])

		inMap[i%len(inMap)] <- Edge{v1, v2}
	}

	for i := 0; i < len(inMap); i++ {
		close(inMap[i])
	}

	numTwoCycles := 0

	for result := range outRed {
		numTwoCycles += result.(int)
	}

	fmt.Println(numTwoCycles)
}
