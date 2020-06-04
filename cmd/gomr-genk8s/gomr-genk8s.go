package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	nmap := flag.Int("nmap", 1, "The number of mappers")
	nred := flag.Int("nred", 1, "The number of reducers")
	inPrefix := flag.String(
		"inprefix",
		"input.txt",
		"The prefix of the input files")
	outPrefix := flag.String(
		"outprefix",
		"output.txt",
		"The prefix for the output files")
	name := flag.String(
		"name",
		"gomr",
		"The name for this job; must be unique for the k8s cluster")
	flag.Parse()

	config := make(map[string]interface{})
	reducers := make([]string, *nred)
	config["nmappers"] = *nmap
	config["nreducers"] = *nred
	for i := 1; i <= *nred; i++ {
		reducers[i-1] = fmt.Sprintf("%v-reducer-%v:3000", *name, i)
	}
	config["reducers"] = reducers
	config["inprefix"] = *inPrefix
	config["outprefix"] = *outPrefix
	config["name"] = *name

	bs, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Panic(err)
	}

	if err := ioutil.WriteFile("config.json", bs, 0644); err != nil {
		log.Panic(err)
	}

	executeTemplate(mapperJobStr, "mappers.job.yaml", config)
	executeTemplate(reducerJobStr, "reducers.job.yaml", config)
	executeTemplate(reducerServiceStr, "reducers.service.yaml", config)
}

func executeTemplate(tl, outf string, config map[string]interface{}) {
	templ, err := template.New("easter egg").
		Funcs(template.FuncMap{
			"iter": iterate,
			"dec":  func(i int) int { return i - 1 },
		}).
		Parse(tl)
	if err != nil {
		log.Panic(err)
	}

	f, err := os.Create(outf)
	if err != nil {
		log.Panic(err)
	}
	w := bufio.NewWriter(f)
	defer w.Flush()

	if err := templ.Execute(w, config); err != nil {
		log.Panic(err)
	}
}

func iterate(start, end int) (ch chan int) {
	ch = make(chan int)
	go func() {
		defer close(ch)
		for i := start; i <= end; i++ {
			ch <- i
		}
	}()
	return
}
