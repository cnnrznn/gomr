package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cnnrznn/gomr"
)

type Data struct {
	Word  string `json:"word"`
	Count int    `json:"count"`
}

func (d Data) Key() string {
	return d.Word
}

func (d Data) Serialize() ([]byte, error) {
	bs, err := json.Marshal(d)
	return bs, err
}

type WordcountProc struct{}

func (w *WordcountProc) Map(in <-chan []byte, out chan<- gomr.Keyer) error {
	defer close(out)

	counts := make(map[string]int)

	for row := range in {
		words := strings.Split(string(row), " ")
		for _, word := range words {
			counts[word]++
		}
	}

	for k, v := range counts {
		out <- Data{
			Word:  k,
			Count: v,
		}
	}

	return nil
}

func (w *WordcountProc) Reduce(in <-chan []byte, out chan<- any) error {
	defer close(out)

	count := 0
	word := ""

	for row := range in {
		data := Data{}
		err := json.Unmarshal(row, &data)
		if err != nil {
			return err
		}

		count++
		word = data.Word
	}

	out <- Data{Word: word, Count: count}

	return nil
}

func main() {
	fmt.Println("Starting wordcount")

	job := gomr.Job{
		Proc: &WordcountProc{},
		Name: "wordcount",
	}

	gomr.
}
