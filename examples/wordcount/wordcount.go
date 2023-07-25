package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cnnrznn/gomr"
	"github.com/cnnrznn/gomr/store"
)

type Line struct {
	Payload string
}

func (l Line) Key() string                { return "" }
func (l Line) Serialize() ([]byte, error) { return nil, nil }
func (l Line) Deserialize(bs []byte) (gomr.Data, error) {
	return Line{Payload: string(bs)}, nil
}

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

func (d Data) Deserialize(bs []byte) (gomr.Data, error) {
	data := Data{}
	err := json.Unmarshal(bs, &data)
	return data, err
}

type WordcountProc struct{}

func (w *WordcountProc) Map(in <-chan gomr.Data, out chan<- gomr.Data) error {
	defer close(out)

	counts := make(map[string]int)

	for row := range in {
		words := strings.Split(row.(Line).Payload, " ")
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

func (w *WordcountProc) Reduce(in <-chan gomr.Data, out chan<- gomr.Data) error {
	defer close(out)

	count := 0
	word := ""

	for row := range in {
		data := row.(Data)
		if word != "" && word != data.Word {
			return fmt.Errorf("Reducer supplied multiple keys")
		}

		count += data.Count
		word = data.Word
	}

	out <- Data{Word: word, Count: count}

	return nil
}

func main() {
	fmt.Println("Starting wordcount")

	inputStore := store.MemStore{
		Data: [][]byte{
			[]byte(" these are words\n"),
			[]byte("written on a page"),
			[]byte("that no one will read"),
		},
	}
	inputStore.Init(store.Config{
		Name: "in1",
		Node: "localhost:8080",
	})

	job := gomr.Job{
		Proc: &WordcountProc{},
		Name: "wordcount",

		InType:   Line{},
		MidType:  Data{},
		InStores: []store.Store{&inputStore},

		Cluster: gomr.Cluster{
			Nodes: []string{
				"localhost:8080",
			},
			Self: 0,
		},
	}

	fmt.Println(job)

	job.MapReduce()
}
