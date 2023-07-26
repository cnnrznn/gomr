package worker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/cnnrznn/gomr"
	"github.com/cnnrznn/gomr/store"
)

type TestProcessor struct {
}

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
	return json.Marshal(d)
}
func (d Data) Deserialize(bs []byte) (gomr.Data, error) {
	data := Data{}
	err := json.Unmarshal(bs, &data)
	return data, err
}

func (t *TestProcessor) Map(in <-chan gomr.Data, out chan<- gomr.Data) error {
	defer close(out)
	for elem := range in {
		words := strings.Split(elem.(Line).Payload, " ")
		for _, word := range words {
			data := Data{Word: word, Count: 1}
			out <- data
		}
	}

	return nil
}

func (t *TestProcessor) Reduce(in <-chan gomr.Data, out chan<- gomr.Data) error {
	defer close(out)

	sum := 0
	key := ""

	for elem := range in {
		sum += elem.(Data).Count
		key = elem.(Data).Word
	}

	out <- Data{Word: key, Count: sum}

	return nil
}

func TestWorkerMap(t *testing.T) {
	t1 := Tier1{
		Job: gomr.Job{
			Proc:   &TestProcessor{},
			InType: Line{},
		},
	}

	s := &store.MemStore{}
	s.Write([]byte("this"))
	s.Write([]byte("is"))
	s.Write([]byte("a"))
	s.Write([]byte("word"))

	stores := []store.Store{}
	stores = append(stores, s)

	outs, err := t1.transform(stores)
	if err != nil {
		t.Error(err)
	}

	for _, out := range outs {
		fmt.Println(out)
	}
}

func TestWorkerReduce(t *testing.T) {
	t1 := Tier1{
		Job: gomr.Job{
			Proc:    &TestProcessor{},
			MidType: Data{},
		},
	}

	bs, _ := Data{Word: "is", Count: 1}.Serialize()

	s := &store.MemStore{}
	for i := 0; i < 3; i++ {
		s.Write(bs)
	}

	out, err := t1.reduce([]store.Store{s})
	if err != nil {
		t.Error(err)
	}

	bs, _ = out.Read()
	data, _ := Data{}.Deserialize(bs)

	if data.(Data).Count != 3 {
		t.Errorf("Expecting a count of 3, got %v", data.(Data).Count)
	}
}
