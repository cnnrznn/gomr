package worker

import (
	"fmt"
	"testing"

	"github.com/cnnrznn/gomr"
	"github.com/cnnrznn/gomr/store"
)

type TestProcessor struct {
}

type Data struct {
	key   string
	count int
}

func (d Data) Key() string {
	return d.key
}

func (t *TestProcessor) Map(in <-chan any, out chan<- gomr.Keyer) {
	for elem := range in {
		data := Data{key: elem.(string), count: 1}
		out <- data
	}
	close(out)
}

func (t *TestProcessor) Reduce(in <-chan any, out chan<- any) {
	sum := 0
	key := ""

	for elem := range in {
		sum += elem.(Data).count
		key = elem.(Data).key
	}

	out <- Data{key: key, count: sum}
	close(out)
}

func TestWorkerMap(t *testing.T) {
	w := New(&TestProcessor{})

	s := &store.MemStore{}
	s.Write("this")
	s.Write("is")
	s.Write("a")
	s.Write("word")

	stores := []store.Store{}
	stores = append(stores, s)

	outs, err := w.transform(stores)
	if err != nil {
		t.Error(err)
	}

	for _, out := range outs {
		fmt.Println(out)
	}
}

func TestWorkerReduce(t *testing.T) {
	w := New(&TestProcessor{})

	s := &store.MemStore{}
	s.Write(Data{"is", 1})
	s.Write(Data{"is", 1})
	s.Write(Data{"is", 1})

	out, err := w.reduce([]store.Store{s})
	if err != nil {
		t.Error(err)
	}

	out.Init(store.Config{})
	data, _ := out.Read()

	if data.(Data).count != 3 {
		t.Errorf("Expecting a count of 3, got %v", data.(Data).count)
	}
}
