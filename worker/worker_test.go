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
	val string
}

func (d Data) String() string {
	return d.val
}

func (d Data) Key() string {
	return d.val
}

func (t *TestProcessor) Map(in <-chan any, out chan<- gomr.Keyer) {
	for elem := range in {
		data := Data{val: elem.(string)}
		out <- data
	}
	close(out)
}

func (t *TestProcessor) Reduce(in <-chan any, out chan<- any) {
	sum := 0

	for _ = range in {
		sum++
	}

	out <- sum
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

	outs, err := w.Map(stores)
	if err != nil {
		t.Error(err)
	}

	for _, out := range outs {
		fmt.Println(out)
	}
}
