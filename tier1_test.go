package gomr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cnnrznn/gomr/store"
)

type TestProcessor struct {
}

type Line struct {
	Payload string
}

func (l Line) Key() string                { return "" }
func (l Line) Serialize() ([]byte, error) { return nil, nil }
func (l Line) Deserialize(bs []byte) (Data, error) {
	return Line{Payload: string(bs)}, nil
}

type CountData struct {
	Word  string `json:"word"`
	Count int    `json:"count"`
}

func (d CountData) Key() string {
	return d.Word
}
func (d CountData) Serialize() ([]byte, error) {
	return json.Marshal(d)
}
func (d CountData) Deserialize(bs []byte) (Data, error) {
	data := CountData{}
	err := json.Unmarshal(bs, &data)
	return data, err
}

func (t *TestProcessor) Map(in <-chan Data, out chan<- Data) error {
	defer close(out)
	for elem := range in {
		words := strings.Split(elem.(Line).Payload, " ")
		for _, word := range words {
			data := CountData{Word: word, Count: 1}
			out <- data
		}
	}

	return nil
}

func (t *TestProcessor) Reduce(in <-chan Data, out chan<- Data) error {
	defer close(out)

	sum := 0
	key := ""

	for elem := range in {
		sum += elem.(CountData).Count
		key = elem.(CountData).Word
	}

	out <- CountData{Word: key, Count: sum}

	return nil
}

func TestWorkerMap(t *testing.T) {
	job := Job{
		Proc:   &TestProcessor{},
		InType: Line{},
		Cluster: Cluster{
			Nodes: []string{"bs"},
			Self:  0,
		},
	}

	s := &store.MemStore{}
	s.Write([]byte("this"))
	s.Write([]byte("is"))
	s.Write([]byte("a"))
	s.Write([]byte("word"))

	output := &store.MemStore{}

	err := job.transform([]store.Store{s}, []store.Store{output})
	if err != nil {
		t.Error(err)
	}

	// TODO check the output is correct
}

func TestWorkerReduce(t *testing.T) {
	job := Job{
		Proc:    &TestProcessor{},
		MidType: CountData{},
		Cluster: Cluster{
			Nodes: []string{"bs"},
			Self:  0,
		},
	}

	bs, _ := CountData{Word: "is", Count: 1}.Serialize()

	s := &store.MemStore{}
	for i := 0; i < 3; i++ {
		s.Write(bs)
	}

	output := &store.MemStore{}

	err := job.reduce([]store.Store{s}, output)
	if err != nil {
		t.Error(err)
	}

	bs, _ = output.Read()
	data, _ := CountData{}.Deserialize(bs)

	if data.(CountData).Count != 3 {
		t.Errorf("Expecting a count of 3, got %v", data.(CountData).Count)
	}
}
