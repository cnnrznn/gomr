package worker

import (
	"github.com/cnnrznn/gomr"
	"github.com/cnnrznn/gomr/store"
)

const (
	CHANBUF = 1024
)

type Tier1 struct {
	Processor gomr.Processor
}

func New(p gomr.Processor) *Tier1 {
	return &Tier1{
		Processor: p,
	}
}

func (w *Tier1) Map(inputs []store.Store) ([]store.Store, error) {
	var problem error
	inChan := make(chan any, CHANBUF)
	outChan := make(chan gomr.Keyer, CHANBUF)

	outs := make(map[string]store.Store)

	go w.Processor.Map(inChan, outChan)

	go func() {
		err := feed(inputs, inChan)
		if err != nil {
			problem = err
		}
	}()

	for row := range outChan {
		key := row.Key()

		if _, ok := outs[key]; !ok {
			outs[key] = &store.MemStore{}
		}

		err := outs[key].Write(row)
		if err != nil {
			problem = err
			break
		}
	}

	if problem != nil {
		return nil, problem
	}

	result := []store.Store{}
	for _, s := range outs {
		result = append(result, s)
	}

	return result, nil
}

func (w *Tier1) Reduce(inputs []store.Store) (store.Store, error) {
	var problem error
	inChan := make(chan any, CHANBUF)
	outChan := make(chan any, CHANBUF)

	result := &store.MemStore{}
	result.Init(store.Config{
		//Name: <name>
	})

	go w.Processor.Reduce(inChan, outChan)

	go func() {
		err := feed(inputs, inChan)
		if err != nil {
			problem = err
		}
	}()

	for row := range outChan {
		err := result.Write(row)
		if err != nil {
			return nil, err
		}
	}

	if problem != nil {
		return nil, problem
	}

	return result, nil
}

func feed(stores []store.Store, inChan chan any) error {
	for _, input := range stores {
		for input.More() {
			data, err := input.Read()
			if err != nil {
				return err
			}

			inChan <- data
		}
	}

	close(inChan)
	return nil
}
