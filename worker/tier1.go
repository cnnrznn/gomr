package worker

import (
	"github.com/cnnrznn/gomr"
	"github.com/cnnrznn/gomr/store"
)

const (
	CHANBUF = 1024
)

type Tier1 struct {
	Job gomr.Job
}

func (w *Tier1) transform(inputs []store.Store) ([]store.Store, error) {
	var problem error
	inChan := make(chan gomr.Data, CHANBUF)
	outChan := make(chan gomr.Data, CHANBUF)

	outs := make(map[string]store.Store)

	go w.Job.Proc.Map(inChan, outChan)

	go func() {
		err := feed(inputs, inChan, w.Job.InType)
		if err != nil {
			problem = err
		}
	}()

	for row := range outChan {
		key := row.Key()

		if _, ok := outs[key]; !ok {
			outs[key] = &store.MemStore{}
		}

		bs, err := row.Serialize()
		if err != nil {
			return nil, err
		}

		err = outs[key].Write(bs)
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

func (w *Tier1) reduce(inputs []store.Store) (store.Store, error) {
	var problem error
	inChan := make(chan gomr.Data, CHANBUF)
	outChan := make(chan gomr.Data, CHANBUF)

	result := &store.MemStore{}

	go w.Job.Proc.Reduce(inChan, outChan)

	go func() {
		err := feed(inputs, inChan, w.Job.MidType)
		if err != nil {
			problem = err
		}
	}()

	for row := range outChan {
		bs, err := row.Serialize()
		if err != nil {
			return nil, err
		}

		err = result.Write(bs)
		if err != nil {
			return nil, err
		}
	}

	if problem != nil {
		return nil, problem
	}

	return result, nil
}

func feed(stores []store.Store, inChan chan gomr.Data, inType gomr.Data) error {
	defer close(inChan)

	for _, input := range stores {
		for input.More() {
			bs, err := input.Read()
			if err != nil {
				return err
			}

			data, err := inType.Deserialize(bs)
			if err != nil {
				return err
			}

			inChan <- data
		}
	}

	return nil
}
