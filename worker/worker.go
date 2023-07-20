package worker

import (
	"github.com/cnnrznn/gomr"
	"github.com/cnnrznn/gomr/store"
)

const (
	CHANBUF = 1024
)

type Worker struct {
	Processor gomr.Processor
}

func New(p gomr.Processor) *Worker {
	return &Worker{
		Processor: p,
	}
}

func (w *Worker) Map(inputs []store.Store) error {
	var inErr, outErr error

	inChan := make(chan any, CHANBUF)
	outChan := make(chan gomr.Keyer, CHANBUF)

	outs := make(map[string]store.Store)

	go w.Processor.Map(inChan, outChan)

	go func() {
		for _, input := range inputs {
			for input.More() {
				data, err := input.Read()
				if err != nil {
					inErr = err
					return
				}

				inChan <- data
			}
		}

		close(inChan)
	}()

	for row := range outChan {
		key := row.Key()

		if _, ok := outs[key]; !ok {
			outs[key] = &store.MemStore{}
		}

		err := outs[key].Write(row.String())
		if err != nil {
			outErr = err
			break
		}
	}

	if inErr != nil {
		return inErr
	}
	if outErr != nil {
		return outErr
	}

	return nil
}
