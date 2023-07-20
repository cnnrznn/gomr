package worker

import (
	"bufio"
	"os"

	"github.com/cnnrznn/gomr"
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

func (w *Worker) Map(inputs []string) error {
	var inErr, outErr error

	inChan := make(chan any, CHANBUF)
	outChan := make(chan gomr.Keyer, CHANBUF)

	outs := make(map[string]bufio.Writer)

	go w.Processor.Map(inChan, outChan)

	go func() {
		for _, input := range inputs {
			file, err := os.Open(input)
			if err != nil {
				// do something
				inErr = err
				return
			}
			defer file.Close()

			// pipe contents to mapper
			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				inChan <- scanner.Text()
			}
		}

		close(inChan)
	}()

	for row := range outChan {
		key := row.Key()

		if _, ok := outs[key]; !ok {
			file, err := os.Create()
			outs[key] = 
		}

		// check if key has file
		// if not there, create
		// output row to file
	}

	if inErr != nil {
		return inErr
	}
	if outErr != nil {
		return outErr
	}

	return nil
}
