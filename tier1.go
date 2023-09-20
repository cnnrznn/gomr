package gomr

import (
	"hash/fnv"
	"sync"

	"github.com/cnnrznn/gomr/store"
)

const (
	CHANBUF = 1024
)

func (j *Job) transform(inputs, outputs []store.Store) error {
	var problem error
	inChan := make(chan Data, CHANBUF)
	outChan := make(chan Data, CHANBUF)

	wg := sync.WaitGroup{}

	// TODO later, the number of goroutines can be increased to improve performance
	wg.Add(1)
	go j.Proc.Map(inChan, outChan, &wg)

	go func() {
		err := feed(inputs, inChan, j.InType)
		if err != nil {
			problem = err
		}
	}()

	go func() {
		wg.Wait()
		close(outChan)
	}()

	for row := range outChan {
		key := row.Key()
		hash := fnv.New32()
		hash.Write([]byte(key))
		index := int(hash.Sum32()) % j.Cluster.Size()

		bs, err := row.Serialize()
		if err != nil {
			return err
		}

		err = outputs[index].Write(bs)
		if err != nil {
			problem = err
			break
		}
	}

	return problem
}

func (j *Job) reduce(inputs []store.Store, output store.Store) error {
	var problem error

	inChans := make(map[string]chan Data)
	inChan := make(chan Data, CHANBUF)
	outChan := make(chan Data, CHANBUF)

	go func() {
		err := feed(inputs, inChan, j.MidType)
		if err != nil {
			problem = err
		}
	}()

	go func() {
		wg := sync.WaitGroup{}

		for data := range inChan {
			key := data.Key()
			if _, ok := inChans[key]; !ok {
				wg.Add(1)
				inChans[key] = make(chan Data, CHANBUF)
				go j.Proc.Reduce(inChans[key], outChan, &wg)
			}
			inChans[key] <- data
		}

		for _, ch := range inChans {
			close(ch)
		}

		wg.Wait()
		close(outChan)
	}()

	for row := range outChan {
		bs, err := row.Serialize()
		if err != nil {
			return err
		}

		err = output.Write(bs)
		if err != nil {
			return err
		}
	}

	if problem != nil {
		return problem
	}

	return nil
}

func feed(stores []store.Store, inChan chan Data, inType Data) error {
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
