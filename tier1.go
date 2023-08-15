package gomr

import (
	"hash/fnv"

	"github.com/cnnrznn/gomr/store"
)

const (
	CHANBUF = 1024
)

func (j *Job) transform(inputs, outputs []store.Store) error {
	var problem error
	inChan := make(chan Data, CHANBUF)
	outChan := make(chan Data, CHANBUF)

	go j.Proc.Map(inChan, outChan)

	go func() {
		err := feed(inputs, inChan, j.InType)
		if err != nil {
			problem = err
		}
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

	if problem != nil {
		return problem
	}

	return nil
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
		for data := range inChan {
			key := data.Key()
			if _, ok := inChans[key]; !ok {
				ch := make(chan Data, CHANBUF)
				defer close(ch)
				inChans[key] = ch
				go j.Proc.Reduce(ch, outChan)
			}
			inChans[key] <- data
		}
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
