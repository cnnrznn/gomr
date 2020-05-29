package gomr

import (
	"sync"
)

const (
	CHANBUF = 4096
	FILEBUF = 1024 * 1024 * 32

	MAPPER  = 0
	REDUCER = 1
)

type Mapper interface {
	Map(in <-chan interface{}, out chan<- interface{})
}

type Partitioner interface {
	Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup)
}

type Reducer interface {
	Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup)
}

type Job interface {
	Mapper
	Partitioner
	Reducer
}

type Keyer interface {
	Key() interface{}
}
