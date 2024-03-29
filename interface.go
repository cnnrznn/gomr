package gomr

import (
	"sync"

	"github.com/cnnrznn/gomr/store"
)

type Mapper interface {
	Map(in <-chan Data, out chan<- Data, wg *sync.WaitGroup) error
}

type Reducer interface {
	Reduce(in <-chan Data, out chan<- Data, wg *sync.WaitGroup) error
}

type Processor interface {
	Mapper
	Reducer
}

type Data interface {
	Key() string
	Serialize() ([]byte, error)
	Deserialize([]byte) (Data, error)
}

type Job struct {
	Proc    Processor
	Name    string
	Cluster Cluster

	// InType is used by the transformer to deserialize input data.
	InType Data
	// MidType is used by the reducer to deserialize shuffled data.
	MidType Data
	// OutType is used by the reducer to serialize output data.
	OutType Data

	// InStore describes the input data stores consumed by this job.
	Inputs []store.Config

	// other stuff in the future
	// ex. Filesize int
}

type Cluster struct {
	Nodes []string // <ip>:<port>
	Self  int      // which index am I?
}

func (c Cluster) Size() int {
	return len(c.Nodes)
}
