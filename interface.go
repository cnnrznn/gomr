package gomr

type Mapper interface {
	Map(in <-chan []byte, out chan<- Keyer) error
}

type Reducer interface {
	Reduce(in <-chan []byte, out chan<- any) error
}

type Processor interface {
	Mapper
	Reducer
}

type Keyer interface {
	Key() string
	Serialize() ([]byte, error)
}

type Job struct {
	Proc    Processor
	Name    string
	Cluster Cluster

	// other stuff in the future
	// ex. Filesize int
}

type Cluster struct {
	Nodes []string // <ip>:<port>
	Self  int      // which index am I?
}
