package gomr

type Mapper interface {
	Map(in <-chan []byte, out chan<- Data) error
}

type Reducer interface {
	Reduce(in <-chan []byte, out chan<- any) error
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

	// other stuff in the future
	// ex. Filesize int
}

type Cluster struct {
	Nodes []string // <ip>:<port>
	Self  int      // which index am I?
}
