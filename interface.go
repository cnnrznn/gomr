package gomr

type Mapper interface {
	Map(in <-chan any, out chan<- Keyer)
}

type Reducer interface {
	Reduce(in <-chan any, out chan<- any)
}

type Processor interface {
	Mapper
	Reducer
}

type Keyer interface {
	Key() string
}

type Job struct {
	Proc Processor

	// other stuff in the future
}
