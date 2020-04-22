# GoMR

GoMR is a super-fast, super-simple, super-easy-to-debug mapreduce framework
for Go. Written to deploy Mapreduce jobs without dealing with the JVM, for
debugging, performance, and to write code in Go!

## An Example

See `examples/wordcount/parallel` for the canonical wordcount mapreduce
program. To build, `cd` into the directory and run `go build`. Then, run with
`./parallel <textfile>`.

## Getting Started

To write your own jobs for go, you need to create and object that satisfies
the interfaces found in `gomr.go`. Namely:

```go 
type Mapper interface {
	Map(in <-chan interface{}, out chan<- interface{})
}

type Partitioner interface {
	Partition(in <-chan interface{}, outs []chan interface{}, wg *sync.WaitGroup)
}

type Reducer interface {
	Reduce(in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup)
}

type Keyer interface {
	Key() interface{}
}
```
