# GoMR

GoMR is a super-fast, super-simple, super-easy-to-debug mapreduce framework
for Go. Written to deploy Mapreduce jobs without dealing with the JVM, for
debugging, performance, and to write code in Go!

## Updates

### July 13, 2023

This project has a lot of attention! At least, more than my other repositories.
Thank you to all who have starred this repo and are interested/found the work.
I hope you continue to follow the project.

There is good news! I recently quit my job and have some time to spend on side projects.
I will be continuing development on this repo, and cleaning up what's here already.

In the past, to distribute the compute I relied on a hacky solution that directly depended on Kubernetes.
In the coming weeks, I'll be removing this explicit dependency and leaving it to the developer/deployer/implementer to define their own infra.
My goal is to have a configuration file that is as simple as providing a list of IP:port(s) where GoMR servers in the same cluster are listening.
This will allow flexible distributed deployments that could be dependent on Kubernetes, but not relying on it.

## An Example

See `examples/wordcount/parallel` for the canonical wordcount mapreduce
program. To build, `cd` into the directory and run `go build`. Then, run with
`./parallel <textfile>`.

## Getting Started

To write jobs for GoMR, we first need to create and object that satisfies the
interfaces found in `gomr.go`. Namely:

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

type Job interface {
	Mapper
	Partitioner
	Reducer
}
```

Second, we need to supply data to the input channel of the mapper. We can do
this manually, or use one of the handy methods found in `input.go`:

```go
inMapChans, outChan := gomr.RunLocal(m, r, wc)
gomr.TextFileParallel(os.Args[1], inMapChans)
```

`m`, `r` are the number of mappers and reducers. `wc` is an object satisfying
the `Job` interface.
