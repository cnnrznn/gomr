# GoMR

GoMR is a super-fast, super-simple, super-easy-to-debug mapreduce framework
for Go. Written to deploy Mapreduce jobs without dealing with the JVM, to
easily debug code, to improve performance, and to write code MR code in Go!

## Getting started

The API for GoMR works as follows.
First, you, the user, implement a GoMR `type Processor interface` for each job you want to run.
A `Processor` implements two functions, `Map` and `Reduce`.
These functions operate on `type Data interface` pieces of data, which are user defined and allow the framework to serialize, deserialize, and determine a reduce key for the data.

See `examples/wordcount` for two implementations of the data interface.
Generally, a job requires two `Data` implementations.
One is used to read the input data from a file, and the other is used for intermediate, "shuffle," data.

Once you implement a `Processor` and two `Data` formats, you can construct a `Job` and invoke `MapReduce()`.
