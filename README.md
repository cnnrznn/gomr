# GoMR

GoMR is a super-fast, super-simple, super-easy-to-debug mapreduce framework
for Go. Written to deploy Mapreduce jobs without dealing with the JVM, to
easily debug code, to improve performance, and to write code MR code in Go!

## Announcement!

After a few months of renovation, I'm happy with the result.
This package began as a hack, then grew into a bigger hack for the sake of giving a talk at some Gopher meetups.
However, over the past few months I decided to revisit the project and implement it properly.
Although there is more testing to be done, I am happy with the code as it stands today.
I am happy with its design and architecture, and I believe I've made good use of the Go type system to create software strata that each handle a specific problem.
There is work yet to be done to productionalize this project.

I hope you all enjoy this Go implementation of a MapReduce framework, and you are able to learn something about Go, MapReduce, or software design from it.

Thank you :)

## Getting started

The API for GoMR works as follows.
First, you, the user, implement a GoMR `type Processor interface` for each job you want to run.
A `Processor` implements two functions, `Map` and `Reduce`.
These functions operate on `type Data interface` pieces of data, which are user defined and allow the framework to serialize, deserialize, and determine a reduce key for the data.

See `examples/wordcount` for two implementations of the data interface.
Generally, a job requires two `Data` implementations.
One is used to read the input data from a file, and the other is used for intermediate, "shuffle," data.

Once you implement a `Processor` and two `Data` formats, you can construct a `Job` and invoke `MapReduce()`.
