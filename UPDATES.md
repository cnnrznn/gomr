## Updates

### Aug 15, 2023
Happy birthday to me :) (Aug 12)

Today I fixed a circular dependency needed to implement Tiers 2 and 3.
I implemented the pre-shuffle work stage of Tier2.
This means that the MapReduce job starts, finds all input data stores local to this worker, creates the intermediate data stores, and executes Tier1 on these inputs and outputs.

Next up, I need to implement shuffle.
I have tentatively decided that shuffle will be pull-based.
Each reducer will query each pier in the cluster and ask for its transform output.
I have also decided that each machine will create transform output for each other machine, even if the data store for that output is empty.
The file should be transferred whether or not it contains data, reducing edge cases and simplifying code.

### Aug 11, 2023

To simplify the shuffle step, I am simplifying the data `Store`s created.

After the transform step, one store will be created for each node in the cluster.
The stores will then be shuffled, with the reducer pulling one file from each pier.
Once all files are local, begin the reduce step.

This will make the reduce step more complicated.
Instead of providing a reducer with a list of stores that all contain the same key, we will provide the reducer Tier1
a stream of `Data`s.
The Tier1 layer will be responsible for spawning a new reducer `Processor` for each new key encountered.
This comes with the downside that multiple reducers will be spawned and running concurrently, which will raise
peak load on the machine.

### July 25, 2023

I am reworking the interface.
As of now, it wasn't clear what data types the interface should deal with.
I have settled on defining an interface type `gomr.Data`, that defines a `Key()` method as well as `Serialize` and `Deserialize`.
A user will be able to supply their own implementation of this interface for both the input and intermediate data.
These data types allow GoMR to move data around while remaining agnostic to the underlying implementation of the Data.

I have written a wordcount example based on the new code base.
In practice it looks very similar to the Tier1 unit tests, but interfaces with the system at the outer level.

The next step is to figure out how data stores should operate.
How should they be populated and communicated across machines?
Who is responsible for creating and naming stores?

### July 21, 2023

Implemented Tier 1 operations.
This includes reading data from stores to be processed by both the map and reduce logic.

The API for Tier 1 currently is:

#### Map

`Map([]Store) []Store, error`

The map tasks takes a list of data stores, loops over each store and feeds the input to the map task.
It reads the resulting rows and produces a set of `Store`'s as output.
It produces exactly 1 store for each unique key produced by the mapper.

#### Reduce
`Reduce([]Store) Store, error`

The reduce tasks takes a list of stores that all pertain to the same reduce key.
After the shuffle, all stores relating to the same key should be present on the same machine, and be fed to the same Reduce task.

GoMR's reducer is designed as a subroutine that consumes a channel of elements all for the same key, and performs whatever reduce logic it wants to produce an output stream.

The Tier1 reduce task combines the output for a single reduce task into a single output store.

#### Next steps

The next step in the project will be designing and implementing Tier 2.
Tier 2 will provide Tier 1 with input and consume its output.
Tier 2 will prepare and process result stores to be consumed by the correct processor.
Essentially, Tier 2 executes the "shuffle" logic on resulting data stores.

### July 20, 2023

Beginnin rework.
I'm taking a new approach to how user code is handled, and simplifying the framework code.

From narrowest to wides scope:
- [Tier 0] User code operates on individual rows, and produces any number of output rows
- [Tier 1] First level of GoMR code feeds data from the OS/filesystem to user code, and stores the output in files
  - Mapper outputs files by key
  - Reducer outputs a file for each key, which may be combined
- [Tier 2] Second level moves files between machines. When map tasks have output all of their data, those outputs need to be sent to the appropriate reducer
- [Tier 3] Third level coordinates job initialization. User code needs to be sent to process on a server somehow

The next step is to design a storage layer for use within Tier 1.
Tier 1 should receive a list of stores as input, and create and close stores for output.

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
