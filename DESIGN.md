# Design of Gomr

This is a design document for multi-machine GoMR.

## Tiers

Tiers are central to the design of this system.
Layers make it easy to separate problems into different related spaces.
The tiers in GoMR are as follows:

- Tier 0: User code. Operates on data
- Tier 1: Executes Tier1. Interacts with data storage.
- Tier 2: Executes Tier2. Moves data stores between machines.
- Tier 3: Executes Tier3. API enty point.

### Tier 0
User code operates on individual rows, and produces any number of output rows
Mappers consume input rows and produce output elements with a key.
Reducers consume all rows for a key and produce output(s) related to that key.

### Tier 1
Tier 1 manages data flow between local data stores and Tier 0.

### Tier 2
Tier 2 manages data flow between machines.
Stores are created for transform output and shuffled between machines.
Intermediate data is received for reduce.

### Tier 3
Tier 3 is an entrypoint for a user (driver) program.
Once the user defines their job, this layer provides the public API to initiate the job.

## Goals of a MapReduce system

1. Take data across multiple machines and compute based on it
2. Map functions are narrow and can be performed locally
3. Reduce functions are wide and require a shuffle + map

### Workflow

1. Driver program communicates with user
2. Executor receives computation from driver
3. Exector performs input -> map -> shuffle -> reduce

### Intermediate data

TB-Discussed later.
Intermediate data is a problem that has a lot of room for optimization.

## File conventions

Intermediate data is everywhere, and I need a convention for naming files.

One idea for a format:

- mapper input: `job-name.in.<number>` - where there is a number for each worker
- reducer input: `job-name.map.<number>.<key>` - each machine has could have an output for key `<key>`
- reducer output: `job-name.reduce.<key>` - a machine will produce one output file for every key it reduces
