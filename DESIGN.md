# Design of Gomr

This is a design document for multi-machine GoMR.

## Tiers

Tiers are central to the design of this system.
Layers make it easy to separate problems into different related spaces.
The tiers in GoMR are as follows:

- Tier 0: User code
- Tier 1: Intra-machine coordination
- Tier 2: Inter-machine coordination
- Tier 3: Application/Cluster coordination

### Tier 0

User code operates on individual rows, and produces any number of output rows

Mappers consume input rows and produce output elements with a key.

Reducers consume all rows for a key and produce output(s) related to that key.

### Tier 1

First level of GoMR code feeds data from the OS/filesystem to user code, and stores the output in files

### Tier 2

Second level moves files between machines. When map tasks have output all of their data, those outputs need to be sent to the appropriate reducer

### Tier 3

Third level coordinates job initialization. User code needs to be sent to process on a server somehow

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
