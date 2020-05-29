# Design of Gomr

This is a design document for multi-machine GoMR.

## Goals of a MapReduce system

1. Take data across multiple machines and compute based on it
2. Map functions are narrow and can be performed locally
3. Reduce functions are wide and require a shuffle + map

## Architecture

### Workflow

1. Driver program communicates with user
2. Executor receives computation from driver
3. Exector performs input -> map -> shuffle -> reduce

### Intermediate data
