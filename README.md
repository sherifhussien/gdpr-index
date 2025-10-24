# Index Layer

### Repository structure

[index](./index): Folder containing the index layer.

[ycsb_trace_generator](./ycsb_trace_generator/): Submodule containing a modified version of GDPRBench (YCSB-based) to produce workload traces

## Build instructions

### 0. Dev environment
To enter the development environment with all the required dependencies, use:
```
$ nix develop
``` 

### 1. Make sure you have fetched all the submodules:
```
$ git submodule update --init --recursive
```

### 2. Build the `Index layer`:
```
$ cd index
$ cmake -S . -B build
$ cmake --build build
```

## Sample execution

### 1. Create the workload traces (~5-10mins):
```
$ cd ycsb_trace_generator
$ bash workload_generator.sh
```
This will create the trace files for the workloads in the [`workload_traces`](./workload_traces) directory.

### 2. Run the index tests.

```
$ cd index
$ ./build/tests <workload load file-path> <workload run filepath> <index> <threads> <key-size> <value-size>
```
