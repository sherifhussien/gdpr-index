# Index Layer

## Build

```bash
$ nix develop
$ cd index
$ cmake -S . -B build
$ cmake --build build
```

## Run Tests

```bash
$ ./build/tests <workload load file-path> <workload run filepath> <index> <threads> <key-size> <value-size>
```

## Memory montiring

```bash
$  ./watch.sh
```