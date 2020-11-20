# mqtt-storage

Benchmark tool to validate if any of existing embedden storages can be used to store MQTT session queue.

## Build
```bash
> cargo build --release
```

## Usage

``` bash
> mqtt-storage -h
mqtt-storage 0.1.0

USAGE:
    mqtt-storage [FLAGS] [OPTIONS]

FLAGS:
    -h, --help       Prints help information
        --memory     Examine in-memory storage
        --rocksdb    Examine rocksdb-rs storage
        --sled       Examine sled-rs storage
    -V, --version    Prints version information

OPTIONS:
    -d, --duration <duration>     [default: 2]
    -p, --parallel <parallel>     [default: 1]
    -q, --queues <queues>         [default: 10]
```

## Results
Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz/32GB
```
> ./mqtt-storage --memory --sled --rocksdb -q 20 -d 20
+---------+----------+-------------+------------+-----------+----------+------------+
| storage | writes   | total write | empty iter | loop iter | reads    | total read |
+---------+----------+-------------+------------+-----------+----------+------------+
| memory  | 31879287 | 1.47GB      | 145585     | 32020186  | 31874601 | 1.47GB     |
+---------+----------+-------------+------------+-----------+----------+------------+
| rocksdb | 4831788  | 228.11MB    | 361        | 780471    | 780110   | 36.81MB    |
+---------+----------+-------------+------------+-----------+----------+------------+
| sled    | 2545344  | 120.20MB    | 24939      | 2565776   | 2540837  | 119.99MB   |
+---------+----------+-------------+------------+-----------+----------+------------+
```

RPi4
```
> ./mqtt-storage --memory --sled -q 20 -d 20
+---------+--------+-------------+------------+-----------+--------+------------+
| storage | writes | total write | empty iter | loop iter | reads  | total read |
+---------+--------+-------------+------------+-----------+--------+------------+
| memory  | 508556 | 24.01MB     | 25477      | 533676    | 508199 | 23.99MB    |
+---------+--------+-------------+------------+-----------+--------+------------+
| sled    | 279826 | 13.21MB     | 28988      | 308625    | 279637 | 13.20MB    |
+---------+--------+-------------+------------+-----------+--------+------------+
```