# mqtt-storage

Benchmark tool to validate if any of existing embedden storages can be used to store MQTT session queue.

![example](img/example.gif)

## Build
```bash
> cargo build --release
```

## Usage

``` bash
> mqtt-storage -h
mqtt-storage 0.1.0

USAGE:
    mqtt-storage [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --duration <duration>      [default: 2]
    -q, --queues <queues>          [default: 10]
    -s, --storage <storage>...     [default: memory]


> mqtt-storage -s memory sled memory rocksdb -q 10 -d 10

    Finished release [optimized] target(s) in 2m 16s
     Running `target/release/mqtt-storage -s memory sled memory rocksdb -q 10 -d 10`
+---------+---------+-------------+------------+-----------+---------+------------+
| storage | writes  | total write | empty iter | loop iter | reads   | total read |
+---------+---------+-------------+------------+-----------+---------+------------+
| memory  | 4736315 | 223.57MB    | 2172587    | 6888440   | 4715853 | 222.60MB   |
+---------+---------+-------------+------------+-----------+---------+------------+
| sled    | 553782  | 26.13MB     | 539426     | 1092676   | 553250  | 26.11MB    |
+---------+---------+-------------+------------+-----------+---------+------------+
| rocksdb | 859690  | 40.54MB     | 51         | 225270    | 225219  | 10.64MB    |
+---------+---------+-------------+------------+-----------+---------+------------+
```
