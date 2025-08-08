# Distributed DataFusion Benchmarks

### Generating tpch data

Generate TPCH data into the `data/` dir

```shell
./gen-tpch.sh
```

### Running tpch benchmarks

After generating the data with the command above:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch --path data/tpch_sf1
```