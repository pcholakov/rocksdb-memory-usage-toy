# Understanding RocksDB block cache + write buffer manager memory usage

This is a little toy to help you build intuitions for how combined workloads and various
configuration parameters in RocksDB impact overall memory usage.

Try it with: `cargo run --release`. You'll see output along the lines of:

```
Cache usage     WBM usage util     Cache ex-WBM util     |   CF mem avg   CF mem max |          RSS |      written         read
303.4 MiB       303.0 MiB    30.3%          0 B     0.0% |      2.2 MiB     27.0 MiB |    308.5 MiB |  558.3 MiB/s    1.1 GiB/s
576.4 MiB       576.0 MiB    57.6%          0 B     0.0% |      6.2 MiB     52.0 MiB |    586.7 MiB |  544.5 MiB/s    4.0 GiB/s
879.7 MiB       871.0 MiB    87.1%          0 B     0.0% |     10.5 MiB     82.0 MiB |    908.8 MiB |  639.7 MiB/s    6.3 GiB/s
1.1 GiB         822.0 MiB    82.2%    156.8 MiB    15.7% |     12.2 MiB    106.0 MiB |      1.2 GiB |  491.7 MiB/s    7.9 GiB/s
1.4 GiB         874.0 MiB    87.4%    445.9 MiB    44.6% |     12.0 MiB    128.0 MiB |      1.6 GiB |  511.3 MiB/s   10.1 GiB/s
1.7 GiB         877.1 MiB    87.7%    701.4 MiB    70.1% |     12.0 MiB    151.0 MiB |      1.9 GiB |  477.7 MiB/s   11.2 GiB/s
1.9 GiB         822.0 MiB    82.2%    944.1 MiB    94.4% |     11.7 MiB    171.0 MiB |      2.2 GiB |  457.5 MiB/s   11.8 GiB/s
2.0 GiB         899.1 MiB    89.9%    997.2 MiB    99.7% |     12.1 MiB    192.2 MiB |      2.3 GiB |  443.5 MiB/s   13.0 GiB/s
2.0 GiB         882.1 MiB    88.2%    999.9 MiB   100.0% |     11.6 MiB    193.2 MiB |      2.4 GiB |  436.3 MiB/s   13.1 GiB/s
2.0 GiB         848.0 MiB    84.8%    999.9 MiB   100.0% |     12.0 MiB    132.6 MiB |      2.5 GiB |  405.2 MiB/s   13.2 GiB/s
```

Play with the constants in [main.rs](src/main.rs) to adjust various parameters, like the block cache
size, number of column families, read and write load.
