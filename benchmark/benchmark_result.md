# Blackwidow Benchmark Results

## Benchmarks Software and Hardware Infomation 

### Hard disk infomation:

A 512GB NVMe SSD disk

```bash
➜  ~ sudo lshw -class disk
  *-namespace
       description: NVMe disk
       physical id: 1
       bus info: nvme@0:1
       logical name: /dev/nvme0n1
       size: 476GiB (512GB)
       capabilities: gpt-1.00 partitioned partitioned:gpt
       configuration: guid=c5a615e2-d5d0-4a43-bd53-2b4e366a2756 logicalsectorsize=512 sectorsize=512 wwid=eui.6479a71e60837309
```

### OS and CPU, RAM infomation:

```bash
➜  ~ screenfetch

 ██████████████████  ████████     wyatt@wayttarch
 ██████████████████  ████████     OS: Manjaro 21.2.3 Qonos
 ██████████████████  ████████     Kernel: x86_64 Linux 5.9.16-1-MANJARO
 ██████████████████  ████████     Uptime: 1h 0m
 ████████            ████████     Packages: 1381
 ████████  ████████  ████████     Shell: zsh 5.8
 ████████  ████████  ████████     Resolution: 3840x1080
 ████████  ████████  ████████     WM: i3
 ████████  ████████  ████████     GTK Theme: Adapta-Nokto-Eta-Maia [GTK2/3]
 ████████  ████████  ████████     Icon Theme: Papirus-Adapta-Nokto-Maia
 ████████  ████████  ████████     Font: Noto Sans 10
 ████████  ████████  ████████     Disk: 95G / 453G (23%)
 ████████  ████████  ████████     CPU: Intel Core i5-8265U @ 8x 3.9GHz [59.0°C]
 ████████  ████████  ████████     GPU: UHD Graphics 620
                                  RAM: 5252MiB / 31972MiB
```

## Hash Get all:
#### Before

``` bash
Running ./blackwidow_bench
Run on (8 X 3900 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x4)
  L1 Instruction 32 KiB (x4)
  L2 Unified 256 KiB (x4)
  L3 Unified 6144 KiB (x1)
Load Average: 1.75, 1.97, 1.98
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
====== HGetall ======
-------------------------------------------------------
Benchmark             Time             CPU   Iterations
-------------------------------------------------------
BenchHGetall 1492162753 ns   1487535911 ns            1
```

#### After

``` bash
Running ./blackwidow_bench
Run on (8 X 3900 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x4)
  L1 Instruction 32 KiB (x4)
  L2 Unified 256 KiB (x4)
  L3 Unified 6144 KiB (x1)
Load Average: 1.83, 1.77, 1.89
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
====== HGetall ======
-------------------------------------------------------
Benchmark             Time             CPU   Iterations
-------------------------------------------------------
BenchHGetall 1236717460 ns   1234710290 ns            1
```

