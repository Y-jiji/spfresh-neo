# SPFresh-Neo

A refactored and streamlined version of [SPFresh](https://github.com/SPFresh/SPFresh) — a SPANN-based approximate nearest neighbor search system with SPDK-backed SSD storage and dynamic index updates (split, merge, reassign).

## Building

```bash
cmake -S . -B build
cmake --build build
```

### Dependencies

- CMake 3.29+
- Boost (headers)
- MPI
- TBB
- OpenMP
- SPDK (with bdev, nvme, uring support)
- libzstd
- liburing, libisal, libisal_crypto, libnuma

### SPDK Runtime

All executables require SPDK environment variables:

```bash
export SPFRESH_SPDK_CONF=script/bdev-uring.json
export SPFRESH_SPDK_BDEV=Uring0
export DPDK_IOVA_MODE=va
```

Hugepages must be configured before running (see `script/setup-hugepages.sh`).

## Executables

- **spfresh** — Main SPFresh workload runner
- **ssdserving** — SSD serving benchmark
- **tape** — Trace-driven workload replay
- **experiment** — Batch vector add experiment with per-batch query evaluation

## Disclaimer

This codebase is refactored from [SPFresh/SPFresh](https://github.com/SPFresh/SPFresh). The following structural changes have been made. **None of these changes alter the core indexing, search, split, merge, or reassign algorithms.**

### Removed components

- **VectorIndex abstract base class** — All virtual dispatch through `VectorIndex*` has been replaced with direct use of the concrete `BKT::Index<T>*` type. The `IExtraSearcher` interface was similarly removed in favor of direct `ExtraDynamicSearcher<T>*`. This eliminates one layer of indirection but does not change any algorithmic behavior.
- **Quantizer support** (`IQuantizer`, `PQQuantizer`, `OPQQuantizer`) — Product quantization codepaths have been removed. The original `GetQuantizedTarget()` call in the search inner loop is replaced by `GetTarget()`, which is equivalent when no quantizer is configured. **If you rely on PQ-compressed vectors, this codebase does not support them.**
- **RocksDB backend** (`ExtraRocksDBController`, `KeyValueIO` interface) — Only the SPDK storage backend is supported. The `m_useKV` / `m_useSPDK` option flags and their associated conditional branches have been removed.
- **TXT and XVEC vector file readers** — Only raw binary vector files (no header, contiguous `N * dim * sizeof(T)` bytes) are supported. The `VectorSetReader` is now a templated mmap-based implementation.
- **Client/Server/Aggregator/Socket infrastructure** — Removed entirely.
- **Python/Java/C# wrappers** — Removed entirely.
- **Neighborhood graph variants** (`NeighborhoodGraph`, `KNearestNeighborhoodGraph`) — Removed; only `RelativeNeighborhoodGraph` is retained.

### Preserved components

- **Core SPANN algorithms** — Head selection (BKT-based and random), head index building, SSD index building, posting list split/merge/reassign, and RNG-based vector placement are all unchanged.
- **GPU-accelerated RNG path** — `BKT::Index::ApproximateRNG` with `getTailNeighborsTPT` GPU kernel dispatch is fully preserved.
- **SPDK I/O path** — `SPDKIO` block controller with DMA buffers, concurrent block allocation, and atomic CAS-based updates is unchanged.
- **Search path** — BKT head search, posting ID collection with `maxDistRatio` filtering, `MultiGet` batch retrieval, per-vector distance computation with version/deletion checking, and deduplication are all identical to the original.
- **Dynamic update path** — `AddIndexSPFresh` with version map allocation, cosine normalization, RNG selection, and async append; background split/merge/reassign thread pools — all unchanged.
- **Data compression** — ZSTD-based posting list compression with dictionary training is preserved.

### Performance implications

The refactoring is **performance-neutral to slightly positive**:

- Replacing `VectorIndex*` with `BKT::Index<T>*` eliminates virtual function dispatch on hot paths (distance computation, sample access, index search during RNG selection). The compiler can now potentially inline these calls.
- All I/O paths, data structures, locking strategies, and algorithmic constants are identical to the original.
- The mmap-based `VectorSetReader` replaces stream-based reading during index build, which may marginally improve build-time I/O performance.

### Structural changes

| Aspect | Original SPFresh | SPFresh-Neo |
|--------|-----------------|-------------|
| Header path | `AnnService/inc/` | `include/` |
| Source path | `AnnService/src/` | `src/` |
| Build system | CMake + .vcxproj | CMake only |
| Head index type | `std::shared_ptr<VectorIndex>` | `std::shared_ptr<BKT::Index<T>>` |
| Disk index type | `std::shared_ptr<IExtraSearcher>` | `std::shared_ptr<ExtraDynamicSearcher<T>>` |
| Storage backends | SPDK + RocksDB | SPDK only |
| Vector file formats | Binary + TXT + XVEC | Binary only |
| Quantization | PQ/OPQ supported | Removed |
| GPU support | Build-time optional | Build-time optional (preserved) |
| Distance utilities | `Core/Common/` | `Utils/` (separate library) |
