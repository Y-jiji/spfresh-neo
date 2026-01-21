# SPFresh Stress Test Bug Fixes

This document chronicles all bugs found and fixed during the SPFresh stress test implementation and debugging.

## Bug #1: Path Duplication in Head Index Folder

**Error Message:**
```
[4] Cannot load head index from ./build/stress_test_index/./build/stress_test_index/head!
```

**Root Cause:**
In `src/SPFresh/SPFreshInterface.cpp` line 273, the `m_headIndexFolder` was set to the full path:
```cpp
opts->m_headIndexFolder = config.indexPath + "/head";  // WRONG
```

However, SPANN code concatenates `m_headIndexFolder` with `m_indexDirectory`, causing path duplication.

**Fix:**
Changed line 273 to use a relative path:
```cpp
opts->m_headIndexFolder = "head";  // Relative path - will be appended to m_indexDirectory
```

**Files Modified:**
- `src/SPFresh/SPFreshInterface.cpp`: Line 273

## Bug #2: Missing Configuration Flags for Head Selection

**Error Message:**
```
[4] Cannot load head index from ./build/stress_test_index/head!
```
(Correct path, but no head vector files were created)

**Root Cause:**
Three critical flags defaulted to false in `createEmptyIndex`:
- `m_selectHead` (from ParameterDefinitionList.h defaults to false)
- `m_buildHead` (from ParameterDefinitionList.h defaults to false)
- `m_noOutput` (from ParameterDefinitionList.h defaults to false)

Without these flags enabled, the head selection phase was skipped during index building, and no head vector files were created.

**Fix:**
Added lines 282-285 in `src/SPFresh/SPFreshInterface.cpp`:
```cpp
// Enable head selection and building
opts->m_selectHead = true;
opts->m_buildHead = true;
opts->m_noOutput = false;  // Enable output of head vectors
```

**Files Modified:**
- `src/SPFresh/SPFreshInterface.cpp`: Lines 282-285

## Bug #3: SPDK Double Initialization

**Error Message:**
```
EAL: PANIC in tailqinitfn_rte_uio_tailq():
Cannot initialize tailq: UIO_RESOURCE_LIST
EAL: FATAL: Cannot init tailq for objects in class rte_uio_tailq
PANIC in tailqinitfn_rte_uio_tailq():
Cannot initialize tailq: UIO_RESOURCE_LIST
```

**Root Cause:**
DPDK/SPDK can only be initialized once per process. In `createEmptyIndex`:

1. Line 326-332: Called `spannIndex->BuildIndex(...)` to create initial index structure
2. Inside `BuildIndex`, if `m_enableSSD` is true, SPDK gets initialized during "Build SSDIndex" phase
3. After returning from `BuildIndex`, actual operations try to use SPDK
4. If any component tries to initialize SPDK again, DPDK panics with double initialization error

**Fix:**
Temporarily disable SSD during the dummy build, then re-enable for actual operations (lines 318-336):

```cpp
// Temporarily disable SSD building during initial index creation to avoid SPDK double-init
bool originalEnableSSD = opts->m_enableSSD;
bool originalBuildSsdIndex = opts->m_buildSsdIndex;
opts->m_enableSSD = false;  // Disable SSD during dummy build
opts->m_buildSsdIndex = false;

// Build an empty index with a single dummy vector to initialize head structures
std::vector<T> dummyVector(config.dimension, 0);
ErrorCode ret = spannIndex->BuildIndex(
    dummyVector.data(),
    1,
    config.dimension,
    config.distanceMethod == DistCalcMethod::Cosine,
    false
);

// Re-enable SSD for actual operations
opts->m_enableSSD = originalEnableSSD;
opts->m_buildSsdIndex = originalBuildSsdIndex;
```

**Files Modified:**
- `src/SPFresh/SPFreshInterface.cpp`: Lines 318-336

## Bug #4: SPDK Already Initialized During Index Building

**Error Message (First iteration):**
```
[DEBUG] Thread 0 calling initialize()...
[DEBUG] Thread 4 calling initialize()...
Segmentation fault (core dumped)
```

**Error Message (Second iteration):**
```
Initializing SPDK...
Segmentation fault (core dumped)
```

**Root Cause:**
The issue evolved through several debugging iterations:

1. **Initial problem**: Multiple worker threads called `initialize()` simultaneously, causing race conditions
2. **First fix attempt**: Called `initialize()` once in main thread - still crashed
3. **Real root cause discovered**: SPDK is **already initialized** during `BuildIndex`!

The call chain during `BuildIndex`:
- `BuildIndex` → creates `ExtraDynamicSearcher` (line 151 of SPANNIndex.cpp)
- `ExtraDynamicSearcher` constructor → creates `SPDKIO` (line 165 of ExtraDynamicSearcher.h)
- `SPDKIO` constructor → calls `m_pBlockController.Initialize(batchSize)` (line 160 of ExtraSPDKController.h)
- This initializes SPDK **during index construction**

When we later called `index->initialize()` from the stress test:
- `SPFreshInterface::initialize()` → `m_index->Initialize()` → `m_extraSearcher->Initialize()` → `db->Initialize()`
- This calls `BlockController::Initialize()` a **second time**
- Even with mutex protection, the second initialization attempts to access `m_ssdSpdkBdev` which causes segfault

**The Real Issue:**
Calling `initialize()` after `BuildIndex` is **redundant and dangerous** - SPDK is already initialized during index creation when `ExtraDynamicSearcher` is constructed.

**Fix:**
**Do NOT call `initialize()` from the stress test** - SPDK is automatically initialized during index building.

In `src/SPFresh/spfresh_stress_test_uint8.cpp`:

1. Removed all `initialize()` calls (lines 389-395):
```cpp
// Note: Do NOT call initialize() here - SPDK is already initialized
// during BuildIndex when ExtraDynamicSearcher is created
```

2. Removed per-thread initialization from `workerThread` function:
```cpp
// Note: SPDK is already initialized in main thread, no per-thread initialization needed
```

In `src/SPFresh/SPFreshInterface.cpp`:

3. Reverted to normal `BuildIndex` with SPDK enabled (lines 318-337):
```cpp
// Build an empty index with a single dummy vector to initialize head structures
// Note: This will also create the ExtraDynamicSearcher with SPDK since m_useSPDK is true
std::vector<T> dummyVector(config.dimension, 0);
ErrorCode ret = spannIndex->BuildIndex(...);
```

**Files Modified:**
- `src/SPFresh/spfresh_stress_test_uint8.cpp`: Lines 389-395 (removed initialize() call)
- `src/SPFresh/SPFreshInterface.cpp`: Lines 318-342 (normal BuildIndex with explicit m_buildSsdIndex=false)

## Bug #5: Multiple SPDKIO Instances Causing Double Initialization

**Error Message:**
```
EAL: UIO_RESOURCE_LIST tailq is already registered
EAL: PANIC in tailqinitfn_rte_uio_tailq():
Cannot initialize tailq: UIO_RESOURCE_LIST
```

**Root Cause:**
After fixing Bug #4, the stress test still crashed with DPDK double initialization. The real issue:

1. The `BlockController` class has `m_initMutex` and `m_numInitCalled` members (lines 80-81 of ExtraSPDKController.h)
2. These were **non-static**, so each `BlockController` instance had its own mutex and counter
3. If multiple `SPDKIO` instances are created (which happens in some scenarios), each creates its own `BlockController`
4. Each `BlockController` thinks it's the "first" one (m_numInitCalled=0), so all try to initialize SPDK
5. DPDK panics because it can only be initialized once per process

**The Fix:**
Make `m_initMutex` and `m_numInitCalled` **static** so they're shared across all `BlockController` instances:

In `include/Core/SPANN/ExtraSPDKController.h` (lines 80-81):
```cpp
// BEFORE (WRONG):
std::mutex m_initMutex;
int m_numInitCalled = 0;

// AFTER (CORRECT):
static std::mutex m_initMutex;
static int m_numInitCalled;
```

In `src/Core/SPANN/ExtraSPDKController.cpp` (lines 13-14):
```cpp
std::mutex SPDKIO::BlockController::m_initMutex;
int SPDKIO::BlockController::m_numInitCalled = 0;
```

Now only the FIRST `BlockController` instance will initialize SPDK, and all subsequent instances will skip initialization.

**Files Modified:**
- `include/Core/SPANN/ExtraSPDKController.h`: Lines 80-81 (made members static), Line 146 (added debug logging)
- `src/Core/SPANN/ExtraSPDKController.cpp`: Lines 13-14 (added static member definitions)

## Bug #6: Version Map Not Initialized - EmptyIndex Error on InsertVector

**Error Message:**
```
[4] Insert with metadata failed with error code: 21
[DEBUG] Thread 0 first insertVector() succeeded with ID: -1
```

**Root Cause:**
Error code 21 is `ErrorCode::EmptyIndex`. In [SPANNIndex.cpp:1056](src/Core/SPANN/SPANNIndex.cpp#L1056), both `AddIndex` and `AddIndexSPFresh` check:

```cpp
begin = m_versionMap.GetVectorNum();
if (begin == 0) { return ErrorCode::EmptyIndex; }
```

The version map tracks which vectors exist in the index. After `BuildIndex` with 1 dummy vector:
1. The dummy vector becomes VID 0 in the head index
2. However, the `m_versionMap` is **not initialized** because we set `m_buildSsdIndex = false` (to avoid SPDK double init)
3. The version map initialization normally happens in `BuildIndex` → `BuildIndexInternal` at line 884:
   ```cpp
   if (!m_extraSearcher->BuildIndex(p_reader, m_index, m_options, m_versionMap))
   ```
4. Since we skip building the SSD index, `m_versionMap.GetVectorNum()` returns 0
5. Any call to `insertVector` fails with `EmptyIndex` error

**The Fix:**
Manually initialize the version map after `BuildIndex` in `createEmptyIndex`:

In `src/SPFresh/SPFreshInterface.cpp` (lines 339-355):
```cpp
// Initialize version map with the dummy vector
// The version map tracks which vectors exist in the index
// Since we built with 1 dummy vector (VID 0), we need to initialize the map to start at 1
auto headIndex = spannIndex->GetMemoryIndex();
if (headIndex) {
    // Initialize: vectorSize=1 (the dummy vector), blockSize, capacity
    spannIndex->GetVersionMap().Initialize(
        1,  // We have 1 vector (the dummy vector)
        headIndex->m_iDataBlockSize,
        headIndex->m_iDataCapacity
    );
    LOG(Helper::LogLevel::LL_Info, "Version map initialized with 1 dummy vector.\n");
}
```

In `include/Core/SPANN/Index.h` (line 78):
```cpp
inline COMMON::VersionLabel& GetVersionMap() { return m_versionMap; }
```

Now the version map correctly reports that 1 vector exists (the dummy vector at VID 0), so subsequent `insertVector` calls will succeed and allocate VIDs starting from 1.

**Files Modified:**
- `src/SPFresh/SPFreshInterface.cpp`: Lines 339-355 (initialize version map after BuildIndex)
- `include/Core/SPANN/Index.h`: Line 78 (added GetVersionMap() public getter)

## Summary

All six bugs have been fixed:
1. ✅ Path duplication in head index folder
2. ✅ Missing head selection/building configuration flags
3. ✅ SPDK double initialization during dummy build
4. ✅ Removed redundant initialize() call (SPDK already initialized during BuildIndex)
5. ✅ Made BlockController initialization mutex/counter static to prevent multiple SPDK initializations
6. ✅ Initialize version map after BuildIndex to allow insertVector operations

The stress test should now run successfully with multiple worker threads inserting vectors via SPDK.
