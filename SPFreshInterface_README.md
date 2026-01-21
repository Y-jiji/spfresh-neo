# SPFresh Interface

A clean, simplified interface over SPFresh for read (KNN search) and write (insert/delete) operations.

## Overview

The SPFreshInterface provides a straightforward API for working with SPFresh indexes, abstracting away the complexity of the underlying SPANN implementation while exposing the core functionality needed for vector similarity search and index updates.

## Features

- **Index Creation**
  - Create new empty indexes from scratch
  - Load existing indexes from disk
  - Save indexes to disk

- **KNN Search (Read Operations)**
  - Single query search
  - Batch query search

- **Vector Insertion (Write Operations)**
  - Single vector insertion
  - Batch vector insertion

- **Vector Deletion**
  - Delete vectors by ID

- **Index Information**
  - Get vector count
  - Get dimension
  - Automatic dimension inference from index

## API Reference

### `createEmptyIndex` (Static Factory Method)

```cpp
static std::shared_ptr<SPFreshInterface<T>> createEmptyIndex(const IndexConfig& config)
```

Creates a new empty SPFresh index from scratch.

**Parameters:**
- `config`: Configuration structure with the following fields:
  - `dimension`: Vector dimension (required)
  - `distanceMethod`: Distance calculation method (L2 or Cosine)
  - `indexPath`: Directory path where the index will be stored (required)
  - `headVectorCount`: Number of cluster centers (default: 1000)
  - SPDK storage backend configuration:
    - `spdkMappingPath`: SPDK device mapping file (default: indexPath/spdk_mapping.txt)
    - `ssdInfoFile`: SPDK SSD info file (default: indexPath/ssd_info.txt)
    - `spdkBatchSize`: SPDK batch size (default: 128)

**Returns:**
- Shared pointer to the interface, or nullptr on failure

**Example:**
```cpp
IndexConfig config;
config.dimension = 128;
config.distanceMethod = DistCalcMethod::L2;
config.indexPath = "/path/to/new/index";
config.headVectorCount = 1000;
config.spdkMappingPath = "/path/to/new/index/spdk_mapping.txt";
config.ssdInfoFile = "/path/to/new/index/ssd_info.txt";
config.spdkBatchSize = 128;

auto interface = SPFreshInterface<float>::createEmptyIndex(config);
if (interface) {
    std::cout << "Created empty index with SPDK!" << std::endl;
}
```

---

### `loadIndex` (Static Factory Method)

```cpp
static std::shared_ptr<SPFreshInterface<T>> loadIndex(const std::string& path)
```

Loads an existing SPFresh index from disk.

**Parameters:**
- `path`: Directory path where the index is stored

**Returns:**
- Shared pointer to the interface, or nullptr on failure

**Example:**
```cpp
auto interface = SPFreshInterface<float>::loadIndex("/path/to/existing/index");
if (interface) {
    std::cout << "Loaded index with " << interface->getVectorCount() << " vectors" << std::endl;
}
```

---

### Constructor

```cpp
SPFreshInterface<T>(std::shared_ptr<SPANN::Index<T>> index)
```

Creates a new interface over an existing SPFresh index (for advanced use).

**Parameters:**
- `index`: Shared pointer to a loaded SPANN index

**Note:** Most users should use `createEmptyIndex()` or `loadIndex()` instead.

---

### `knnSearch`

```cpp
std::vector<SearchResult> knnSearch(const T* query, int k)
```

Performs KNN search for a single query vector.

**Parameters:**
- `query`: Pointer to the query vector (dimension inferred from index)
- `k`: Number of nearest neighbors to return

**Returns:**
- Vector of `SearchResult` objects containing `vectorID` and `distance`

**Example:**
```cpp
std::vector<float> query(dimension);
// ... populate query vector ...
auto results = interface.knnSearch(query.data(), 10);
for (const auto& result : results) {
    std::cout << "VID: " << result.vectorID
              << ", Distance: " << result.distance << std::endl;
}
```

---

### `batchKnnSearch`

```cpp
std::vector<std::vector<SearchResult>> batchKnnSearch(
    const T* queries,
    int numQueries,
    int k
)
```

Performs batch KNN search for multiple query vectors.

**Parameters:**
- `queries`: Pointer to array of query vectors (dimension inferred from index)
- `numQueries`: Number of query vectors
- `k`: Number of nearest neighbors to return per query

**Returns:**
- Vector of result vectors, one for each query

**Example:**
```cpp
std::vector<float> queries(5 * dimension);
// ... populate queries ...
auto batchResults = interface.batchKnnSearch(queries.data(), 5, 10);
for (size_t i = 0; i < batchResults.size(); ++i) {
    std::cout << "Query " << i << " found "
              << batchResults[i].size() << " neighbors" << std::endl;
}
```

---

### `insertVector`

```cpp
int insertVector(const T* vector)
```

Inserts a single vector into the index.

**Parameters:**
- `vector`: Pointer to the vector data (dimension inferred from index)

**Returns:**
- The assigned vector ID on success, or -1 on failure

**Example:**
```cpp
std::vector<float> newVector(dimension);
// ... populate vector ...
int vectorID = interface.insertVector(newVector.data());
if (vectorID >= 0) {
    std::cout << "Inserted with ID: " << vectorID << std::endl;
}
```

---

### `batchInsertVectors`

```cpp
std::vector<int> batchInsertVectors(const T* vectors, int numVectors)
```

Inserts multiple vectors into the index.

**Parameters:**
- `vectors`: Pointer to array of vectors (dimension inferred from index)
- `numVectors`: Number of vectors to insert

**Returns:**
- Vector of assigned IDs for each inserted vector

**Example:**
```cpp
std::vector<float> vectors(100 * dimension);
// ... populate vectors ...
auto vectorIDs = interface.batchInsertVectors(vectors.data(), 100);
std::cout << "Inserted " << vectorIDs.size() << " vectors" << std::endl;
```

---

### `deleteVector`

```cpp
bool deleteVector(int vectorID)
```

Deletes a vector from the index by ID.

**Parameters:**
- `vectorID`: ID of the vector to delete

**Returns:**
- `true` if deletion was successful, `false` otherwise

**Example:**
```cpp
if (interface.deleteVector(42)) {
    std::cout << "Successfully deleted vector 42" << std::endl;
}
```

---

### `getVectorCount`

```cpp
int getVectorCount() const
```

Returns the number of vectors currently in the index.

**Returns:**
- Current vector count

---

### `getDimension`

```cpp
int getDimension() const
```

Returns the dimension of vectors in the index.

**Returns:**
- Vector dimension

---

### `initialize`

```cpp
bool initialize()
```

Initializes the interface (required for multi-threaded contexts).

**Returns:**
- `true` if initialization was successful

**Example:**
```cpp
if (!interface->initialize()) {
    std::cerr << "Failed to initialize interface" << std::endl;
    return 1;
}
```

---

### `saveIndex`

```cpp
bool saveIndex(const std::string& path)
```

Saves the index to disk.

**Parameters:**
- `path`: Directory path where the index will be saved

**Returns:**
- `true` if save was successful

**Example:**
```cpp
if (interface->saveIndex("/path/to/save/index")) {
    std::cout << "Index saved successfully!" << std::endl;
}
```

---

## SearchResult Structure

```cpp
struct SearchResult {
    int vectorID;      // Vector ID
    float distance;    // Distance to query vector
};
```

## Supported Types

The interface supports the following template types (matching SPANN::Index supported types):
- `float` - 32-bit floating point
- `std::int8_t` - 8-bit signed integer
- `std::uint8_t` - 8-bit unsigned integer
- `std::int16_t` - 16-bit signed integer

## Building

The interface is built as part of the SPFresh project:

```bash
mkdir build && cd build
cmake ..
make
```

This builds:
- `libSPFreshInterface.a` - Static library
- `spfresh_example` - Example usage executable (for existing indexes)
- `spfresh_create_index` - Example for creating new indexes with SPDK
- `spfresh_create_index_spdk` - Alternative example for SPDK index creation

## Usage Examples

### Creating a New Index with SPDK

See [src/SPFresh/example_create_index.cpp](src/SPFresh/example_create_index.cpp) for a complete example of creating a new empty index with SPDK backend.

**SPDK Requirements:**
- NVMe SSD hardware
- SPDK installed and configured
- Devices bound to SPDK userspace drivers
- Root/sudo access

To run the example:

```bash
sudo ./spfresh_create_index /path/to/new/index
```

This example demonstrates:
- Creating an empty index with SPDK (direct NVMe access)
- Configuring SPDK-specific parameters
- High-performance vector insertion and search
- Creating SPDK device mapping and SSD info files

### Using an Existing Index

See [src/SPFresh/example_usage.cpp](src/SPFresh/example_usage.cpp) for a complete example of working with an existing index.

To run the example:

```bash
./spfresh_example /path/to/existing/index
```

This example demonstrates:
- Loading an existing index
- Performing single and batch searches
- Inserting new vectors
- Advanced operations

## Error Handling

All methods perform input validation and log errors using the SPFresh logging system. Methods return appropriate error indicators:
- Search methods return empty vectors on error
- Insert methods return -1 on error
- Delete methods return `false` on error

Check the logs for detailed error information.

## Thread Safety

- Multiple concurrent reads (searches) are thread-safe
- Writes (inserts/deletes) are internally synchronized
- Call `initialize()` once per thread in multi-threaded contexts before performing operations

## Quick Start

### Creating a New Index and Adding Vectors

```cpp
#include "SPFresh/SPFreshInterface.h"

using namespace SPTAG::SSDServing::SPFresh;

// 1. Configure and create empty index
IndexConfig config;
config.dimension = 128;
config.distanceMethod = DistCalcMethod::L2;
config.indexPath = "/path/to/index";

auto interface = SPFreshInterface<float>::createEmptyIndex(config);

// 2. Initialize
interface->initialize();

// 3. Insert vectors
std::vector<float> vector(128);  // Your vector data
int vectorID = interface->insertVector(vector.data());

// 4. Search
std::vector<float> query(128);   // Your query
auto results = interface->knnSearch(query.data(), 10);

// 5. Save
interface->saveIndex(config.indexPath);
```

### Loading and Using an Existing Index

```cpp
// 1. Load index
auto interface = SPFreshInterface<float>::loadIndex("/path/to/index");

// 2. Initialize
interface->initialize();

// 3. Search
std::vector<float> query(interface->getDimension());
auto results = interface->knnSearch(query.data(), 10);

// 4. Insert more vectors
std::vector<float> newVector(interface->getDimension());
int vectorID = interface->insertVector(newVector.data());
```

## Design Philosophy

The interface is designed to:
1. **Simplify usage** - Hide internal complexity while exposing essential functionality
2. **Minimize parameters** - Infer dimension from the index automatically
3. **Be type-safe** - Use templates for compile-time type checking
4. **Handle errors gracefully** - Validate inputs and return clear error indicators
5. **Support batching** - Provide efficient batch operations for multiple vectors
6. **Enable easy index creation** - Provide factory methods for creating new indexes

## Storage Backend

### SPDK Backend

SPFresh uses SPDK (Storage Performance Development Kit) for direct NVMe access, providing maximum I/O performance.

**Advantages:**
- Direct NVMe access (bypasses kernel I/O stack)
- Significantly lower latency
- Higher throughput for random access
- Reduced CPU usage for I/O operations
- Optimal for high-performance workloads

**Requirements:**
- NVMe SSD hardware
- SPDK installation and configuration
- Root/sudo privileges
- Userspace NVMe driver setup
- SPDK device mapping file with NVMe device information
- SSD info file with device specifications

**Use Cases:**
- Production high-performance systems
- Latency-sensitive applications
- High-throughput vector search workloads
- Large-scale similarity search deployments

## Notes

- The dimension parameter has been removed from all methods as it's automatically inferred from the index
- All vector pointers should point to contiguous memory with the correct dimension
- For batch operations, vectors should be laid out sequentially in memory
- SPDK backend is always used for maximum performance with direct NVMe access
- Ensure SPDK is properly configured before creating or loading indexes
