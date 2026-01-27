#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include "Core/SPANN/ExtraSPDKController.h"

using namespace SPTAG;

bool ExtraSPDKControllerTest() {
    std::cout << "Testing SPDK Controller Basic Functionality" << std::endl;

    const char* spdkConf = std::getenv("SPFRESH_SPDK_CONF");
    const char* spdkBdev = std::getenv("SPFRESH_SPDK_BDEV");
    const char* spdkIODepth = std::getenv("SPFRESH_SPDK_IO_DEPTH");
    const char* ldLibraryPath = std::getenv("LD_LIBRARY_PATH");

    std::cout << "  SPDK Configuration:" << std::endl;
    std::cout << "    LD_LIBRARY_PATH: " << (ldLibraryPath ? ldLibraryPath : "not set") << std::endl;
    std::cout << "    SPFRESH_SPDK_CONF: " << (spdkConf ? spdkConf : "not set") << std::endl;
    std::cout << "    SPFRESH_SPDK_BDEV: " << (spdkBdev ? spdkBdev : "not set") << std::endl;
    std::cout << "    SPFRESH_SPDK_IO_DEPTH: " << (spdkIODepth ? spdkIODepth : "not set") << std::endl;

    if (!spdkBdev || !spdkConf) { return false; }

    const char* mappingPath = "test_spdk_mapping";
    SizeType blockSize = 4096;
    SizeType capacity = 10000;
    SizeType postingBlocks = 256;
    SizeType bufferSize = 1024;
    int batchSize = 64;
    int compactionThreads = 1;

    std::cout << "  Initializing SPDKIO..." << std::endl;
    std::unique_ptr<SPANN::SPDKIO> spdkIO;
    try {
        spdkIO = std::make_unique<SPANN::SPDKIO>(mappingPath, blockSize, capacity, postingBlocks, bufferSize, batchSize, compactionThreads);
    } catch (const std::exception& e) {
        std::cerr << "  FAILED: Exception during SPDKIO initialization: " << e.what() << std::endl;
        return false;
    }

    std::cout << "  Checking if SPDK is properly configured..." << std::endl;
    if (!spdkConf || !spdkBdev) {
        std::cerr << "  FAILED: SPDK is not configured properly" << std::endl;
        std::cerr << "    Please set SPFRESH_SPDK_CONF and SPFRESH_SPDK_BDEV" << std::endl;
        return false;
    }
    std::cout << "  PASSED: SPDK configuration check passed" << std::endl;

    std::cout << "  Testing SPDKIO Put and Get operations..." << std::endl;
    SizeType testKey = 100;
    const std::string putData = "Test data for Put/Get operations. This string should be stored and retrieved correctly.";

    std::cout << "  Putting data with key " << testKey << "..." << std::endl;
    ErrorCode ret = spdkIO->Put(testKey, putData);
    if (ret != ErrorCode::Success) {
        std::cerr << "  FAILED: Put operation failed, error code: " << static_cast<int>(ret) << std::endl;
        return false;
    }
    std::cout << "  PASSED: Put operation succeeded" << std::endl;

    std::cout << "  Getting data with key " << testKey << "..." << std::endl;
    std::string getData;
    ret = spdkIO->Get(testKey, &getData);
    if (ret != ErrorCode::Success) {
        std::cerr << "  FAILED: Get operation failed, error code: " << static_cast<int>(ret) << std::endl;
        return false;
    }
    std::cout << "  PASSED: Get operation succeeded" << std::endl;

    std::cout << "  Verifying Put/Get data matches..." << std::endl;
    if (putData != getData) {
        std::cerr << "  FAILED: Put/Get data mismatch" << std::endl;
        std::cerr << "    Expected size: " << putData.size() << ", Got size: " << getData.size() << std::endl;
        std::cerr << "    Expected: " << putData << std::endl;
        std::cerr << "    Got: " << getData << std::endl;
        return false;
    }
    std::cout << "  PASSED: Put/Get data matches correctly" << std::endl;

    std::cout << "  Testing multiple Put/Get operations..." << std::endl;
    const int numKeys = 5;
    std::vector<std::string> testDataArray;
    for (int i = 0; i < numKeys; ++i) {
        testDataArray.push_back("Test data for key " + std::to_string(i) + ". This is batch test data.");
    }

    bool allPassed = true;
    for (int i = 0; i < numKeys; ++i) {
        SizeType key = 200 + i;
        ret = spdkIO->Put(key, testDataArray[i]);
        if (ret != ErrorCode::Success) {
            std::cerr << "  FAILED: Put operation failed for key " << key << std::endl;
            allPassed = false;
        }
    }

    if (allPassed) {
        std::cout << "  PASSED: All Put operations succeeded" << std::endl;
    } else {
        return false;
    }

    allPassed = true;
    for (int i = 0; i < numKeys; ++i) {
        SizeType key = 200 + i;
        std::string retrievedData;
        ret = spdkIO->Get(key, &retrievedData);
        if (ret != ErrorCode::Success) {
            std::cerr << "  FAILED: Get operation failed for key " << key << std::endl;
            allPassed = false;
        } else if (testDataArray[i] != retrievedData) {
            std::cerr << "  FAILED: Data mismatch for key " << key << std::endl;
            allPassed = false;
        }
    }

    if (allPassed) {
        std::cout << "  PASSED: All Get operations and verifications succeeded" << std::endl;
    } else {
        return false;
    }

    std::cout << "  Testing Delete operation..." << std::endl;
    SizeType deleteKey = 150;
    const std::string deleteData = "Data to be deleted";
    ret = spdkIO->Put(deleteKey, deleteData);
    if (ret != ErrorCode::Success) {
        std::cerr << "  FAILED: Put operation before Delete failed" << std::endl;
        return false;
    }

    ret = spdkIO->Delete(deleteKey);
    if (ret != ErrorCode::Success) {
        std::cerr << "  FAILED: Delete operation failed" << std::endl;
        return false;
    }
    std::cout << "  PASSED: Delete operation succeeded" << std::endl;

    std::cout << "  Verifying deleted key cannot be retrieved..." << std::endl;
    std::string deletedData;
    ret = spdkIO->Get(deleteKey, &deletedData);
    if (ret == ErrorCode::Success) {
        std::cerr << "  FAILED: Deleted key should not be retrievable" << std::endl;
        return false;
    }
    std::cout << "  PASSED: Deleted key properly inaccessible" << std::endl;

    std::cout << "  Shutting down SPDK controller..." << std::endl;
    spdkIO->ShutDown();
    std::cout << "  PASSED: SPDK controller shutdown completed" << std::endl;

    return true;
}

int main(int argc, char* argv[]) {
    std::cout << "======================================" << std::endl;
    std::cout << "Extra SPDK Controller Test" << std::endl;
    std::cout << "======================================" << std::endl;

    bool testPassed = ExtraSPDKControllerTest();

    std::cout << "\n======================================" << std::endl;
    if (testPassed) {
        std::cout << "ALL TESTS PASSED" << std::endl;
        std::cout << "======================================" << std::endl;
        return 0;
    } else {
        std::cout << "SOME TESTS FAILED" << std::endl;
        std::cout << "======================================" << std::endl;
        return 1;
    }
}
