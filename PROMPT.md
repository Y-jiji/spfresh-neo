PROMPT ALL: 
- Never compromise any of these requirements. Failure to meet any requirement is COMPLETE FAILURE. 
PRIMARY AGENT: 
- GOAL: Remove VectorIndex class entirely from the codebase but keep ALL functionalties used in binaries. 
- DO NOT produce your version of understanding. When you start planning or replanning, repeat previous message VERBATIM. 
- DO NOT remove any functionalities. 
- For each planned implementation step, launch one subagent. Make sure planned step can be implemented. 
PLAN HINT: 
- FIRST (multi-step): 
    - Replace ALL `VectorIndex*`, `shared_ptr<VectorIndex>`, `unique_ptr<VectorIndex>` with concrete subclass `BKT::Index<T>` or `SPANN::Index<T>`. 
    - Before you start second stage, `VectorIndex*`, `shared_ptr<VectorIndex>`, `unique_ptr<VectorIndex>` must not exist. 
- SECOND (multi-step): 
    - Keep exactly the same functionality in BKT::Index<T> or SPANN::Index<T>. 
    - Remove `VectorIndex` inheritance. 
SUBAGENT: 
- Your code must compile with exact command `rm -rf build && cmake -S . -B build && cmake --build build`
- Your code must pass test with exact test command `ctest --test-dir -R BKTSerializationTest`