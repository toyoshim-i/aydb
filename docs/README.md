# Documentation Guide

Start here to use `aydb`, look up an API, or understand its design. User documentation and the implementation textbook serve different purposes.

## Use aydb in an application

| Task | Read |
| :--- | :--- |
| Choose the application-facing interface | [Public API boundaries](reference/PUBLIC_API.md) |
| Store and retrieve bytes directly from Rust | [Native storage guide](guides/NATIVE_STORAGE.md) |
| Use SQL through SQLite | [SQLite adapter guide](guides/SQLITE.md) |
| Find methods, configuration, and API contracts | [API reference guide](reference/README.md) |

The current interfaces are native Rust storage operations and the SQLite adapter. The native SQL frontend, optional compression, and vector/hybrid search are planned work; see the [roadmap](ARCHITECTURE.md#7-phased-implementation-roadmap). Future vector primitives extend the native storage API. The native SQL frontend will receive its own execution API reference and SQL language manual when implemented.

## Understand and develop the engine

* [Architecture and roadmap](ARCHITECTURE.md): requirements, durability contracts, subsystem boundaries, and future milestones.
* [Development guidelines](DEVELOPMENT.md): contribution workflow and verification requirements.
* [Phase 1 overview](phase1/00_OVERVIEW.md): storage engine textbook. Chapters cover [platform I/O](phase1/01_PAL_AND_OS_CACHE.md), [metadata and GC](phase1/02_BOOT_META_AND_GC.md), [WAL and recovery](phase1/03_EPOCH_WAL_AND_RECOVERY.md), [segments and blobs](phase1/04_WISCKEY_AND_SEGMENTS.md), and [LRU and engine coordination](phase1/05_CAPPED_LRU_AND_KV_API.md).
* [Phase 2 overview](phase2/00_OVERVIEW.md): SQLite integration textbook. Chapters cover [adapter internals](phase2/01_SQLITE_VTAB_ADAPTER.md), [application workload benchmarks](phase2/02_SQLITE_VS_AYDB_BENCHMARKS.md), and [standard SQL workload benchmarks](phase2/03_STANDARD_SQL_BENCHMARKS.md).

The textbook explains why and how the engine works. The guides explain how to call it. Rustdoc describes signatures and individual items. Existing chapter paths remain available so earlier links keep working.
