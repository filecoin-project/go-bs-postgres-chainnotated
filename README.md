# go-bs-postgres-chainnotated

This module implements an [IPFS Blockstore](https://github.com/ipfs/go-ipfs-blockstore/) backed by a combination of a PostgreSQL RDBMS coupled with a write-through RAM cache. A number of augmentations make this suitable as a massive-scale blockstore ( e.g. for something like the Filecoin blockchain ).

### Notable differences/improvements overa standard blockstore are:
  - Everything is keyed by complete CIDs instead of multihash
  - Block-data is transparently stored zstd-compressed where sensible
  - There is ability to efficiently record and query DAG-forming block relationships directly in the database
  - If configured with an instance namespace, keeps a log of recently-accessed blocks, which is then used to bulk-load blocks into the LRU cache on cold-starts
  - Has a mode tracking every Read/Write with millisecond precision
  - Ability to track filecoin-chain specific changes in real time
