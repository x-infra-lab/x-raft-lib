# Roadmap

What's done, and what's left before a `1.0`. This is a port of
[etcd-io/raft](https://github.com/etcd-io/raft) plus pluggable Transport /
Storage modules; the protocol core is mature, the surrounding production and
release machinery is mostly in place, and the remaining work is API hardening
and broader test coverage.

## Done

### Open-source compliance
- Apache-2.0 `LICENSE` + `NOTICE` (etcd-io/raft attribution) at the repo root
  and in `raft-core`; license headers on all hand-written source files.
- English `README.md` + Chinese `README.zh.md`; per-module READMEs.
- `CONTRIBUTING`, `CODE_OF_CONDUCT`, `SECURITY`, `CHANGELOG` at the repo root.
- Sonatype-ready pom: `<licenses>`, `<developers>`, `<scm>`,
  `<issueManagement>`, and a `release` profile (sources + javadoc + GPG +
  `central-publishing-maven-plugin`). A `workflow_dispatch` GitHub Actions
  job (`maven-publish.yml`) runs the signed deploy.

### Production hardening
- Bounded queues for `pendingReadIndexMessages` and `readStates`
  (drop-oldest + warn + metric); `Config.maxPendingReadIndexMessages` /
  `maxReadStates`.
- Pluggable `RaftMetrics` (zero external deps) wired across the common events.
- Error categorisation: `RaftException` carries a `Code`; invariant
  violations throw `RaftInvariantException`.
- Lifecycle: `Node.stop(timeout, TimeUnit)`, lock-free `Node.basicStatus()`,
  `Node.registerLeaderObserver`, `Config.daemonEventLoop`.
- `Config.validate()` rejects unsafe defaults; `electionElapsed` /
  `heartbeatElapsed` widened to `long`.
- `Storage` interface extended (`append` / `setHardState` / `applySnapshot` /
  `createSnapshot` / `compact` / `close`) with full atomicity / durability /
  concurrency / async-path javadoc.

### Integration
- `Transport` interface in raft-core (core stays I/O-free).
- **raft-transport-grpc** — unary RPC hot path, client-streaming + chunked
  RPC for snapshots (bypasses protobuf's 2 GiB single-message ceiling),
  TLS / mTLS support.
- **raft-storage-rocksdb** — three column families, atomic Ready-cycle
  persistence via `writeBatched`, streaming snapshot sidecar, reopen-from-disk
  verified.
- **raft-examples** — runnable distributed KV demo (`KvClusterDemo#main`).
- **raft-tests** — cross-module integration (single-node, 3-node cluster,
  restart-from-disk, leader failover, dynamic membership, snapshot install,
  partition, chaos, soak).
- GitHub Actions CI (`.github/workflows/ci.yml`): JDK 17/21 matrix, full
  reactor `mvn install`, jacoco summary.

## Remaining before 1.0

### Public API boundary
- Isolate internal packages (`tracker/`, `quorum/`, `confchange/`, `Util`)
  from the public surface — JPMS `module-info` or an `internal` convention.
- Stop leaking generated `Eraftpb` protobuf types through the public API.
- Decide on `Config` → Builder and `Node.propose` → exception/Result before
  freezing the API.

### Storage / transport
- Streaming snapshot persistence for >1 GiB state in raft-core's
  create/apply path (rocksdb already has a sidecar; the core API still hands
  `byte[]`).
- Tunable RocksDB options (block cache, compaction style).
- Protocol version field + capability negotiation for safe rolling upgrades.

### Observability & ops
- Structured logging / MDC (node-id / term / correlation fields).
- Rate-limited proposal-rejection logging.

### Test coverage
- Fill out the datadriven corpus (13 → 28) to match etcd-raft.
- Per-module jacoco gates (currently only raft-core is gated).

### Release & tooling
- Cut the first Maven Central release and add the version badge.
- Spotless / Checkstyle style gate; tag → release automation.

## Deliberate non-changes

- POJO → record/Builder for `Config` / `Ready` / `SoftState` — large blast
  radius across tests, limited benefit.
- `(value, error)` records → exceptions — current pattern works.
- Splitting the `Raft` god-class — architectural churn, no behaviour change.
- `Changer.initProgress` keeps `Next = max(lastIndex, 1)` instead of
  etcd-raft's `lastIndex + 1` (the +1 form worsens fresh-peer convergence and
  breaks the `snapshot_install_after_compact` scenario; see `Changer.java`).
