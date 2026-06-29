# Bug: chaos-soak-weekly CI 任务 SoakStabilityTest 失败

## 概述

| 项目     | 详情                                                                 |
|----------|----------------------------------------------------------------------|
| 测试     | `SoakStabilityTest.sustainedProposalsStayHealthy`                    |
| 环境     | GitHub Actions (共享 runner)                                         |
| 持续时间 | 测试运行 385.6s 后失败                                                |
| 失败断言 | `commit (906666) must advance within 15s — wedged loop?` (实际 15023ms > 15000ms) |
| 日期     | 2026-06-29                                                           |

## 错误日志

```
13:22:51.373 [raft-node-2] DEBUG raft - 2 received MsgAppResp(rejected, hint: (index 872020, term 2)) from 1 for index 872024
13:22:51.397 [test-raft-node-3-tick] WARN  raft - 3 A tick missed to fire. Node blocks too long!
13:23:37.797 [test-raft-node-3-tick] WARN  raft - 3 A tick missed to fire. Events queue full!
13:23:37.875 [raft-grpc-send-2] WARN  GrpcTransport - send to peer 3 failed: UNAVAILABLE: io exception
13:23:38.122 [raft-grpc-send-1] WARN  GrpcTransport - send to peer 2 failed: UNAVAILABLE: io exception

java.lang.AssertionError:
[commit (906666) must advance within 15s — wedged loop?]
Expecting actual: 15023L to be less than: 15000L
```

## 根本原因

### 核心问题：Storage 锁竞争导致 Leader 的 Ready 循环阻塞

`forceSnapshotAndCompact()` 从测试线程调用，在 Leader 的 `RocksDbStorage` 上持有 `synchronized(lock)` 长达数秒（慢 CI 上甚至超过 15s），导致 Leader 的 applier 线程和整个 Raft 推进流程被阻塞。

### 故障链

```
┌─ 测试线程 ──────────────────────────────────┐
│ forceSnapshotAndCompact(leader, ...)        │
│   ├─ createSnapshotStreaming()              │
│   │   ├─ file write + fsyncFile()    ← 慢  │
│   │   ├─ atomic rename + fsyncDir()  ← 慢  │
│   │   └─ db.write(sync=true)         ← 慢  │
│   └─ compact()                              │
│       └─ db.deleteRange(900K+ keys)  ← 慢  │
│   (全程持有 synchronized(lock))              │
└─────────────────────────────────────────────┘
         │ 锁竞争
         ▼
┌─ Applier 线程 ──────────────────────────────┐
│ storage.writeBatched() → 等待 lock → 阻塞   │
│ ∴ 无法调用 node.advance()                   │
└─────────────────────────────────────────────┘
         │ AdvanceEvent 无法入队
         ▼
┌─ Event Loop 线程 ───────────────────────────┐
│ waitingAdvance = true                       │
│ → 不能 emit 新 Ready                       │
│ → MsgApp 消息堆积在内存中无法发送给 follower │
└─────────────────────────────────────────────┘
         │ 无 MsgApp 到达 follower
         ▼
┌─ Follower 节点 ─────────────────────────────┐
│ 收不到新 MsgApp → 不回 MsgAppResp           │
│ Leader 无法获得 quorum 确认 → commit 停滞    │
└─────────────────────────────────────────────┘
```

### 二级故障：gRPC Server 线程被 `events.put()` 阻塞

当节点的 events 队列（容量 1024）被填满后：

1. gRPC handler 线程调用 `node.step(msg)` → `events.put(new RecvEvent(msg))` → **阻塞**（blocking put）
2. 所有 gRPC handler 线程阻塞 → gRPC server 对外完全无响应
3. 其他节点发送消息时得到 `UNAVAILABLE: io exception`
4. 这使得集群中多个节点互相不可达，进一步加剧 commit 停滞

```
Node 2 (Leader) sendExecutor → blockingStub.send(to node 3) → 等待响应 (hang)
                                                                    ↑
Node 3 gRPC handler → node.step() → events.put(RecvEvent) → 阻塞 (队列满)
```

## 涉及代码

| 文件 | 关键位置 | 问题 |
|------|----------|------|
| `RocksDbStorage.java` | `createSnapshotStreaming()` L473-521 | 文件 I/O + fsync 在 `synchronized(lock)` 内 |
| `RocksDbStorage.java` | `compact()` L448-463 | `db.deleteRange` 在 `synchronized(lock)` 内 |
| `RocksDbStorage.java` | `writeBatched()` L798-850 | Applier 需要同一把锁 |
| `DefaultNode.java` | `advance()` L484-487 | `events.put(AdvanceEvent)` 在队列满时阻塞 |
| `DefaultNode.java` | `step()` L462-476 | `events.put(RecvEvent)` 在队列满时阻塞 gRPC handler |
| `SoakStabilityTest.java` | L100-106 | 在持续 proposal 同时对 leader 执行 snapshot+compact |

## 加重因素

1. **Node 1 日志不匹配**：reject MsgApp（hint: index 872020），leader 需要回退重发，减少可用 quorum
2. **Node 3 事件队列饱和**：128 个 tick 未处理 + 1024 事件队列满
3. **GitHub Actions 共享 runner**：磁盘 I/O 慢（共享存储）、CPU 争抢、可能的 GC pause
4. **大数据量**：900K+ 条目，compact 的 `deleteRange` 范围巨大

## 建议修复方案

### 方案 1（推荐）：缩小 Storage 锁粒度

将 `createSnapshotStreaming()` 中的文件 I/O（write, fsync, rename）移到 `synchronized(lock)` **之外**，只在最终更新 RocksDB 元数据时持锁：

```java
public Eraftpb.Snapshot createSnapshotStreaming(...) {
    // 1. 在锁外执行文件 I/O
    Path tmpPath = ...;
    try (OutputStream out = ...) { writer.writeTo(out); out.flush(); }
    fsyncFile(tmpPath);
    Files.move(tmpPath, finalPath, ATOMIC_MOVE);
    fsyncDir(snapDir);

    // 2. 只在更新元数据时持锁
    synchronized (lock) {
        // 验证 + db.write(metadata)
    }
}
```

### 方案 2：RecvEvent 使用非阻塞入队

对来自 gRPC 的外部消息，使用 `offer()` + 丢弃（类似 tick 的处理方式），避免阻塞 gRPC handler 线程：

```java
// DefaultNode.step() 中对外部 RecvEvent:
if (!events.offer(new RecvEvent(msg))) {
    rn.raft.logger.warn("dropping inbound {} from {:x}: events queue full", msg.getMsgType(), msg.getFrom());
    rn.raft.metrics.onMessageDropped();
}
```

### 方案 3：AdvanceEvent 使用独立通道

给 `AdvanceEvent` 一个独立的、不受主队列容量限制的通道（或优先级），避免与 RecvEvent 竞争队列槽位。

### 方案 4（临时缓解）：增加测试 liveness 阈值

当前 15s 阈值在慢 CI 上裕度不足（实际只超了 23ms），可放宽至 30s：

```java
.isLessThan(30_000); // 从 15_000 改为 30_000
```

## 复现条件

1. 长时间 soak 运行（30 分钟，900K+ 条目）
2. 对 leader 执行 `forceSnapshotAndCompact` 同时持续 propose
3. 慢速磁盘 I/O 环境（如 GitHub Actions 共享 runner）
4. follower 存在日志不匹配需要回退的情况
