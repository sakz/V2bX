# V2bX 性能审计与优化报告

> 两阶段工作：代码级性能审计与优化（Phase 1）+ 端到端 A/B 验证（Phase 2）
> 所有结论均基于可复现实验数据，未实测处已明确标注。

---

## 一、Phase 1：代码级优化

### 1.1 审计发现的主要问题

| 类别 | 问题 | 位置 |
|---|---|---|
| 数据竞争 ×6 | `UpdateUser` 写 `AliveList`/`UUIDtoUID` 与 `CheckLimit` 并发读竞争；`GetOnlineDevice` 替换 `OldUserOnline` 指针与连接 goroutine 竞争；`UserLimitInfo.{ExpireTime,DynamicSpeedLimit,OverLimit}` 多 goroutine 无同步读写 | `limiter/limiter.go` 等 |
| 统计正确性 | sing 核心路径 `ConnCounter.Read/Write` 用 `Store` 而非 `Add`，流量被覆盖式少报（只记最后一包大小） | `common/counter/conn.go` |
| 热路径浪费 | 嗅探排除域名含 `regexp:` 时**每连接重新编译正则**（4248ns / 8.5KB / 46 allocs 每次匹配）；`strings.ToLower(domain)==d` 每连接分配 | `core/xray/app/dispatcher/default.go` |
| 热路径浪费 | 限速 bucket 已存在时仍每次新建再丢弃；`LoadOrStore` 失败后冗余二次 `Store` | `limiter/limiter.go` |
| 分配开销 | `format.UserTag` 用 `fmt.Sprintf` 构造全热路径 key | `common/format/user.go` |
| goroutine 风暴 | Hysteria2 `AddUsers/DelUsers` 为每用户创建一个 goroutine 只为做本就互斥的 map 写入（万级用户同步瞬时 10k goroutine） | `core/hy2/user.go` |
| 日志风暴 | 动态限速 `SpeedChecker` 成功时也打 Error 日志 | `node/task.go` |
| 定时器 | Task 循环用 `time.After` 每轮新建 timer | `common/task/task.go` |
| 行为笔误 | push 任务 interval 变更时误用 `PullInterval` 重启 | `node/task.go:162` |

### 1.2 修改清单（12 个 tracked 文件）

| 文件 | 修改 |
|---|---|
| `limiter/limiter.go` | `AliveList`/`UUIDtoUID` 改 COW + `atomic.Pointer`（读路径零锁零分配）；`OldUserOnline` 改原子指针快照；`UserLimitInfo` 热写字段改 atomic；限速 bucket Load-first；批量 COW 更新（无增删时零拷贝） |
| `limiter/dynamic.go` | 适配 atomic 字段 |
| `node/task.go` | AliveList 赋值改 `SetAliveList()`；修复 PushInterval 笔误；SpeedChecker 仅失败时记日志 |
| `common/counter/conn.go` | **`Store`→`Add` 计费修复** |
| `common/counter/traffic.go` | 新增 `AddBoth()`（单查找双计数） |
| `common/format/user.go` | `fmt.Sprintf` → 字符串拼接 |
| `common/task/task.go` | `time.After` → 复用 Timer（保持非阻塞 Close 语义） |
| `core/xray/app/dispatcher/default.go` | 正则编译缓存 `excludeRegexpCache`；`EqualFold` 替代 ToLower 比较 |
| `core/hy2/user.go` | 移除 goroutine-per-user，改串行循环 |
| `core/hy2/hook.go` | `OverLimit.Swap(false)`；`AddBoth` 合并查找 |
| `core/hy2/logger.go` | `OverLimit` 改 atomic Store |
| `cmd/server.go` | opt-in 诊断探针：`V2BX_PPROF_ADDR` 环境变量门控的 pprof/expvar 端点（默认零开销），供压测与线上诊断 |

### 1.3 微基准结果（Apple M3 Max / Go 1.25）

新增回归基准：`common/format/user_bench_test.go`、`common/counter/counter_bench_test.go`、`limiter/limiter_bench_test.go`、`core/xray/app/dispatcher/regex_bench_test.go`。

| Benchmark | Before | After | Δ |
|---|---:|---:|---:|
| CheckLimit @10k users（每连接） | 349.9 ns / 416 B / 8 allocs | **230.7 ns / 304 B / 6 allocs** | -34% / -27% |
| CheckLimit 含限速 bucket | 372.8 ns / 416 B / 8 | **219.7 ns / 304 B / 6** | -41% |
| UserTag | 52.7 ns | **25.7 ns** | -51% |
| 嗅探正则（每连接编译 vs 缓存） | 3565 ns / 8525 B / 46 | **405 ns / 0 B / 0** | -89% / -100% |
| AddLimiter 10k users | 3.64 ms / 3.74 MB / 64k allocs | **2.44 ms / 3.42 MB / 44k** | -33% / -31% |
| UpdateUser ±100 @10k | 61.9 µs | 595 µs* | 冷路径代价 |

\* COW 批量拷贝仅发生在 pull 间隔（默认 ≥60s）一次，换取每连接热路径免锁读取。
数据竞争：race detector 实证 6 处 → **0**（回归测试 `limiter/race_test.go`）。

---

## 二、Phase 2：端到端 A/B 验证

### 2.1 测试台架（`e2e/`，独立 go module，不影响主构建）

- mock Panel（UniProxy 全端点：config/user/alivelist/push/alive + ETag/304 + 延迟注入 + 故障注入）
- 内嵌真实 Xray 客户端（同一 xray-core 库，vless/tcp，SOCKS 入口 ×4 实例轮转多用户）
- 流量泵（满速 / `-mbps` 定速两种模式）+ 连接保持器
- 探针：runtime.MemStats / goroutine 总数 / ps RSS+CPUTime 采样
- 方法：baseline = HEAD worktree + 相同探针补丁；optimized = 工作树；同配置同时长多重复实验

### 2.2 A/B 结果汇总

| Metric | Baseline | Optimized | Change |
|---|---:|---:|---:|
| 吞吐（16 conns 满速 30s） | 21.24 Gbps | 21.36 Gbps | ±噪声 |
| CPU / Gbps | 1.51 s/Gbps | 1.48 s/Gbps | ≈0 |
| 场景A（1000u/100c/100Mbps×3 reps） | 全指标中位数一致 | 同左 | ≈0 |
| HeapAlloc @10k users idle | 13.6 MB | 13.7 MB | 曲线重合 |
| RAM / configured user | ~1047 B | ~1047 B | O(N) 线性、常数极小 |
| HeapObjects（场景A） | 125.9k | 128.7k | +2.3%（噪声） |
| RAM / active conn（空闲隧道） | ~40–75 KB | 同左 | 0 |
| goroutine / active conn | ~3–4 | 同左 | 0 |
| goroutine @users 100→10000 | 恒定 13 | 恒定 13 | O(1) ✓ |
| Data races | 6 | 0 | ✅ |
| Panel 延迟 5s / 宕机 | — | 吞吐 18.7→17.9/18.3 Gbps，无 goroutine 堆积 | 数据面隔离 ✅ |

**结论：通用吞吐与稳态内存收益 ≈0（符合 Amdahl 定律——被优化函数不在数据面稳态路径）。本组修改的价值在正确性与条件性场景。**

### 2.3 真实 pprof（20s 高负载）

- CPU：~93% syscall/kernel（readv/writev/kevent）；V2bX 业务层 flat≈0%；GC 1.18%
- allocs：90%+ 来自 xray-core `buf.New/Release`
- mutex：20s 窗口总等待仅 17ms
- goroutine：78 个全部 parked 等待，无泄漏

→ 高吞吐下瓶颈在内核网络栈与上游 core 自身，非 V2bX 业务层。

### 2.4 GC 调优实验（30s @21Gbps）

| 配置 | Gbps | CPU s | GC 次 | Pause 合计 | RSS |
|---|---:|---:|---:|---:|---:|
| GOGC=100（默认） | 21.36 | 31.6 | 175 | 14.3ms | 51M |
| GOGC=50 | 21.36 | 33.3 (+5%) | 410 | 40ms | **46M (-10%)** |
| GOGC=200 | 21.20 | 31.2 | **82 (-53%)** | 7.3ms | 56M (+10%) |
| GOMEMLIMIT=250/400MiB | ≈默认 | ≈默认 | ≈默认 | — | live heap 小，不触发 |

建议：内存紧张机型 `GOGC=50`（10% RSS 换 5% CPU）；充裕机型 `GOGC=200`；`GOMEMLIMIT` 仅作失控保护。不建议硬编码进程序。

### 2.5 关键容量指标（macOS 实测外推，Linux 待复测）

```
RAM / configured user      ≈ 1 KB（V2bX 层 ~396B + xray 协议对象）
RAM / active connection    ≈ 40–75 KB（空闲隧道，xray bufferSize=64 默认）
goroutine / connection     ≈ 3–4
goroutine / configured user ≈ 0（O(1)）
```

---

## 三、ROI 分类与最终建议

| 级别 | 项目 |
|---|---|
| **HIGH**（可靠性/正确性） | race×6 修复；sing 核心计费 Store→Add 修复 |
| **MEDIUM**（条件性性能） | regexp 规则缓存（有规则部署每连接省 4.2µs+8.5KB alloc）；hy2 万级用户同步 burst 消除；日志风暴修复 |
| **LOW**（微收益，实测整体不可见） | UserTag/EqualFold/bucket/AddBoth/Timer |

**✅ 建议提交** —— 定位为"正确性修复 + 条件性性能优化"，实测零回归。
注意：不要以"整体吞吐/RAM 下降 xx%"描述本次修改——端到端数据显示通用收益≈0。

## 四、遗留问题（预先存在，未在本次修改范围）

1. `buildInbound` 对纯 TCP 且无 `network_settings` 的节点 nil 解引用 panic（`core/xray/inbound.go:69`）
2. `TestConf_Watch` 挂起、`TestLego_CreateCertByDns` 依赖真实 ACME 凭证（CI 环境）
3. Linux VPS 上 RSS 绝对值复测（macOS 归还语义不同，相对结论不受影响）

## 五、复现方式

```bash
# 构建（需要 build tags 选择 core）
GOEXPERIMENT=jsonv2 go build -tags "xray" -o V2bX .

# 单元测试 / race / vet
GOEXPERIMENT=jsonv2 go test ./... 
GOEXPERIMENT=jsonv2 go test -race ./limiter/ ./common/task/
GOEXPERIMENT=jsonv2 go vet ./...

# 微基准
GOEXPERIMENT=jsonv2 go test -run=NONE -bench=. ./limiter/ ./common/... ./core/xray/app/dispatcher/

# 端到端台架
cd e2e && GOEXPERIMENT=jsonv2 go run ./harness \
  -bin ./V2bX -label opt -scenario tput -users 1000 -conns 16 -secs 30 -out results
```
