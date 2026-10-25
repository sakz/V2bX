# 动态限速与 Hysteria2 用户限速设计

## 背景

当前代码里的动态限速配置（`EnableDynamicSpeedLimit` / `DynamicSpeedLimitConfig`）只完成了字段定义和部分骨架：

- `node/task.go` 创建了 `dynamicSpeedLimitPeriodic`，但从未调用 `Start`。
- `reportUserTrafficTask` 没有把用户流量累计到 `c.traffic`。
- `SpeedChecker` 即使被调用也拿不到数据。
- `UpdateDynamicSpeedLimit` 只更新 `UserLimitInfo`，不会刷新连接已经持有的 `ratelimit.Bucket`，所以已有的长连接不会降速。
- 动态限速到期时，如果用户没有静态限速，当前 `CheckLimit` 会删除 `UserLimitInfo`，后续连接会被当作未知用户拒绝。
- Hysteria2 的 `CheckLimit` 返回值被丢弃，`speed_limit` 实际没有应用到流量上。

## 目标

1. 让 `EnableDynamicSpeedLimit` 成为可用功能，语义与文档一致。
2. 动态限速触发和到期时，已有长连接立即生效。
3. 让 Hysteria2 的用户级 `speed_limit` 和动态限速真正应用到 TCP/UDP 流量。
4. 不破坏未开启动态限速的节点，尤其是 xray 的 splice 性能路径。

## 非目标

- 不实现跨节点全局带宽配额。限速仍然按节点实例、按用户 UUID 独立执行。
- 不改变面板节点级 `up_mbps/down_mbps` 的 Hy2 语义。
- 不实现滑动窗口或跨窗口累计。

## 动态限速语义

配置字段：

| 字段 | 单位 | 含义 |
| --- | --- | --- |
| `Periodic` | 秒 | 统计窗口长度，也是检查任务间隔 |
| `Traffic` | 字节 | 单个窗口内触发限速的流量阈值 |
| `SpeedLimit` | Mbps | 触发后应用的限速值 |
| `ExpireTime` | 分钟 | 动态限速持续时间 |

规则：

- 每 `Periodic` 秒结算一次，只统计本窗口内新增的 `Upload + Download`。
- 窗口内流量 `>= Traffic` 时，为该用户设置动态限速。
- 动态限速持续 `ExpireTime` 分钟，到期后回落到“静态用户限速 / 节点限速 / 不限速”。
- 窗口结束时无论是否触发都清零，避免低流量用户长期累积后被误限速。
- 同一窗口内持续触发会重新计算到期时间，相当于续期。

配置校验：`Periodic > 0`、`Traffic > 0`、`SpeedLimit > 0`、`ExpireTime > 0`。不合法时记录错误日志并不启动动态限速任务。

## 核心组件

### 1. `limiter.SpeedBucket`

新增一个可动态刷新的限速桶，替代 `Limiter.SpeedLimiter` 中直接保存的 `*ratelimit.Bucket`。

职责：

- 持有静态速率（字节/秒）、动态速率（字节/秒）、动态到期时间。
- 实现 `Wait(count int64)`，供 xray、sing、Hy2 的传输路径调用。
- 速率变化时替换内部令牌桶，但 `SpeedBucket` 指针保持不变，因此连接不需要重连。
- `Wait` 时检查动态限速是否过期；过期后自动回落到静态速率。
- 速率为 0 时 `Wait` 直接返回，不产生令牌桶开销。

`Limiter.CheckLimit` 返回类型从 `*ratelimit.Bucket` 改为 `*limiter.SpeedBucket`。

`Limiter` 增加 `dynamicEnabled` 标记：

```go
dynamicEnabled = EnableDynamicSpeedLimit && DynamicSpeedLimitConfig.Valid()
```

当 `dynamicEnabled` 为 true 时，即使当前有效速率为 0，`CheckLimit` 也返回非空 `SpeedBucket`。这样已有连接从建立开始就持有一个可刷新对象，后续动态限速可以直接作用到它。

### 2. `common/rate.Waiter`

`common/rate` 不再依赖具体的 `*ratelimit.Bucket`，改为接受接口：

```go
type Waiter interface {
    Wait(count int64)
}
```

`rate.Conn` 和 `rate.Writer` 都改为持有 `Waiter`。这样 `limiter.SpeedBucket` 可以直接嵌入现有 xray/sing 限速包装，不产生包循环依赖。

### 3. 动态流量窗口 `node.trafficWindow`

新增一个小型并发安全结构：

```go
type trafficWindow struct {
    mu      sync.Mutex
    traffic map[string]int64 // uuid -> bytes
}
```

方法：

- `Add(uuid string, n int64)`
- `Swap() map[string]int64`：原子换出一个新 map，作为本次检查窗口。
- `Delete(uuid string)`
- `Reset()`

`Controller` 在动态限速开启时初始化它。`nodeInfoMonitor` 检测到节点配置变化时调用 `Reset`。

### 4. 用户流量的 UUID

当前 `panel.UserTraffic` 只有 `UID`，没有 `UUID`。动态限速需要按 UUID 找到 `UserLimitInfo`。给该结构增加内部字段：

```go
UUID string `json:"-" msgpack:"-"`
```

三个核心在生成 `UserTraffic` 时填入 UUID；面板上报仍只使用 UID，不影响协议。

## 数据流

### 流量累计

`reportUserTrafficTask`：

1. 调用 `server.GetUserTrafficSlice(tag, true)` 获取并重置核心流量计数。
2. 上报面板。
3. 如果动态限速开启，把每条 `UserTraffic` 的 `Upload + Download` 累加到 `trafficWindow`。

`GetUserTrafficSlice` 受 `MinReportTraffic` 影响：低于阈值的流量会在核心侧继续累计，直到超过阈值后一次性返回。因此不会丢失或重复计入动态窗口，只是统计会延后。

### 周期检查

`SpeedChecker`：

1. `traffic := trafficWindow.Swap()`。
2. 遍历窗口内用户，比较 `traffic[uuid] >= Traffic`。
3. 达到阈值时调用 `limiter.UpdateDynamicSpeedLimit`，设置动态速率和到期时间。
4. 记录触发日志。
5. 未达到阈值的用户随窗口一起丢弃，下个窗口重新统计。

### 限速触发与到期

`UpdateDynamicSpeedLimit`：

1. 更新 `UserLimitInfo.DynamicSpeedLimit` 和 `ExpireTime`。
2. 查找或创建用户的 `SpeedBucket`。
3. 调用 `SpeedBucket.SetDynamic(...)`，替换内部令牌桶。

`SpeedBucket.Wait`：

1. 如果动态限速已过期，清除动态速率并回落到静态速率。
2. 调用当前令牌桶的 `Wait`。

`CheckLimit` 在用户连接时也会处理一次过期：

- 过期后只清零 `DynamicSpeedLimit` 和 `ExpireTime`。
- 不再删除 `UserLimitInfo`，保留设备数限制等静态信息。

## Hysteria2 接入

在 `HookServer.LogTraffic(id, tx, rx)` 中：

1. 保留现有 `OverLimit` 判断。
2. 通过 `limiter.GetSpeedBucket(id)` 获取用户限速桶。
3. 调用 `bucket.Wait(int64(tx) + int64(rx))`。
4. 再累计流量并返回 true。

效果：

- Hy2 TCP 和 UDP 都受用户级 `speed_limit` 与动态限速约束。
- 上传和下载共用同一个额度，与 xray/sing 行为一致。
- 已有 Hy2 连接无需重连，每次 `LogTraffic` 都会取到最新桶。

## xray / sing 接入

### sing

- 动态限速开启时，`CheckLimit` 对所有有效用户返回非空 `SpeedBucket`。
- `RoutedConnection` 始终包装限速连接。
- `RateLimitWriter` / `Conn` 每次读写调用 `SpeedBucket.Wait`，速率变化立即生效。
- UDP packet 路径当前没有接入限速，保持不变。

### xray

- 动态限速开启时，`CheckLimit` 对所有有效用户返回非空 `SpeedBucket`。
- TCP 的上下行 writer 都包装动态限速 writer。
- UDP 的 outbound writer 同样包装。
- 因为限速 writer 不能与 splice 共存，动态限速开启的节点会把 `CanSpliceCopy` 设为 3。
- 未开启动态限速的节点行为不变：有静态/节点限速时仍按现有逻辑关闭 splice，无任何限速时保留 splice。

## 并发与一致性

- `SpeedBucket` 内部用 `sync.Mutex` 保护速率切换；`Wait` 快速路径只做原子读取，不持有锁等待。
- `trafficWindow` 用 `sync.Mutex` 保护，`Swap` 在锁内换 map。
- `Controller.limiter` 改为 `atomic.Pointer[limiter.Limiter]`，避免节点重载与动态检查任务并发读写指针。
- `Limiter` 增加 `Tag` 字段，动态检查任务通过当前 limiter 读取 tag，避免节点重载时 tag 读写竞争。

## 错误处理

- 动态配置不合法：记录错误日志，禁用动态限速任务，保留普通用户/节点限速。
- `UpdateDynamicSpeedLimit` 找不到用户：记录 debug 日志，不中断检查任务。
- Hy2 取不到 limiter 或 bucket：跳过限速，但保持现有流量统计和断连逻辑。
- 面板上报失败不影响动态窗口统计：流量已经发生，仍然计入当前窗口。

## 测试计划

### limiter

- 静态限速 + 动态限速同时存在时取较小值。
- 动态限速更新后，已有 `SpeedBucket` 的速率立即变化。
- 动态限速过期后回落到静态限速。
- 静态限速为 0 时，动态限速过期后回落到不限速。
- 动态到期不会删除 `UserLimitInfo`，后续连接不会被拒绝。
- 动态限速开启但当前无限制时，`CheckLimit` 返回非空可刷新桶。
- 现有 `TestRepeatedOnlineIPAllocations` 保持 0 分配。

### node

- `trafficWindow` 的 Add/Swap/Delete/Reset。
- 窗口内流量低于阈值不触发，且窗口被清零。
- 达到阈值触发动态限速。
- 用户删除后窗口记录被清理。
- 检查任务与流量累计并发运行时无数据竞争（`-race`）。

### 集成

- `go test -race ./limiter ./node`
- `go test ./...`（按项目可编译的 tags）
- 手动验证 Hy2 用户静态限速：给用户设置较小 `speed_limit`，观察 TCP/UDP 吞吐。

## 兼容性

- 配置字段和单位保持不变。
- 动态限速从“不可用”变为“可用”，开启后行为符合文档。
- Hy2 用户级限速从“未生效”变为“生效”，已有 `speed_limit` 配置的用户会开始受到限制。
- 动态限速开启的 xray 节点会关闭 splice，这是保证已有连接即时变速的代价；未开启节点不受影响。
