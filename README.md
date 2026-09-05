# OPGate — OpenCode Session Gateway

轻量、高性能的 Go HTTP 反向代理，部署在 OpenCode API（OpenAI-compatible）前面。
为 **RikkaHub / Python httpx / requests** 等无法发送 `x-opencode-session` 的第三方客户端
自动注入并维护稳定的会话 ID，使同一 conversation 的所有请求复用同一个 session。

> 定位：**OpenCode API Reverse Proxy + Session Affinity Layer + Streaming Response Spill Cache**。
> 网关不保存 conversation history —— 客户端永远是历史的权威来源。

```
HTTP Request
     │
     ▼
SessionResolver          Hash(messages[:-1]) → Redis lookup
     │                       miss / error / 首条消息 → 生成 UUID（绝不阻塞）
     ▼
Session S1 ──► X-Opencode-Session: S1 ──► OpenCode API
     │
     │ SSE（旁路，Redis 不在实时路径上）
     ├──────────────► Client（实时 flush）
     └──────────────► ResponseCollector
                          │  RAM page buffer (4MiB)
                          ▼  PAGE_SIZE reached
                      异步 Redis page 写入（固定 worker 池 + 有界队列）
                          │
                       [DONE]
                          ▼
                      Finalize：等待在途页 → HGETALL → 按 seq 排序
                      → pages + RAM tail → 解析 assistant message
                          │
                          ▼
              Hash(full conversation state) → S1 写入 Redis
```

## 快速开始

```bash
go build -o ocgate ./cmd/ocgate
cp config.example.yml config.yml   # 修改 upstream.base_url 后启动
./ocgate -config config.yml
```

客户端只需把 base URL 指向网关（例如 `http://127.0.0.1:8080/v1`），
不需要任何 `x-opencode-session` 适配。

## 配置

```yaml
server:
  listen: ":8080"
upstream:
  base_url: "https://opencode.example.com/v1"   # 路径前缀会被保留
redis:
  addr: "127.0.0.1:6379"
  db: 0
  timeout: 2s
session:
  ttl: 168h            # stateHash → sessionID 映射 TTL，默认 7 天
response_cache:
  page_size: 4MiB      # RAM 页大小（1MiB–8MiB 建议）
  ttl: 30m             # 临时分页 TTL（须明显小于 session.ttl）
  workers: 4           # 页写入 worker 数（固定）
  max_pending_pages: 8 # 有界队列深度，防止 Redis 慢时 RAM 无限增长
limits:
  max_request_body: 32MiB
logging:
  level: info          # debug|info|warn|error
```

## 核心设计

### Session 算法

| 场景 | 行为 |
|---|---|
| 请求带 `x-opencode-session` | 原样透传，绝不覆盖；响应成功后补建 `Hash(full state) → S1` |
| `messages.length == 1` | 新 UUID；**不**建 `Hash(U1)→S1`，成功后只建 `Hash(U1,A1)→S1` |
| `messages.length > 1` | `stateHash = Hash(messages[:-1])` 查 `oc:v1:session:{clientHash}:{stateHash}`；命中复用，miss 生成新 UUID |
| Redis 故障 / miss | 生成 UUID 继续转发，网关永远可用 |

成功完成的响应才会建立映射；流中断、上游错误、缓存写失败、缺 `[DONE]`
一律 **Abort**，宁可下次 session miss 也不写错误映射（§22/§23）。

### Conversation State Hash

`Conversation State → Canonical JSON → SHA-256 → stateHash`，
session ID 永远不参与哈希。Canonical JSON 规则：

- UTF-8、紧凑；对象键按字节序排序（重复键取最后）
- **数字保留原始词法形式**（`1`、`1.0`、`1e0` 哈希不同——确定性优先）
- 字符串最小转义（引号/反斜杠/控制字符），其余码点原样输出
- 数组顺序有意义；字段缺失 ≠ 显式 null

Assistant message 以客户端回显的形态重建（`{"content":...,"role":"assistant"[,"tool_calls":[...]]}`），
保证下一轮客户端携带完整历史时能命中映射。内容为结构化数组（多模态）时
无法可靠重建，跳过建映射。

### Redis 数据（仅两类）

```
oc:v1:session:{clientHash}:{stateHash} → sessionID   (SET EX 168h)
oc:v1:tmp:{requestID}                  → Hash{000001:页1, 000002:页2, ...} (EXPIRE 每次刷新)
```

- `clientHash = SHA-256(Authorization / X-Api-Key / Api-Key / "anonymous")`，
  原始凭证不进 key、不进日志
- 分页字段 `000001` 起递增，Finalize 按 seq 排序拼接，**最后一页留在 RAM tail**
- 版本前缀 `oc:v1` 便于未来 schema 升级

### SSE 旁路（性能红线）

`Redis 永远不出现在客户端 SSE 实时输出的关键路径上`：

1. 上游 chunk → 立即写给客户端（`FlushInterval: -1`）
2. 同一份数据旁路给 Collector：仅追加 RAM 页缓冲（纳秒级）
3. 满 PAGE_SIZE 才经有界队列交固定 worker 异步写 Redis
4. Redis 写失败 / 队列满 → 标记 `failed`，丢弃后续缓存、保护 RAM，
   客户端 SSE 继续不受影响；Finalize 将拒绝建映射

已知边界（§19/§59）：Redis 长期跟不上时选择资源保护而不是无限缓冲；
两个 conversation 历史完全相同时网关无法区分（协议层面限制，接受）。

## Metrics

`/metrics`（Prometheus，`opgate_` 命名空间）：session_resolve_hit / miss /
client_supplied，stream_completed / aborted，page_flush_success / error，
finalize_success / error，session_mapping_created / error，
stream_duration、response_bytes、page_count、redis_latency、finalize_duration。
另有 `/healthz`。

## 测试

覆盖 DEVELOP.md §57 全部场景（单元 + miniredis/httptest 端到端）：

```bash
go test ./... -count=1
```

- Test 1–6：首条请求 / 命中 / 多轮 / branching / 相同首条消息 / 相同完整历史
- Test 7/9：长流分页 + RAM tail 拼接（20MB 响应 ≈ 19.5MB/4MiB 页）
- Test 8：最后一页 in-flight 竞态（Finalize 等待，不丢数据）
- Test 10：Redis 页写失败 → SSE 照常、Finalize 失败、不建映射
- Test 11：上游断流 → Abort、不建映射
- Test 12：客户端自带 session 透传且补建映射
- Test 13：Redis 不可用 → 仍生成 UUID 转发
- Test 14：`stream=false` → 无分页、直接建映射
- 并发（§46）：4 个 conversation 并行互不干扰
- 实时性（§15）：客户端在 upstream 结束前就开始收到数据

注：Finalize 阶段会一次性拼装完整原始 SSE 流用于解析 assistant message
（§24 的设计取舍），网关的稳态 RAM 由 `page_size + max_pending_pages` 决定，
与响应总大小无关。

## 日志与安全

- 结构化日志含 `request_id / session_id / state_hash / client_hash`（均短前缀）
- 不记录 Authorization / API Key / 消息内容
- 请求体大小受限（413）；内部 requestID 不暴露给上游；临时数据强制 TTL

## 部署

### Docker（GHCR 多架构镜像）

镜像：`ghcr.io/ccbkkb/opgate`，支持 `linux/amd64`、`linux/arm64`、`linux/arm/v7`，
兼容 Debian/Ubuntu/Fedora/Arch 与 Alpine（静态二进制，无 libc 依赖）。

```bash
# 固定版本
docker pull ghcr.io/ccbkkb/opgate:v0.1.0
# 或 latest
docker pull ghcr.io/ccbkkb/opgate:latest

docker run -d --name ocgate -p 8080:8080 \
  -v $PWD/config.yml:/etc/ocgate/config.yml:ro \
  ghcr.io/ccbkkb/opgate:latest
```

或使用 docker compose（含 Redis）：

```bash
cp config.example.yml config.yml   # 修改 upstream.base_url
docker compose up -d
```

容器内配置路径为 `/etc/ocgate/config.yml`（只读挂载即可），监听端口默认 `:8080`，
自带 `/healthz` HEALTHCHECK。

### 二进制下载

从 [GitHub Releases](https://github.com/ccbkkb/OPGate/releases) 下载对应平台包：

| 平台 | 文件 |
|---|---|
| Linux x86_64 / arm64 / armv7 / 386（静态，Debian/Ubuntu/Alpine 等通用） | `ocgate_<ver>_linux-*.tar.gz` |
| macOS Intel / Apple Silicon | `ocgate_<ver>_macos-*.tar.gz` |
| Windows amd64 / arm64 | `ocgate_<ver>_windows-*.zip` |
| Android（Termux） | `ocgate_<ver>_android-arm64.tar.gz` |

```bash
tar -xzf ocgate_<ver>_linux-amd64.tar.gz
sha256sum -c SHA256SUMS.txt --ignore-missing   # 校验
./ocgate -version
```

Termux 用户：

```bash
tar -xzf ocgate_<ver>_android-arm64.tar.gz && chmod +x ocgate && ./ocgate -config config.yml
```

## 发布流程（自动化）

推送 `v*` 标签即自动完成（`.github/workflows/release.yml`）：

1. **交叉编译** 9 个平台的二进制（linux amd64/arm64/armv7/386、macos amd64/arm64、
   windows amd64/arm64、android arm64），`CGO_ENABLED=0` 静态链接，
   版本号通过 `-ldflags "-X main.version=..."` 注入；
2. **构建多架构 Docker 镜像** 并推送到 `ghcr.io/ccbkkb/opgate`
   （`vX.Y.Z`、`vX.Y`、`vX`、`latest` 多标签）；
3. **创建 GitHub Release**：自动生成完整的 release 说明正文（下载表、Docker 用法、
   SHA256 校验、快速开始、自上个 tag 以来的完整变更列表），并上传全部产物与 `SHA256SUMS.txt`。

```bash
git tag v0.1.0 && git push origin main --follow-tags
```

普通 push/PR 由 `ci.yml` 执行 build + vet + test。

## 冒烟测试（真实二进制 + 真实 TCP Redis）

```bash
go build -o ocgate ./cmd/ocgate
(cd scripts/smoke && go mod tidy && go run .)   # SMOKE OK 即通过
```

覆盖：配置加载、监听、两轮真实 HTTP 请求的 session 复用（new → resolved）、
metrics 输出、SIGTERM 优雅退出。
