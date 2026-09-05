OpenCode Session Gateway （OPGate）

---

ps：

一切都源于:

> Hey there,
> 
> Some of your requests to OpenCode Go are missing an x-opencode-session header. If we don't have this we cannot properly optimize our service. Starting 09/06 requests missing this header may error.
> 
> Here are your useragents that are missing this header:
> 
>> RikkaHub-Android/2.4.15
> We don't recognize this client — add x-opencode-session (one stable ID per conversation) or ask its maintainer to.
> 
> Python httpx
> Add x-opencode-session (one stable ID per conversation) to your requests.
> 
> Thank you.

我需要一个高性能的反代网关，让那些没有适配`x-opencode-session`的第三方客户端也能继续使用 OpenCode API。
另外，Redis 哈希缓存可以在配置文件中设置缓存有效期，默认 24*7=168h

---

1. 项目目标

开发一个轻量、高性能、易维护的 Go HTTP Reverse Proxy，用于代理 OpenCode API。

项目的核心目的：

«对不支持 "x-opencode-session" 的第三方 OpenAI-compatible 客户端，在网关层自动补全并维护稳定的 "x-opencode-session"，使同一个 conversation 的多次请求能够复用同一个 session。»

典型客户端包括但不限于：

- RikkaHub
- OpenAI-compatible Python 客户端
- 自己编写的 "httpx" / "requests" 客户端
- 其他无法自定义 "x-opencode-session" 的客户端

网关本身不负责保存完整 conversation history。

客户端仍然是 conversation history 的权威来源。

网关只维护：

Conversation State Hash → OpenCode Session ID

并在流式响应过程中临时缓存 assistant response，以便在响应完成后计算新的 conversation state hash。

---

2. 核心设计原则

必须遵守以下原则。

2.1 Session ID 和 Request ID 完全不同

Session ID

表示一个 conversation。

例如：

S1 = 550e8400-e29b-41d4-a716-446655440000

同一个 conversation 的多个 HTTP request 可以使用同一个 session：

Request R1 → Session S1
Request R2 → Session S1
Request R3 → Session S1

Request ID

只表示一次 HTTP 请求。

例如：

R1 = 0b9d...
R2 = 7a31...

每一次 HTTP 请求都生成新的 request ID。

Request ID 只用于临时 SSE response page cache：

oc:v1:tmp:{requestID}

request 完成并 finalize 后，临时数据应该被删除。

---

3. Session 算法

这是整个项目最核心的逻辑。

假设客户端发送：

{
  "messages": [
    {"role": "user", "content": "U1"}
  ]
}

---

3.1 客户端主动提供 "x-opencode-session"

如果请求已经包含：

x-opencode-session: S1

那么：

«必须尊重客户端提供的 session ID。»

不要覆盖。

直接向 OpenCode 转发：

x-opencode-session: S1

可以在响应正常完成后建立：

Hash(full conversation state) → S1

这样客户端未来即使某一次请求没有携带 header，网关仍然可能恢复到该 session。

---

4. 客户端没有提供 Session 时

4.1 第一条消息

如果请求没有 "x-opencode-session"，且：

messages.length == 1

那么认为这是一个新 conversation。

立即生成：

S1 = UUID

然后向 OpenCode 转发：

x-opencode-session: S1

注意：

«首条请求不需要建立 "Hash(U1) → S1" 的正式 mapping。»

只需要在本次 AI 响应成功完成以后建立：

Hash(U1 + A1) → S1

其中：

- "U1" = 用户第一条消息
- "A1" = OpenCode 完整返回的 assistant message

---

5. 后续请求的 Session Resolution

假设客户端下一次发送：

U1
A1
U2

网关认为最后一条：

U2

是本次新提交的 user message。

因此：

history = messages[:-1]

得到：

U1
A1

计算：

stateHash = Hash(U1, A1)

然后查询：

oc:v1:session:{clientHash}:{stateHash}

如果命中：

→ S1

本次请求使用：

x-opencode-session: S1

---

5.1 查询 miss

如果：

Hash(messages[:-1])

没有找到对应 session：

S1 = UUID()

继续转发请求。

这意味着：

«Session resolution miss 不应该阻塞 OpenCode 请求。»

Redis 故障或 mapping miss 不应该让整个 OpenCode API 不可用。

---

6. 为什么使用 "messages[:-1]"

不要扫描所有历史前缀。

不要：

Hash(U1)
Hash(U1,A1)
Hash(U1,A1,U2)
Hash(U1,A1,U2,A2)
...

然后寻找最长匹配。

正常 OpenAI-compatible 客户端会在每次请求中携带完整 conversation history。

因此第二次请求：

[U1,A1,U2]

直接去掉最新消息：

[U1,A1]

即可定位之前已经完成的 conversation state。

这样算法非常简单：

请求进入：
    Hash(messages[:-1])
        ↓
    Redis GET
        ↓
    得到 session

响应成功完成：

完整 messages + assistant response
        ↓
    Hash(...)
        ↓
    Redis SET

---

7. Conversation State Hash

核心关系必须保持：

Conversation State
        ↓
Canonical JSON
        ↓
SHA-256
        ↓
State Hash
        ↓
Session ID

即：

stateHash → sessionID

绝对不要：

Hash(messages + sessionID)

Session ID 不属于 conversation state。

---

8. Canonical Message

Hash 不能只针对：

message.content

必须考虑完整 Message。

至少需要正确处理：

- "role"
- "content"
- "name"
- "tool_calls"
- "tool_call_id"
- 其他 OpenAI-compatible message 字段

"content" 可能是：

string

也可能是：

array

例如多模态消息。

因此不要把 "content" 强制当成 string。

建议定义稳定的内部 message representation，并使用稳定的 canonical JSON 序列化规则。

需要保证：

«相同语义的 message 得到相同 hash。»

至少要明确：

- 字段顺序
- 空字段处理
- null 与不存在字段的区别
- 数组顺序
- UTF-8
- JSON number 表示

不要让 Go struct 的偶然序列化行为决定协议 hash。

---

9. Client Identity

Redis key 必须区分不同客户端/用户。

例如：

oc:v1:session:{clientHash}:{stateHash}

其中：

clientHash = SHA-256(client identity)

client identity 可以来自请求的认证信息。

例如 API Key / Authorization token。

但是：

«绝对不能把原始 Authorization 或 API Key 写入 Redis key。»

推荐：

SHA256(auth credential)

如果请求没有认证信息，则需要定义稳定的匿名 client identity。

不要使用 IP 作为唯一身份。

---

10. 相同 Conversation 的碰撞

这是设计上的重要边界。

假设 Chat A 和 Chat B 都开始：

U1 = "你好"

因为这是两个独立首条请求：

Chat A → S1
Chat B → S2

如果最终：

Chat A:
U1 → A1

Chat B:
U1 → A1

那么：

Hash(U1,A1)

完全相同。

此时两个 mapping 可能指向同一个 session。

这不是必须修复的问题。

原因：

«如果两个 conversation 的完整历史完全相同，那么从网关收到的 HTTP 数据中无法区分它们。»

一旦 conversation 开始产生不同历史：

Chat A:
U1,A1,U2A

Chat B:
U1,A1,U2B

新的 state hash 就会分开。

因此：

«不要为了理论上的完全相同 conversation 引入复杂的 conversation ID、连接状态或本地 session manager。»

保持算法简单。

---

11. Branching / 编辑历史

必须支持 conversation branching。

例如：

U1
A1
U2
A2

用户修改成：

U1
A1
U3

本次 request：

messages[:-1]
=
U1,A1

可以命中：

Hash(U1,A1) → S1

因此继续使用：

S1

新响应：

A3

最终：

Hash(U1,A1,U3,A3) → S1

原来的：

Hash(U1,A1,U2,A2) → S1

可以继续存在。

因此 state hash mapping 天然形成 conversation tree：

             U1,A1
            /     \
          U2,A2   U3,A3

不同 branch 可以复用同一个 OpenCode session ID。

不要主动删除旧 state mapping。

只使用 TTL 清理长期不用的 mapping。

---

12. Redis 数据结构

整个项目尽量只使用两类 Redis 数据。

---

12.1 正式 Session Mapping

Key：

oc:v1:session:{clientHash}:{stateHash}

Value：

sessionID

例如：

SET oc:v1:session:a81f...:72ab... "550e8400-e29b..."
EX 604800

默认 TTL：

7 days

可以配置。

---

12.2 临时 Response Page

使用 Redis Hash。

Key：

oc:v1:tmp:{requestID}

Fields：

000001 → page data
000002 → page data
000003 → page data

例如：

HSET oc:v1:tmp:R1 000001 <bytes>
HSET oc:v1:tmp:R1 000002 <bytes>
HSET oc:v1:tmp:R1 000003 <bytes>
EXPIRE oc:v1:tmp:R1 1800

这样不需要大量 Redis key。

Finalize 时：

HGETALL oc:v1:tmp:R1

按照 sequence number 排序。

最后：

DEL oc:v1:tmp:R1

---

13. 临时 Page Cache 的目的

不能把整个长时间 SSE 响应保存在 RAM。

AI 可能出现：

«十几分钟甚至更长时间的 reasoning / thinking。»

如果持续：

SSE → RAM
SSE → RAM
SSE → RAM
...

可能导致单请求占用大量内存，最终 OOM。

因此采用：

SSE
 ↓
小型 RAM buffer
 ↓
达到 PAGE_SIZE
 ↓
异步写 Redis
 ↓
清空 RAM buffer

---

14. Page Size

默认：

response_cache:
  page_size: 4MiB

建议允许配置：

1MiB
2MiB
4MiB
8MiB

第一版默认 4 MiB。

不要默认使用几十 MB。

---

15. SSE 旁路原则

这是整个项目最重要的性能要求之一。

数据流必须是：

OpenCode
    │
    │ SSE
    ▼
Gateway
    │
    ├──────────────→ Client
    │                实时收到
    │
    └──────────────→ Response Collector
                       │
                       ▼
                     Redis

绝对不能设计成：

OpenCode
 ↓
Redis
 ↓
Client

也就是说：

«Redis 永远不能位于客户端 SSE 实时输出的关键路径上。»

---

16. Client SSE 必须优先

OpenCode 发来一个 SSE chunk 后：

1. 尽快写给客户端
2. 同时旁路交给 collector
3. collector 的 Redis 操作不能阻塞客户端输出

不能因为：

Redis SET latency = 100ms

导致：

Client SSE latency = 100ms

---

17. 不要每个 SSE 写 Redis

错误设计：

SSE #1 → Redis
SSE #2 → Redis
SSE #3 → Redis
...

正确：

SSE #1
SSE #2
SSE #3
...
SSE #N
   ↓
RAM page 达到 4 MiB
   ↓
一次 Redis write

Redis write 次数应该约等于：

response_size / page_size

而不是 SSE event 数量。

---

18. Page Flush Worker

不要每一个 page 都无限制启动 goroutine。

推荐固定数量 worker。

例如：

response_cache:
  workers: 4

结构：

Collector
    ↓
bounded page queue
    ↓
┌───────┬───────┬───────┬───────┐
│Worker1│Worker2│Worker3│Worker4│
└───────┴───────┴───────┴───────┘
    ↓
 Redis

同时限制：

max_pending_pages: 8

避免 Redis 故障时无限堆积 goroutine / RAM。

---

19. 一个重要的资源边界

理论上无法同时保证：

1. SSE 永远完全不受 Redis 速度影响
2. RAM 永远固定上限
3. Redis 永远不可用也不丢任何数据

例如：

AI output = 100 MB/s
Redis throughput = 10 MB/s

必然会产生 backlog。

因此第一版应该明确：

«正常情况下，Redis 是高速临时存储；如果 Redis 长时间无法跟上，应进行资源保护，而不是无限增长 RAM。»

第一版不要求实现本地磁盘 fallback。

---

20. Finalize 时的 Race Condition

这是必须正确实现的地方。

例如：

page #10

正在异步写 Redis。

同时收到了：

[DONE]

不能立即：

HGETALL

否则 page #10 可能还没写完。

正确流程：

收到 [DONE]
    ↓
停止产生新的 page flush
    ↓
seal 当前 RAM tail
    ↓
等待所有已经提交的 page flush 完成
    ↓
HGETALL 临时 Redis Hash
    ↓
按 seq 排序
    ↓
pages + RAM tail
    ↓
得到完整 assistant response

---

21. 最后一页不要强制写 Redis

如果 SSE 结束时 RAM 中还有：

300 KiB

不要为了形式统一再执行一次 Redis write。

直接：

Redis pages
+
RAM tail

拼接。

这样可以少一次 Redis 操作。

所以 Finalize 的最终数据来源是：

completed Redis pages
+
current RAM tail

---

22. "Finalize()" 的语义

Finalize 只允许在正常 stream completion 时调用。

典型流程：

HTTP 2xx
+
SSE 正常完成
+
收到 [DONE]

然后：

Finalize

如果：

- OpenCode 返回 HTTP error
- SSE 中途断开
- TCP connection reset
- 上游 context canceled
- collector 数据不完整
- Redis page flush 失败

则：

Abort

不要建立新的：

stateHash → sessionID

mapping。

---

23. 为什么失败时不能“猜”

例如：

真实 A1：

hello world

但 Redis page 丢了一页，网关只拿到了：

hello

绝对不能：

Hash(U1,"hello") → S1

因为这样会污染正式 session mapping。

正确行为：

Finalize failed
    ↓
不更新 session mapping
    ↓
删除/等待 TTL 清理临时缓存

宁可下一次 request session miss，也不能产生错误 mapping。

---

24. SSE 数据保存策略

第一版建议：

«Redis page 保存 SSE 原始 payload bytes，而不是保存自定义的 assistant message JSON。»

原因：

- 不需要在每个 chunk 上重新构造 message
- 避免网关改变 OpenAI-compatible response
- 更容易兼容未来新的 SSE delta 字段
- 出问题时可以更容易 debug

Collector 在转发 SSE 的同时，可以保存原始 SSE payload。

Finalize 时：

pages
 ↓
拼接完整 SSE stream
 ↓
统一解析
 ↓
提取 assistant message

---

25. Assistant Message 不能只提取 "delta.content"

不能假设：

assistant response = 所有 delta.content 拼起来

现代 OpenAI-compatible API 可能包含：

- "content"
- "tool_calls"
- function/tool call arguments
- reasoning
- annotations
- finish information
- 其他 provider-specific fields

因此需要一个专门的 SSE parser / accumulator。

至少必须保证：

«对普通 text completion 正确工作。»

同时代码结构应允许未来增加 tool calls 等支持。

---

26. Response Collector 不负责 Session

Collector 只负责：

SSE → Assistant Message

它不知道：

S1 是什么
stateHash 是什么
clientHash 是什么

Session Resolver 也不应该知道：

SSE
Redis page

两个模块必须解耦。

---

27. 推荐核心组件

只保留几个真正需要抽象的类型。

SessionResolver
    ↓
SessionStore

ResponseCollector
    ↓
TempPageStore

ProxyHandler
    ↓
SessionResolver
    ↓
ReverseProxy
    ↓
ResponseCollector

---

28. SessionStore Interface

逻辑接口：

GetSession(clientHash, stateHash)
PutSession(clientHash, stateHash, sessionID, TTL)

Redis 是第一种实现。

不要让业务代码直接调用：

redis.Client.Get(...)

---

29. TempPageStore Interface

逻辑接口：

PutPage(requestID, seq, data, TTL)
GetPages(requestID)
DeletePages(requestID)

这样未来如果 Redis 换成：

- Valkey
- Dragonfly
- 其他 KV

不会影响 Collector。

---

30. 不需要抽象 Redis Client

不要过度设计：

KVEngine
CacheEngine
StorageEngine
DatabaseEngine

项目只需要：

SessionStore
TempPageStore

两个业务接口即可。

---

31. Reverse Proxy

使用 Go 标准库：

net/http/httputil.ReverseProxy

不要自己重新实现 HTTP proxy。

需要自定义：

- upstream URL
- request header
- request body
- response handling
- SSE streaming
- error handling

---

32. Request Body 处理

为了计算：

messages

Gateway 必须读取 request body。

但是：

«读取以后必须恢复 "r.Body"。»

否则 ReverseProxy 转发时 upstream 会收到空 body。

流程：

ReadAll(r.Body)
    ↓
Parse JSON
    ↓
Resolve session
    ↓
r.Body = new body(bytes)
    ↓
ReverseProxy

必须正确设置：

Content-Length

必要时同步更新。

---

33. 不要修改客户端的请求 JSON

Gateway 的主要修改应该只有：

x-opencode-session

不要：

- 修改 messages
- 删除 messages
- 重排 messages
- 添加 fake message
- 修改 model
- 修改 stream
- 修改 temperature

除非以后明确增加配置。

---

34. "x-opencode-session" Header

Header 名称：

x-opencode-session

大小写由 HTTP 标准处理，但代码中统一使用：

X-Opencode-Session

或：

x-opencode-session

即可。

不要同时制造多个 header。

如果客户端已经提供：

x-opencode-session: S1

直接透传。

---

35. Streaming Response

对于：

stream: true

必须保持：

Content-Type: text/event-stream

以及 SSE 的实时 flush。

不能等整个 response 完成后再返回。

这是项目的硬性要求。

---

36. 不要使用全局大 Buffer

禁止类似：

var allResponses []byte

保存整个 response。

正确：

current page
+
Redis completed pages

---

37. Request 生命周期

每个请求建议有：

requestID
clientHash
sessionID
stateHash
collector

但这些只是 request-local state。

不要把它们放进全局 map 长期保存。

---

38. Context

所有 Redis 操作必须使用 request / background context 的合理衍生 context。

注意：

«HTTP request context 在客户端断开后通常会 cancel。»

因此 Finalize 的清理行为需要根据实际实现决定是否使用独立的短生命周期 cleanup context。

例如：

request context
    ↓
client disconnect
    ↓
request context canceled

但：

Redis DEL temporary pages

仍然可能需要执行。

因此 cleanup 可以使用独立 timeout context。

不要使用：

context.Background()

无限期执行 Redis 操作。

应该有明确 timeout。

---

39. Redis 操作 Timeout

建议：

redis:
  timeout: 2s

具体值可配置。

任何：

- GET session
- SET session
- page write
- page read
- page delete

都不能无限等待。

---

40. Session TTL

默认：

7 days

配置：

session:
  ttl: 168h

临时 response：

response_cache:
  ttl: 30m

临时 TTL 必须明显小于 session TTL。

---

41. Redis Key Version

所有 key 必须带版本：

oc:v1:

例如：

oc:v1:session:...
oc:v1:tmp:...

未来修改 key schema 时可以直接升级：

oc:v2:

避免旧数据兼容地狱。

---

42. 错误处理原则

Session Redis GET 失败

如果客户端没有 session：

生成 UUID
继续请求

不要让 Redis 故障直接让 OpenCode API 不能用。

记录 error metric/log。

---

Session Redis SET 失败

不要影响已经完成的客户端 response。

记录错误。

下一次请求可能发生 session miss。

---

Temp page SET 失败

不要回写客户端。

客户端 SSE 继续。

但是标记当前 collector：

cacheFailed = true

最终：

Finalize → fail

不要提交正式 state mapping。

---

43. Logging

日志必须带：

request_id
session_id
state_hash

但：

«不要记录完整 Authorization/API Key。»

例如：

INFO request completed
request_id=R1
session_id=S1
state_hash=72ab...
pages=4
bytes=12.4MB
duration=832s

---

44. 不要记录完整用户消息

默认日志禁止输出：

messages

因为可能包含：

- 用户隐私
- token
- 密钥
- 文件内容
- prompt

调试时如果需要，可以提供明确的 debug 配置，但默认关闭。

---

45. Metrics

至少提供：

session_resolve_hit
session_resolve_miss
session_client_supplied

stream_completed
stream_aborted

page_flush_success
page_flush_error

finalize_success
finalize_error

session_mapping_created
session_mapping_error

以及：

stream_duration
response_bytes
page_count
redis_latency
finalize_duration

如果使用 Prometheus，指标名称统一使用项目 namespace。

---

46. 并发

Gateway 必须支持多个 conversation 同时进行：

Chat A → R1 → S1
Chat B → R2 → S2
Chat C → R3 → S3

所有 request-local state 必须独立。

绝对不能使用：

global currentSession
global currentBuffer
global currentMessages

---

47. Collector 并发安全

建议一个 request 对应一个 Collector。

尽量采用：

«单 goroutine owner»

而不是给所有字段加 mutex。

如果确实存在多个 goroutine 写入，则明确设计 ownership。

目标：

SSE Reader
    ↓
Collector

不要让十几个 goroutine 同时修改同一个 buffer。

---

48. Page Sequence

每个 request 的 page 从：

000001

开始递增。

必须保证：

000001
000002
000003
...

没有重复 sequence。

Finalize 时按照 seq 排序。

不要依赖 Redis HGETALL 返回顺序。

---

49. 临时数据 TTL Refresh

每次 page flush 可以刷新：

oc:v1:tmp:{requestID}

TTL。

这样长时间 thinking 的 response 不会在 30 分钟内过期。

例如每次 page：

HSET
EXPIRE 1800

如果一条响应持续 15 分钟，TTL 会持续续期。

---

50. 正式 Mapping TTL Refresh

不要每次 lookup 都无条件刷新所有 mapping。

建议：

«当一个新的完整 conversation state 成功生成时，创建/刷新这个 state mapping 的 TTL。»

例如：

Hash(U1,A1) → S1

新一轮完成：

Hash(U1,A1,U2,A2) → S1

只需要确保新 state 活跃。

旧 state 允许自然过期。

---

51. 一个完整例子

第一次：

POST
messages=[U1]

Gateway：

requestID=R1
sessionID=S1(UUID)

转发：

x-opencode-session: S1

OpenCode：

A1 streaming...

Collector：

page1
page2
page3
tail

完成：

A1 = page1 + page2 + page3 + tail

计算：

Hash(U1,A1)

Redis：

oc:v1:session:C:H(U1,A1) → S1

删除：

oc:v1:tmp:R1

---

第二次：

POST
messages=[U1,A1,U2]

计算：

Hash(U1,A1)

Redis hit：

S1

转发：

x-opencode-session: S1

得到：

A2

最终：

Hash(U1,A1,U2,A2) → S1

---

52. 非 Streaming 请求

第一版也应考虑：

stream=false

如果 upstream 返回完整 JSON：

无需 page cache

直接：

解析 assistant message
→ Hash(messages + assistant)
→ SET session mapping

只有真正的 streaming response 使用 Response Page Cache。

这样逻辑更清晰。

---

53. 非 Chat Completion 请求

如果请求不是：

/v1/chat/completions

或者 body 无法解析为预期 OpenAI-compatible chat request：

不要强行处理。

直接：

ReverseProxy

保持透明代理行为。

---

54. 安全边界

必须：

- 不记录 API Key
- 不记录完整 prompt
- 不把原始 API Key 放 Redis key
- 限制 request body 大小
- 限制单 request 最大临时缓存大小
- Redis key 必须带版本
- 临时数据必须 TTL
- 不允许客户端通过 request_id 访问其他请求的临时数据
- 不把内部 request_id 暴露给 upstream，除非以后明确需要

---

55. 配置

第一版保持少量配置。

建议：

server:
  listen: ":8080"

upstream:
  base_url: "https://..."

redis:
  addr: "127.0.0.1:6379"
  db: 0
  timeout: 2s

session:
  ttl: 168h

response_cache:
  page_size: 4MiB
  ttl: 30m
  workers: 4
  max_pending_pages: 8

limits:
  max_request_body: 32MiB

不要增加没有明确需求的配置。

---

56. Graceful Shutdown

收到 SIGTERM：

1. 停止接受新 request
2. 等待已有 HTTP request
3. 不再创建新的 page flush
4. 尽可能完成已有 page flush
5. 超时后退出
6. 临时 Redis 数据依靠 TTL 清理

不要求 shutdown 时扫描 Redis 并清理所有 tmp key。

---

57. 测试要求

必须测试以下场景。

Test 1：首条请求

[U1]

预期：

生成 S1

---

Test 2：第二条请求命中

[U1,A1,U2]

预期：

Hash(U1,A1) → S1

---

Test 3：连续多轮

U1,A1,U2,A2,U3,A3

每一轮都应该继续：

S1

---

Test 4：Branching

U1,A1,U2,A2

修改：

U1,A1,U3

预期：

Hash(U1,A1) → S1

---

Test 5：相同首条消息

Chat A：

U1

Chat B：

U1

必须各自首次生成 session。

---

Test 6：相同完整历史

两个 conversation：

U1,A1

hash 相同是允许的。

不要引入复杂机制强行区分。

---

Test 7：长 SSE

模拟：

> PAGE_SIZE

的 response。

确认：

- RAM 不保存完整 response
- Redis page 数量正确
- page sequence 正确
- 最终 assistant message 完整

---

Test 8：最后 page race

模拟：

page flush 正在进行
+
[DONE]

确认 Finalize 会等待 pending flush。

最终不能丢数据。

---

Test 9：RAM tail

模拟：

page1 = 4MiB
tail = 100KiB

最终必须：

page1 + tail

---

Test 10：Redis page write failure

预期：

Client 仍然收到完整 SSE
Finalize failure
不写新的 state mapping

---

Test 11：上游中途断流

预期：

Abort
不建立新的 state mapping

---

Test 12：客户端主动提供 session

请求：

x-opencode-session: S99

预期：

最终 upstream 收到 S99

Gateway 不得替换成 S1。

---

Test 13：Redis 不可用

客户端没有 session。

预期：

Gateway 生成 UUID
继续请求 upstream

---

Test 14：stream=false

确认：

不创建临时 page cache

并正常建立：

stateHash → session

---

58. 性能验收

至少验证：

SSE latency

Redis 正常/变慢时：

客户端 SSE 输出不能明显被 Redis latency 阻塞。

Memory

模拟：

100MB
500MB
1GB

response。

确认：

«Gateway RAM 不随完整 response 大小线性增长。»

Redis

确认：

Redis write count ≈ response_size / page_size

而不是：

Redis write count ≈ SSE event count

---

59. 第一版不要追求“完美识别 Conversation”

这个项目不是：

«从任意 OpenAI-compatible request 中 100% 推断 conversation identity。»

这是做不到的。

它采用的是：

完整历史 state
    ↓
state hash
    ↓
session affinity

因此必须明确：

«客户端如果不提供 conversation ID，并且两个 conversation 在某一时刻具有完全相同的历史，那么网关无法从请求本身区分它们。»

这是协议层面的限制。

不需要通过复杂架构掩盖这个事实。

---

60. 最终数据模型

整个系统最终只需要理解这张图：

                    HTTP Request
                         │
                         ▼
                 ┌───────────────┐
                 │ SessionResolver│
                 └───────┬───────┘
                         │
                         ▼
                     Session S1
                         │
                         ▼
                    OpenCode API
                         │
                         │ SSE
             ┌───────────┴───────────┐
             ▼                       ▼
          Client                 Collector
        immediate                    │
        streaming                    ▼
                                RAM Page Buffer
                                     │
                               PAGE_SIZE reached
                                     │
                                     ▼
                              Async Redis Page
                                     │
                                     ▼
                                   Redis
                                     │
                                  [DONE]
                                     │
                                     ▼
                                  Finalize
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
                    Redis Pages             RAM Tail
                          │                     │
                          └──────────┬──────────┘
                                     ▼
                             Complete Assistant
                                     │
                                     ▼
                          Complete Conversation State
                                     │
                                     ▼
                                  SHA-256
                                     │
                                     ▼
                             stateHash → S1

---

61. 开发优先级

按以下顺序开发，不要一开始同时写所有东西。

Phase 1

实现：

OpenAI request parsing
Message model
Canonical hash
SessionStore
SessionResolver

先让：

[U1] → S1
[U1,A1,U2] → S1

跑通。

Phase 2

实现：

ReverseProxy
x-opencode-session injection

确认普通 HTTP request 正常。

Phase 3

实现：

SSE transparent streaming

确认客户端实时收到数据。

Phase 4

实现：

ResponseCollector
Redis temporary pages

确认长 response 不会全部留在 RAM。

Phase 5

实现：

Finalize
page flush synchronization
assistant message reconstruction
state hash update
tmp cleanup

Phase 6

加入：

metrics
logging
timeouts
graceful shutdown

Phase 7

完整压力测试和故障测试。

---

62. 完成标准

当以下流程可以稳定运行时，核心功能才算完成：

RikkaHub / Python httpx
        │
        ▼
Gateway
        │
        ▼
OpenCode

客户端完全不需要知道：

x-opencode-session

但是网关自动实现：

Conversation #1
U1
 ↓
A1
 ↓
U2
 ↓
A2
 ↓
U3
 ↓
A3

所有请求都稳定使用：

S1

同时：

- SSE 实时输出
- 长 response 不全部占用 RAM
- Redis page 异步写入
- "[DONE]" 后正确 finalize
- 最后一页 race 不丢数据
- Redis 临时缓存自动 TTL
- 成功 response 才更新 state mapping
- Redis 故障不会让已有 SSE 直接失败
- 客户端主动提供 session 时不覆盖
- conversation branching 正常工作
- 不保存完整 conversation history
- 不记录敏感认证信息

---

63. 最终原则

整个实现应始终围绕下面这个简单模型：

                 request
                    │
                    ▼
          Hash(previous messages)
                    │
                    ▼
             session lookup
                    │
                    ▼
                 Session
                    │
                    ▼
                OpenCode
                    │
                    ▼
                  SSE
               ┌────┴────┐
               ▼         ▼
            Client    Temp Cache
                        │
                       DONE
                        │
                        ▼
                  Assistant Message
                        │
                        ▼
                Hash(full state)
                        │
                        ▼
                  Session Mapping

不要把这个项目做成一个新的 conversation database。

它本质上只是一个：

«OpenCode API Reverse Proxy + Session Affinity Layer + Streaming Response Spill Cache。»

保持这三个职责清晰分离，代码量会很小，故障边界也会非常清楚。
