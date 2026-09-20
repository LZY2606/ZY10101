# JReactive-8583 调用链与消息监听顺序分析

> 分析对象：当前工作区源码（Kotlin，Netty 4.2.7.Final，j8583 3.0.0，Java 17）。
> 目的：在不改任何产品代码的前提下，给出**可核验**的连接初始化、pipeline 形态、异常/idle/echo/重连顺序分析，并附最小并发测试。
> 约定：所有引用均为 `仓库相对路径:行号`，可直接在 IDE 中打开核对。Netty 侧结论基于 4.2.7.Final 源码（`ByteToMessageDecoder`、`LengthFieldBasedFrameDecoder`、`DefaultChannelPipeline`、`IdleStateHandler`、`LoggingHandler`）。

## 0. 结论速览

- 单连接内，inbound 事件严格按 pipeline 顺序串行执行，这是 **Netty 保证**的；业务 listener 的执行顺序只是「`CopyOnWriteArrayList` 按插入顺序」+「`onMessage` 返回 `false` 即短路」的**当前实现行为**，不是 Netty 语义。
- `ParseExceptionHandler` 对真实 ISO 解析错误**实际上不会回 650 报文**：j8583 抛出的 `java.text.ParseException` 在 Netty 4 的 `ByteToMessageDecoder` 中被包成 `DecoderException` 后才向后传播，而 handler 用 `cause is ParseException` 判断（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/ParseExceptionHandler.kt:30`）。已用临时端到端测试实证（见风险 R1）。
- idle 心跳是 **ALL_IDLE**（不是 READER_IDLE）：`IdleStateHandler(0, 0, idleTimeout)`（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/Iso8583ChannelInitializer.kt:82`）。
- `EchoMessageListener` 对**所有** network-management 类报文（含 0x0810 响应）都回包且返回 `false` 截断监听链（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/EchoMessageListener.kt:12-27`）。两端同时启用 echo 时会出现 0x0800→0x0810→0x0820→… 的有限级联（已实证）。
- 重连由 client 端 `ReconnectOnCloseListener` 管理，调度在 **boss EventLoopGroup** 上；同一连接生命周期内 close 只通知一次，因此「stop 与 close 并发时最多安排一次重连」这一终局不变量成立——新增并发测试证明之（见第 7 节）。
- 但有几个真实的顺序隐患：R2 已调度的重连任务不可取消且 `connectAsync` 会无条件把停止标志重置为 `false`；R4 `connect(host, port)` / `connect(SocketAddress)` 重载实际会阻塞到连接关闭为止。

## 1. 启动：configuration 如何决定 pipeline

### 1.1 初始化路径

**Server**
1. 构造 `Iso8583Server(port, config, factory)`（`src/main/kotlin/com/github/kpavlov/jreactive8583/server/Iso8583Server.kt:17-22`）。父类构造块在此时把 `EchoMessageListener` 预置进共享的 `CompositeIsoMessageHandler`（仅当 `addEchoMessageListener=true`，默认 `false`）（`src/main/kotlin/com/github/kpavlov/jreactive8583/AbstractIso8583Connector.kt:99-103`）。
2. `init()` 创建 boss / worker 两个 `NioEventLoopGroup` 并调用 `createBootstrap()`（`AbstractIso8583Connector.kt:54-59`）。worker 线程数取 `configuration.workerThreadsCount`，传 `0` 时由 Netty 决定（`AbstractIso8583Connector.kt:84-91`；Netty 默认 `max(1, 2*CPU)`）。
3. `Iso8583Server.createBootstrap()` 构建 `ServerBootstrap`：boss 负责 accept，worker 同时负责 child channel I/O 与 `addLast(workerGroup, …)` 的 handler 执行；child handler 是 `Iso8583ChannelInitializer`（`Iso8583Server.kt:37-60`）。
4. `start()` 执行 `bind().sync().await()`，绑定成功的 listener 在 **boss EventLoop** 上把 server channel 存入 `channelRef`（`Iso8583Server.kt:25-35`）。

**Client**
1. 构造 `Iso8583Client(address, config, factory)`（`src/main/kotlin/com/github/kpavlov/jreactive8583/client/Iso8583Client.kt:18-22`）。
2. `init()` 同上，但 client 只创建一个 group：`Bootstrap.group(bossEventLoopGroup)`，**boss 与 worker 是同一个 NioEventLoopGroup**（`Iso8583Client.kt:92-107`、`AbstractIso8583Connector.kt:56-57`）。
3. `createBootstrap()` 还会 new 出 `ReconnectOnCloseListener(client, reconnectInterval, bossEventLoopGroup)`（`Iso8583Client.kt:109-114`）。默认重连间隔 100 ms（`src/main/kotlin/com/github/kpavlov/jreactive8583/client/ClientConfiguration.kt:21`）。
4. `connect()` → `connectAsync().sync()`，随后返回的是 **`channel.closeFuture()`**（注意：不是连接建立 future）（`Iso8583Client.kt:34-37`）。

### 1.2 Pipeline 装配（每个新连接执行一次）

`Iso8583ChannelInitializer.initChannel()` 按固定顺序 addLast（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/Iso8583ChannelInitializer.kt:64-93`）：

| # | 名称 | 类 | 执行器 | 由哪个配置开关控制 |
|---|---|---|---|---|
| 1 | `lengthFieldFrameDecoder` | `LengthFieldBasedFrameDecoder` 或 `StringLengthFieldBasedFrameDecoder` | channel 自己的 EventLoop | 总是添加；类型由 `encodeFrameLengthAsString` 决定（:66-69, :118-137） |
| 2 | `iso8583Decoder` | `Iso8583Decoder`（extends `ByteToMessageDecoder`） | channel EventLoop | 总是添加（:70） |
| 3 | `iso8583Encoder` | `Iso8583Encoder`（`@Sharable`，extends `MessageToByteEncoder<IsoMessage>`） | channel EventLoop | 总是添加（:71） |
| 4 | `logging` | `IsoMessageLoggingHandler`（extends `LoggingHandler`，双向） | **workerGroup 固定子执行器** | `addLoggingHandler`，默认 `false`（:72-74） |
| 5 | `replyOnError` | `ParseExceptionHandler`（inbound） | workerGroup | `replyOnError`，默认 `false`（:75-77） |
| 6 | `idleState` | `IdleStateHandler(0,0,idleTimeout)` | workerGroup | `addEchoMessageListener`，默认 `false`（:78-83） |
| 7 | `idleEventHandler` | `IdleEventHandler`（inbound） | workerGroup | 同上（:84-88） |
| 8 | （匿名，无名字） | 共享的 `CompositeIsoMessageHandler` | workerGroup | 总是作为 `customChannelHandlers[0]` 传入（:90-91；构造点见 `Iso8583Client.kt:104`、`Iso8583Server.kt:54`） |
| — | 用户自定义 | `ConnectorConfigurer.configurePipeline` 追加 | 视调用而定 | configurer 钩子（:92；接口见 `src/main/kotlin/com/github/kpavlov/jreactive8583/ConnectorConfigurer.kt:43-48`） |

要点：

- 帧解码器参数：`maxFrameLength=8192`、`lengthFieldOffset=0`、`lengthFieldLength=2`、`lengthFieldAdjust=0`、**`initialBytesToStrip=lengthFieldLength`**（即剥掉长度头）（`Iso8583ChannelInitializer.kt:129-135`；默认值见 `src/main/kotlin/com/github/kpavlov/jreactive8583/ConnectorConfiguration.kt:8-36`）。
- 字符串长度头模式额外重写 `getUnadjustedFrameLength`，按 ASCII 把长度字节转成数字（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/codec/StringLengthFieldBasedFrameDecoder.kt:38-50`）。
- encoder 自己写长度头，不使用 `LengthFieldPrepender`：二进制头走 `isoMessage.writeToBuffer(lengthHeaderLength)`，字符串头走 `String.format("%0Nd")`（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/codec/Iso8583Encoder.kt:17-40`）。
- #4-#8 通过 `addLast(workerGroup, …)` 注册。Netty 默认 `SINGLE_EVENTEXECUTOR_PER_GROUP=true`，**同一 channel 上这些 handler 固定绑定 workerGroup 中的同一个子 EventExecutor**（`DefaultChannelPipeline.childExecutor` 4.2.7），所以 #4→#8 的回调按 pipeline 顺序串行，且与 channel EventLoop 之间通过队列切换线程。client 的 workerGroup 就是唯一的 Nio group；server 的 workerGroup 是独立的 worker group。

## 2. 每个 handler 看到的数据形态

**Inbound（收方向）**

1. `lengthFieldFrameDecoder`：输入原始累积 `ByteBuf`（可能含半帧/多帧）。输出单帧 `ByteBuf`，**长度头已被剥离**，其后是 j8583 报文字节（MTI+bitmap+fields）。不完整帧返回 null，字节保留在 `ByteToMessageDecoder` 的 cumulation 里。
2. `iso8583Decoder`：输入恰好一帧的 `ByteBuf`。`decode` 把所有可读字节拷成 `ByteArray` 并调用 `messageFactory.parseMessage(bytes, 0)`，输出一个 `IsoMessage`（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/codec/Iso8583Decoder.kt:31-43`）。
3. `iso8583Encoder`：inbound 事件直接透传（`MessageToByteEncoder` 是 outbound handler）。
4. `logging`：`LoggingHandler.channelRead` 先按 `IsoMessage` 定制格式化打印，再 `fireChannelRead`（4.2.7 `LoggingHandler:276`；格式化见 `src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/IsoMessageLoggingHandler.kt:82-119`）。
5. `replyOnError`：正常消息透传；只有 inbound 异常（`exceptionCaught`）才介入（`ParseExceptionHandler.kt:26-35`）。
6. `idleState`：消息透传，仅维护读/写空闲计时；`idleEventHandler` 透传普通消息，只消费 `IdleStateEvent`（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/IdleEventHandler.kt:18-29`）。
7. `CompositeIsoMessageHandler`：`msg as? T` 成功后遍历 listener 链；无论是否处理，最后都 `super.channelRead(ctx, msg)` 向后传（`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/CompositeIsoMessageHandler.kt:23-41`）。类型不匹配时**安全转换返回 null**——所以 `catch (ClassCastException)` 分支是死代码（:28-38）。
8. 用户自定义 handler（若有）：看到 `IsoMessage`，最终到 pipeline tail 被丢弃。

**Outbound（发方向，`channel.writeAndFlush(IsoMessage)` 或 `ctx.writeAndFlush`）**

- outbound 从发起位置**向前**遍历。业务 listener 在 #8/#7 处用 `ctx.writeAndFlush(...)` 发起的写出，会依次经过：`iso8583Encoder`（`IsoMessage`→带长度头的 `ByteBuf`，`Iso8583Encoder.kt:17-40`）→ `lengthFieldFrameDecoder`（它是 inbound-only，写出透传）→ socket。
- 若启用 logging：`LoggingHandler` 是 duplex（4.2.7 `LoggingHandler:42,284`），#4 位于 encoder **之后**（更靠近 tail）。从 #7/#8 发起的写出会先在 `logging.write` 看到 **`IsoMessage`**（明文日志），编码后的 `ByteBuf` 不再经过它。从更靠前 handler（如 #5）写出同样只在 logging 处记录 `IsoMessage`。
- encoder 抛异常会被设到 write promise 上返回给调用方，**不会**触发 `exceptionCaught`（`MessageToByteEncoder` 语义）。

## 3. Listener 链的顺序语义

- 存储结构是 `CopyOnWriteArrayList`（`CompositeIsoMessageHandler.kt:20`）；分发时先取 `size` 再按下标遍历（:47-66），因此**一次分发内**按分发开始时的快照顺序执行，分发过程中 add/remove 不影响当次列表（COW 迭代语义）。
- `EchoMessageListener` 在连接器构造块中加入（`AbstractIso8583Connector.kt:99-103`），**早于**任何 `client/server.addMessageListener(...)`，所以它永远是链头。
- 每个 listener：先 `applies(msg)`，命中则调用 `onMessage`；返回 `false` 立即终止后续 listener（`CompositeIsoMessageHandler.kt:50-66,75-96`）。
- `EchoMessageListener.applies` 只判断 `type & 0x0800 != 0`（`EchoMessageListener.kt:12-13`），即 network-management 类的**请求、响应、通知全部命中**；命中后回 `createResponse`（j8583 把 MTI +0x10）并返回 `false`（`EchoMessageListener.kt:21-27`；j8583 `MessageFactory.createResponse` 语义：type+16）。后果见 R3。
- listener 异常：`failOnError=true`（默认）时异常被重新抛出（`CompositeIsoMessageHandler.kt:84-93`），沿 inbound 方向传到 tail——异常**不会回退给前面的 `replyOnError`**（Netty 异常只向 tail 传播）。tail 默认只 `logger.warn`，不关闭连接（4.2.7 `DefaultChannelPipeline.onUnhandledInboundException:1171`）。

## 4. 五条时间线

线程标注：`[IO]` = 该 channel 的 EventLoop（client 为唯一 Nio group 中绑定的线程；server 为 worker Nio 线程）；`[WK]` = workerGroup 为该 channel 固定的子 EventExecutor 线程（client 上与 `[IO]` 同属一个 NioEventLoopGroup 但通常不是同一线程；server 上属于 worker group）。Netty 在 `[IO]`→`[WK]` 边界自动排队，顺序不丢不乱。

### 4.1 正常请求（client 发 0x0200，server 业务 listener 回 0x0210）

```
client 线程(用户)                 client channel                server child channel
─────────────────                 ──────────────                ────────────────────
sendAsync(0x0200)
 Iso8583Client.kt:136-140
  ch.writeAndFlush ──────► [WK?不，写出来自tail侧]
                           encoder: IsoMessage→ByteBuf
                           (+2字节长度头)            ── TCP ──►  [IO] socketRead
                                                            lengthFieldFrameDecoder: 累积/切帧
                                                            iso8583Decoder: ByteBuf→IsoMessage 0x0200
                                                            [WK] logging(若启用): DEBUG 打印
                                                            [WK] Composite: Echo.applies?0x0200&0x0800=0 跳过
                                                            [WK] 业务listener.applies=true→onMessage
                                                                 createResponse(0x0210), ctx.writeAndFlush
                                                                 CompositeIsoMessageHandler/用户ctx
                                                            ◄── encoder: 0x0210→ByteBuf(长度头)
[IO] ◄── TCP ── lengthDecoder 透传/encoder 已过 ── 读
 lengthFieldFrameDecoder: 切出一帧
 iso8583Decoder: →0x0210
 [WK] logging(若启用)
 [WK] Composite: 业务 listener 处理 0x0210
```

顺序保证：每跳都在单 EventExecutor 内 FIFO；`[IO]` 与 `[WK]` 之间由 Netty 的 in-task 排队保序。**偶然部分**：业务 listener 谁先执行取决于 add 顺序（`CompositeIsoMessageHandler.kt:48-66`）；echo 在链头是构造顺序导致（`AbstractIso8583Connector.kt:99-103`）。

### 4.2 半帧分两次到达（长度头声称 N 字节，先到 m<N）

```
[IO] 第1次 socketRead(m 字节)
  lengthFieldFrameDecoder.decode: 可读字节不足一个完整帧 → 返回 null
    · 若是连2字节长度头都不够：根本不解析长度（Netty 4.2.7 LengthFieldBasedFrameDecoder.decode 开头判断）
    · ByteToMessageDecoder 把字节保留在 cumulation，不向下传播任何消息
  → iso8583Decoder 完全不被调用；[WK] 无活动；listener 无感知
[IO] 第2次 socketRead(剩余字节)
  cumulator 拼接（ByteToMessageDecoder.channelRead → MERGE_CUMULATOR）
  lengthFieldFrameDecoder: 得到完整帧 → 输出去掉长度头的 ByteBuf
  iso8583Decoder: parseMessage → IsoMessage
  [WK] … Composite → listener
```

说明：`Iso8583Decoder` 自身**不做**粘包处理，它假设上游每次恰好给一帧；半帧/粘包正确性完全依赖 `LengthFieldBasedFrameDecoder`。一次 TCP 读到两帧时，`ByteToMessageDecoder.callDecode` 循环切帧并在同一次 read 事件里依次 `fireChannelRead`（4.2.7 `ByteToMessageDecoder.callDecode:449-496`），因此多帧按字节序进入 listener。

### 4.3 解析失败（帧完整，但 ISO body 不合法）

```
[IO] lengthFieldFrameDecoder: 成功切帧（长度本身合法）
[IO] iso8583Decoder.decode: messageFactory.parseMessage 抛 java.text.ParseException
     （或 j8583 的其他 RuntimeException）
     ByteToMessageDecoder.callDecode 捕获后包成 DecoderException(cause=ParseException) 重新抛出
     （4.2.7 ByteToMessageDecoder.java:497-501；channelRead 外层同样包装:291-295）
     同时 finally：cumulation 若已不可读则 release 并置 null —— 坏帧字节被丢弃，连接可继续对齐下一帧
[WK?] exceptionCaught 从 iso8583Decoder 的 context 向后（tail 方向）传播：
     · replyOnError=true 时进入 ParseExceptionHandler.exceptionCaught
         判断 cause is ParseException —— 实际 cause 是 DecoderException → 不匹配
         （ParseExceptionHandler.kt:30）→ 不发 0x1644，仅 ctx.fireExceptionCaught 继续向后
     · Composite 不重写 exceptionCaught，透传
     · 到 pipeline tail：DefaultChannelPipeline.onUnhandledInboundException 仅 logger.warn
     · 连接不关闭、不重连
```

已实证：用与生产一致的 pipeline（`LengthFieldBasedFrameDecoder(8192,0,2,0,2)` + `Iso8583Decoder` + `ParseExceptionHandler`）写入 `00 28` + 40 字节 `'X'`，捕获到 `DecoderException: java.text.ParseException … no parsing guide for message type`，且 outbound 无任何回复（临时验证测试已删除，结论可按上述步骤复现）。
长度层异常（`TooLongFrameException`、`CorruptedFrameException`）同属 `DecoderException` 路径，同样不会触发 650 回复；超长帧还会进入 Netty 的 discard 模式（4.2.7 `LengthFieldBasedFrameDecoder.exceededFrameLength:375-392`）。

### 4.4 远端关闭（server 进程退出 / RST / FIN）

```
[IO] NioEventLoop 检测到 OP_READ=0（或 IOException）
  pipeline inbound channelInactive/channelUnregistered 依次传播
    · Composite/Idle/Logging 均未处理 channelInactive（默认透传）
  DefaultChannelPromise(closeFuture) 被设为 success → 通知 closeFuture 上的 listener
[IO→boss group 线程] ReconnectOnCloseListener.operationComplete
  （client 连接成功时注册：Iso8583Client.kt:82-87；代码：ReconnectOnCloseListener.kt:28-33）
    · channel.disconnect()
    · scheduleReconnect(): disconnectRequested==false →
      bossEventLoopGroup.schedule(client.connectAsync(), reconnectInterval=100ms)
      （ReconnectOnCloseListener.kt:35-44）
[100ms 后，boss group 线程] client.connectAsync()
    · requestReconnect() 把 disconnectRequested 重置为 false（Iso8583Client.kt:75）
    · b.connect()；失败 → connectFuture listener 再 scheduleReconnect()（:77-81）；成功 → 注册新 closeFuture listener（:82-87）
```

固定 100ms、无退避、无上限（`ClientConfiguration.kt:21`）。`client.isConnected` 在此期间为 false（`Iso8583Client.kt:162-166`）。现有 IT 用 `sleep(3)` 跨过这段（`src/test/java/com/github/kpavlov/jreactive8583/it/ClientReconnectIT.java:20`）。

### 4.5 主动 stop（client `disconnect()`）

```
用户线程 disconnectAsync()（Iso8583Client.kt:118-123）
  1) reconnectOnCloseListener.requestDisconnect() → disconnectRequested=true
  2) channel.close()
[IO] 关闭完成 → closeFuture 通知 ReconnectOnCloseListener.operationComplete
  scheduleReconnect() 读到 disconnectRequested==true → 不调度（ReconnectOnCloseListener.kt:36）
disconnect() 等待 close future（:125-128）
之后必须再调用 shutdown() 才会 shutdownGracefully 两个 group（AbstractIso8583Connector.kt:61-64）
```

server 侧区别：`Iso8583Server.stop()` 只关闭 **server socket**（停止 accept），`deregister()` + `close().syncUninterruptibly()`（`src/main/kotlin/com/github/kpavlov/jreactive8583/server/Iso8583Server.kt:76-93`）；**已建立的 child channel 不会被关闭**，线程组也不释放，要 `shutdown()` 才级联处理（:62-65）。


## 5. idle 事件与 echo 的发生顺序

- 开关是同一个：`addEchoMessageListener=true` 同时做两件事——构造块把 `EchoMessageListener` 加进 listener 链（`AbstractIso8583Connector.kt:100-102`），initializer 才添加 `idleState` + `idleEventHandler`（`Iso8583ChannelInitializer.kt:78-89`）。
- `IdleStateHandler(0, 0, idleTimeout)`：readerIdle=0、writerIdle=0 表示禁用，**只有 ALL_IDLE 在 idleTimeout 秒内既无入站读也无出站写时触发**（Netty 4.2.7 `IdleStateHandler`：`<=0` 不调度对应任务）。默认 30s（`ConnectorConfiguration.kt:8`），IT 配置为 2s（`src/test/resources/application-test.properties`）。
- 触发链：`[WK]` IdleStateHandler 发出 `IdleStateEvent(ALL_IDLE)`（任务在绑定的子执行器上执行）→ `IdleEventHandler.userEventTriggered` 匹配 ALL_IDLE（也匹配 READER_IDLE，但 READER_IDLE 永不产生），`ctx.write(0x0800)+flush`（`IdleEventHandler.kt:22-28`），且**不调用 `fireUserEventTriggered`**——其后的 Composite 与用户 handler 收不到 idle 事件（这是当前实现行为）。
- 写出经 encoder 加长度头发出。对端收到 0x0800：`EchoMessageListener.applies` 命中 → 回 0x0810。本端收到 0x0810 时自己的 `EchoMessageListener` 同样命中（只看 class nibble）→ 回 0x0820 并把该消息从业务 listener 链上**截走**（`EchoMessageListener.kt:21-27`、`CompositeIsoMessageHandler.kt:58-64`）。详见 R3。
- 注意区分：`EchoMessageListener`（回被动 echo 响应，inbound）与 `IdleEventHandler`（主动心跳）是两个独立组件，只是由同一开关启用。

## 6. 线程所有权：EventLoop、listener 回调、reconnect future

**EventLoop/线程所有权**

- Server：boss group（accept，server channel 的 EventLoop 在此）、worker group（每个 child channel 被分配一个 NioEventLoop，负责该连接所有 I/O）（`Iso8583Server.kt:42-43`）。
- Client：单个 NioEventLoopGroup 同时充当 boss 与 worker（`Iso8583Client.kt:95`）；该连接的所有 I/O 在被分配的那个 NioEventLoop 线程上。
- `addLast(workerGroup, …)` 的 handler（logging/replyOnError/idle/composite 及用户经 configurer 之后的 handler 若也指定 group）：每个 channel 从 workerGroup 中**固定绑定一个**子 EventExecutor（Netty `SINGLE_EVENTEXECUTOR_PER_GROUP` 默认 true）。**业务 listener 回调运行在这个 worker 子执行器线程上，而不是 selector I/O 线程**（server 上两者属于不同 group；client 上属于同一 group 但 Netty 仍在不同 EventExecutor 间切换排队）。因此在 listener 里做阻塞操作会占住该执行器，同 channel 后续消息/idle 任务全部排队等待（`LockDetectionIT` 里 server listener `Thread.sleep(5)` 正是在模拟这点，`src/test/java/com/github/kpavlov/jreactive8583/it/LockDetectionIT.java:166-170`）。

**Channel 引用与 future 所有权**

- `channelRef: AtomicReference<Channel>`（`AbstractIso8583Connector.kt:32,93-97`）：server 在 bind 成功 listener（boss 线程）写；client 在 connect 成功 listener（boss group 线程）写（`Iso8583Client.kt:82-87`）；`sendAsync/isConnected` 可从任意线程读（`Iso8583Client.kt:136-140,162-166`）。原子引用保证引用可见性，但不保证「读到的 channel 一定仍 active」。
- 重连 future：`ReconnectOnCloseListener` 是 `ChannelFutureListener`，每次连接成功由 client 把它注册到**该 channel 的 closeFuture**（`Iso8583Client.kt:85`）；连接失败时改由 connectFuture listener 调 `scheduleReconnect()`（:77-81）。调度用的 `ScheduledExecutorService` 是 client 的 bossEventLoopGroup（:109-114），返回的 `ScheduledFuture` **没有被保存**，因此无法 cancel。
- `disconnectRequested: AtomicBoolean`（`ReconnectOnCloseListener.kt:18,20-26,36`）是唯一的停止/重连仲裁状态，读改写不是复合原子操作（见 R2）。

**哪些顺序是 Netty 保证的，哪些只是当前实现偶然**

| 行为 | 归属 |
|---|---|
| 同一 Channel 上 inbound/outbound/任务按提交顺序、无并发地执行（happen-before + FIFO） | **Netty 保证**（单 EventExecutor 串行；跨执行器边界由 task 队列保序） |
| closeFuture / connectFuture 的 listener 恰好通知一次、成功失败只居其一 | **Netty 保证**（`ChannelPromise` 不可变完成语义） |
| 半帧不向下游传播、完整帧按字节序输出；异常沿 inbound 向 tail 传播 | **Netty 保证**（`ByteToMessageDecoder`、`LengthFieldBasedFrameDecoder`、`DefaultChannelPipeline`） |
| ALL_IDLE 在无读写后恰好按配置秒数触发（计时随事件重置） | **Netty 保证**（`IdleStateHandler`） |
| 同一 channel 上多个 workerGroup handler 绑定同一个子执行器 | **Netty 保证**（默认 option，可被关闭） |
| 业务 listener 的先后次序（echo 永远第一、之后按 add 顺序） | **实现偶然**：构造块顺序 + COW list 插入序（`AbstractIso8583Connector.kt:99-103`、`CompositeIsoMessageHandler.kt:20,48-66`） |
| listener 返回 `false` 即整条链短路（且消息仍 `fireChannelRead` 给 pipeline 后续 handler） | **实现约定**（`CompositeIsoMessageHandler.kt:40,58-64`），非 Netty 机制 |
| listener 回调运行在 worker 子执行器而非 I/O 线程 | **实现选择**（initializer 传入 workerGroup，`Iso8583ChannelInitializer.kt:73,76,79,84,91`） |
| idle 事件不传到用户 handler | **实现偶然**（`IdleEventHandler.kt:18-29` 未 fire） |
| 重连固定 100ms、无退避、无上限、不可取消 | **实现选择**（`ClientConfiguration.kt:21`、`ReconnectOnCloseListener.kt:35-44`） |
| parse 失败不回 650、不关闭连接 | **框架行为 + 实现假设的交叉**：handler 假设收到裸 `ParseException`，但 Netty 4 包了一层（R1） |

## 7. 四个具体风险（均为代码层面可复现的隐患，非已发生的线上故障）

### R1. `replyOnError` 对真实解析错误失效：错误被包装，650 回复永不发出

- 现象预期：打开 `replyOnError` 后，收到无法解析的 ISO 报文应回 0x1644（field 24=650）（`ParseExceptionHandler.kt:30-34,38-55`）。
- 实际机制：j8583 抛 `java.text.ParseException`；Netty 4 的 `ByteToMessageDecoder.callDecode/channelRead` 把所有非 `DecoderException` 包成 `DecoderException` 再 `fireExceptionCaught`（4.2.7 `ByteToMessageDecoder.java:291-295,497-501`）。`ParseExceptionHandler` 判断的是 `cause is ParseException`（`ParseExceptionHandler.kt:30`），对 `DecoderException` 不命中，错误继续到 tail 只打 warn，连接保持打开且无任何 650 回复。
- 复现步骤（已在本地用临时代码实证后删除）：
  1. 用默认帧参数组装 pipeline：`LengthFieldBasedFrameDecoder(8192,0,2,0,2)` → `Iso8583Decoder(defaultFactory)` → `ParseExceptionHandler(factory,true)` → 收集 handler；
  2. `writeInbound(0x00 0x28 + 40个0x58('X'))`；
  3. 断言：收集到的异常类型为 `DecoderException`、`getCause()` 为 `ParseException`，`channel.outboundMessages()` 为空。
- 影响（推断，未观察到线上故障）：对端按「收到 650 后重同步」设计时将永远等不到回复；坏帧后的长度对齐仍由帧解码器保证（坏帧字节在异常时被丢弃），但应用层错误处理顺序与文档/配置承诺不一致。
- 现有测试覆盖：`src/test/java/com/github/kpavlov/jreactive8583/netty/pipeline/ParseExceptionHandlerTest.java:46-72` 直接调用 handler 并手工传入裸 `ParseException`，**恰好绕过了包装**，所以测试全绿但生产路径不成立；仓库中无任何端到端坏报文测试，也没有 `replyOnError(true)` 的集成测试。

### R2. 停止与关闭的竞态：已调度的重连不可取消，`connectAsync` 还会复活重连意图

- 机制：`scheduleReconnect()` 返回的 `ScheduledFuture` 未保存（`ReconnectOnCloseListener.kt:38-42`）；`requestDisconnect()` 只设置一个布尔（:24-26）；而每次 `connectAsync()` 开头无条件 `requestReconnect()` 把布尔重置为 `false`（`Iso8583Client.kt:75`）。
- 复现步骤（确定性，无需真实网络）：
  1. 远端先关闭，`operationComplete` 已在 EventLoop 队列中/已执行 → 一个 100ms 的重连任务已入队；
  2. 业务线程随后调用 `disconnectAsync()`（`disconnectRequested=true`，但任务已无法取消）；
  3. 100ms 后任务执行 `client.connectAsync()`：第 75 行立刻把 `disconnectRequested` 改回 `false`；若服务端恰好在此时恢复，连接被重建——与「已主动停止」相矛盾；若仍失败，还会再次自我调度（`Iso8583Client.kt:77-81`）。
- 影响（推断）：抖动场景下观察到的「明明停了又连上 / 停止后仍有重连日志」类顺序困惑多源于此；这是状态设计问题，不是日志错觉。
- 现有测试覆盖：`ClientReconnectIT`（`src/test/java/com/github/kpavlov/jreactive8583/it/ClientReconnectIT.java:14-25`）只覆盖「服务端下线→等 3 秒→服务端重启→自动重连」，不断言调度次数，也不覆盖「关闭后再主动停止」窗口。新增的并发测试覆盖了同一状态机的终局不变量（第 8 节），但未替代对本窗口的修复。

### R3. `EchoMessageListener` 对 echo *响应*也回包：级联 0x0810→0x0820→… 并对业务 listener 隐藏网络管理消息

- 机制：`applies` 只比较 class nibble（`EchoMessageListener.kt:12-13`）；j8583 `createResponse` 无条件 MTI+0x10；`onMessage` 返回 `false` 截断后面所有业务 listener（`EchoMessageListener.kt:21-27`）。
- 复现步骤（已用临时代码实证后删除）：
  1. 组装 `CompositeIsoMessageHandler`，先 add `EchoMessageListener(factory)` 再 add 一个「看见任意 0x08xx 就置标记」的业务 listener（复刻构造块顺序）；
  2. `EmbeddedChannel.writeInbound(factory.newMessage(0x0810))`；
  3. 观测：outbound 产出 0x0820（实测输出 `REPLY TYPE=0x820`），业务 listener 标记保持 `false`。
  4. 两端都启用 echo 的真实链路：A 的 ALL_IDLE 心跳 0x0800 → B 回 0x0810 → A 回 0x0820 → B 回 0x0830 → … 直到 MTI 高 nibble 溢出 class 位（&0x0800 不再命中）才停止；每跳还刷新双方的 ALL_IDLE 计时。
- 影响（推断）：单次空闲产生一串无业务含义的管理报文；业务侧任何想接收 0x0810/0x08x0 的监听器永远收不到（echo 在链头且短路）。这正属于「连接抖动后监听顺序难以解释」的一类来源。
- 现有测试覆盖：无。`EchoFromClientIT`（`src/test/java/com/github/kpavlov/jreactive8583/it/EchoFromClientIT.java:51-66`）测的是**业务** listener 对服务端主动 0x0800 回 0x0810；两端 echo 开关在测试配置里均未打开（`src/test/java/com/github/kpavlov/jreactive8583/example/client/Iso8583ClientConfig.java:41-46`、`example/server/Iso8583ServerConfig.java:30-34`），`EchoMessageListener` 本身没有单元测试。

### R4. `connect(host, port)` / `connect(SocketAddress)` 重载会阻塞到连接关闭

- 机制：`connect(SocketAddress)` 改完地址后调用无参 `connect()` 并对其返回值再 `.sync()`（`Iso8583Client.kt:61-64`）；而无参 `connect()` 返回的是 **`channel.closeFuture()`**（:34-37）。因此这两个公开重载在连接正常期间永不返回，直到第一次断线。`connect(host,port)` 同样转到该路径（:48-51）。
- 复现步骤：
  1. 启动任意 TCP 服务端；
  2. 客户端调用 `client.connect("127.0.0.1", port)`；
  3. 观测：调用线程一直阻塞，直到服务端关闭该连接才返回（期间进程看起来「卡死在 connect」）。
- 影响（推断）：在主线程/启动流程里使用这两个重载的调用方会被挂到首次断线，进而把后续初始化、监听器注册等顺序全部打乱，极易产生与重连交织的诡异时序。
- 现有测试覆盖：无。所有测试只用无参 `connect()`（`src/test/java/com/github/kpavlov/jreactive8583/it/AbstractIT.java:34`、`LockDetectionIT.java:106`）。

**附（不作为四条之一，仅记录）**：`Iso8583Server.stop()` 只关 listening socket，不关闭已 accept 的 child channel、也不关闭 EventLoopGroup（`Iso8583Server.kt:76-93`），需调用方记得再 `shutdown()`；仓库内没有针对 `stop()` 生命周期的测试。另外测试套件全局开启并行（`src/test/resources/junit-platform.properties`），且 IT 固定使用 9876 端口（`src/test/resources/application-test.properties`），并行 IT 之间存在端口互踩的脆弱性（这是测试基础设施问题，非产品代码风险）。

## 8. 新增最小并发测试

文件：`src/test/java/com/github/kpavlov/jreactive8583/netty/pipeline/ReconnectOnCloseListenerConcurrencyTest.java`

- **选定的终局不变量**：对同一条连接，「主动 stop（`requestDisconnect`）」与「连接关闭（closeFuture 通知 `operationComplete`）」并发发生时，调度器重连任务数 `∈ {0,1}`，绝不超过一次。
- **为什么它是关键不变量**：它是 R2 所描述状态机的安全边界——即使存在「已调度任务不可取消」的窗口，单次 stop/close 事件本身也绝不允许产生两个重连；破坏它意味着一次抖动可分裂出多条并行连接（`channelRef` 被互相覆盖，`sendAsync` 可能写到旧 channel）。
- **确定性手段（不用 sleep 碰运气）**：
  - `EmbeddedChannel`：无 socket、无 selector 线程、无监听端口；closeFuture 通知机制与真实 channel 完全一致。
  - 自定义单线程 `ScheduledThreadPoolExecutor`（`RecordingScheduledExecutor`）：覆盖两个 `schedule` 重载，仅计数，且把生产任务替换成 no-op，**不会真正调用 `client.connectAsync()`**，避免任何真实 I/O 与二次调度。
  - 两个 `CyclicBarrier` 让「stop 线程」与「close 线程」在同一起跑线释放并共同收尾；`@RepeatedTest(200)` 重复 200 次穷举两种交错顺序（stop 先 / close 先），断言每次 `scheduleCount ∈ [0,1]`。
  - 另有两个边界测试：仅远端关闭时必须恰好调度 1 次；先 stop 后 close 时必须 0 次。
- **资源清理（测试结束无残留）**：每次迭代 `channel.finishAndReleaseAll()` 释放 EmbeddedChannel 与全部 ByteBuf；`executor.shutdownNow()` 丢弃所有未执行的重连任务，并 `awaitTermination(5s)` 断言线程已终结。测试内仅有的两个线程（`test-stop`/`test-close`）在断言前 `join`。
- mock 边界最小：仅 mock `Iso8583Client.connectAsync()`（返回 null），被测的 `ReconnectOnCloseListener` 与 Netty 的 closeFuture 通知都是**真实生产代码路径**。

## 9. 复现与验证命令

```bash
./gradlew classes   # 安装/编译主代码
./gradlew test      # 全量测试演示（含新增并发测试与既有 IT）
# 仅跑新增测试：
./gradlew test --tests "com.github.kpavlov.jreactive8583.netty.pipeline.ReconnectOnCloseListenerConcurrencyTest"
```

新增测试结果（本地）：`tests=202, failures=0, errors=0`（200 次并发重复 + 2 个边界用例）。

## 10. 关键文件索引

- 连接骨架：`src/main/kotlin/com/github/kpavlov/jreactive8583/AbstractIso8583Connector.kt:54-64,84-103`
- Client/重连：`src/main/kotlin/com/github/kpavlov/jreactive8583/client/Iso8583Client.kt:72-90,92-116,118-128`、`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/ReconnectOnCloseListener.kt:20-44`
- Server：`src/main/kotlin/com/github/kpavlov/jreactive8583/server/Iso8583Server.kt:25-35,62-93`
- Pipeline：`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/pipeline/Iso8583ChannelInitializer.kt:64-93,118-137`
- Codec：`src/main/kotlin/com/github/kpavlov/jreactive8583/netty/codec/Iso8583Decoder.kt:31-43`、`.../Iso8583Encoder.kt:17-40`、`.../StringLengthFieldBasedFrameDecoder.kt:38-50`
- Listener/异常/idle/log：`.../pipeline/CompositeIsoMessageHandler.kt:23-96`、`.../EchoMessageListener.kt:12-27`、`.../IdleEventHandler.kt:18-29`、`.../ParseExceptionHandler.kt:26-55`、`.../IsoMessageLoggingHandler.kt:82-142`
- 配置：`src/main/kotlin/com/github/kpavlov/jreactive8583/ConnectorConfiguration.kt:177-191`、`.../client/ClientConfiguration.kt:15-39`
