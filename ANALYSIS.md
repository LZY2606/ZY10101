# JReactive-8583 调用链与顺序分析

> 分析对象：本仓库当前代码（Git `a40a1bd`），Netty `4.2.7.Final`、j8583 `3.0.0`、JDK 17 字节码目标（本地用 JDK 24 编译运行）。
> 所有文件路径相对仓库根目录，行号对应当前工作树。结论分两类标注：
> - **【Netty 保证】**：由 Netty 4.2 的线程模型 / pipeline 契约保证；
> - **【实现偶然】**：仅由当前实现的写法决定，没有契约保护，改动或配置变化即可破坏。

---

## 1. 从 connect / bind 到 pipeline 建成

### 1.1 客户端

1. `new Iso8583Client(...)` 只保存地址、配置、消息工厂；构造器 `init {}` 块在
   `addEchoMessageListener=true` 时把 `EchoMessageListener` 加进共享的
   `CompositeIsoMessageHandler`（`AbstractIso8583Connector.kt:99-103`）。
2. `client.init()`（`AbstractIso8583Connector.kt:54-59`）创建两个 EventLoopGroup
   与 `Bootstrap`：
   - `bossEventLoopGroup = NioEventLoopGroup()`（默认 `2*CPU` 线程，
     `AbstractIso8583Connector.kt:82`）——客户端**只有这一个 group** 被用于 channel
     事件循环（`Iso8583Client.kt:95`）和重连调度（`Iso8583Client.kt:113`）；
   - `workerEventLoopGroup = NioEventLoopGroup(workerThreadsCount)`（0 表示 Netty
     默认，`AbstractIso8583Connector.kt:84-91`）——**只作为 pipeline handler 的
     offload executor**，不承载任何 channel 的原生 I/O；
   - `createBootstrap()`（`Iso8583Client.kt:92-116`）装配 channel 类型、远端地址、
     `Iso8583ChannelInitializer`，并在最后创建 `ReconnectOnCloseListener`
     （`Iso8583Client.kt:109-114`）。
3. `connectAsync()`（`Iso8583Client.kt:72-90`）：
   - 先 `reconnectOnCloseListener.requestReconnect()` 把 `disconnectRequested`
     置回 `false`（`ReconnectOnCloseListener.kt:20-22`）；
   - 调 `b.connect()`；连接失败时在 connect future 回调里
     `scheduleReconnect()`（`Iso8583Client.kt:77-81`）；
   - 成功时把 channel 存入 `AtomicReference`，并给该 channel 的 `closeFuture`
     挂上同一个 `reconnectOnCloseListener`（`Iso8583Client.kt:82-87`）。
4. `connect()`（`Iso8583Client.kt:34-37`）先对 connect future `.sync()`
   等待建连，然后**返回 channel 的 closeFuture**（不是 connect future）。
   `connect(serverAddress)`（`Iso8583Client.kt:61-64`）又对这个返回值调一次
   `.sync()`——第二次 sync 会一直阻塞到该连接关闭——这是 **【实现偶然】** 的
   API 行为，与 Javadoc “Connects synchronously to specified remote address”
   的直觉相反；真正“只等建连”的入口是 `connectAsync()`。

### 1.2 服务端

1. `server.init()` 同样建两个 group（`AbstractIso8583Connector.kt:54-59`）；
   `createBootstrap()`（`Iso8583Server.kt:37-60`）用 boss/worker 两个 group，
   每个 accept 到的子 channel 使用同一个 `Iso8583ChannelInitializer` 实例
   （`Iso8583Server.kt:48-56`）。
2. `start()`（`Iso8583Server.kt:24-35`）`bind().sync().await()`，bind future
   完成回调里把**ServerSocketChannel** 存入 `channelRef`
   （`Iso8583Server.kt:28-32`）。`isStarted` 判断的就是这个监听 channel
   （`Iso8583Server.kt:70-74`）；已接受的子 channel 没有保存引用。
3. `stop()` 只关闭监听 channel（`Iso8583Server.kt:76-93`）：
   `deregister()` + `close().syncUninterruptibly()`。Netty 的
   `ServerChannel.close()` **不会自动关闭已接受的子 channel**——
   **【实现偶然】**：调用 `stop()` 后已有连接仍然存活，只有 `shutdown()`
   （`Iso8583Server.kt:62-65`，关闭 worker group）才会最终关闭它们。

### 1.3 configuration 如何决定 pipeline

`Iso8583ChannelInitializer.initChannel`（`Iso8583ChannelInitializer.kt:64-93`）
对**每个新 channel**执行一次，固定装配前三个 codec，之后按配置开关追加：

| 顺序 | name | 类型 | executor | 配置开关 | 代码 |
|---|---|---|---|---|---|
| 1 | `lengthFieldFrameDecoder` | `LengthFieldBasedFrameDecoder` 或 `StringLengthFieldBasedFrameDecoder` | channel 自己的 EventLoop | 总是 | `Iso8583ChannelInitializer.kt:66-69,118-137` |
| 2 | `iso8583Decoder` | `Iso8583Decoder extends ByteToMessageDecoder` | 同上 | 总是 | `:70` |
| 3 | `iso8583Encoder` | `Iso8583Encoder extends MessageToByteEncoder<IsoMessage>`（`@Sharable`） | 同上 | 总是 | `:71,100-104` |
| 4 | `logging` | `IsoMessageLoggingHandler extends LoggingHandler`（`@Sharable`，双向） | **workerGroup** | `addLoggingHandler`（默认 false） | `:72-74,110-116` |
| 5 | `replyOnError` | `ParseExceptionHandler`（`@Sharable`） | **workerGroup** | `replyOnError`（默认 false） | `:75-77,97-98` |
| 6 | `idleState` | `IdleStateHandler(0,0,idleTimeout)` | **workerGroup** | `addEchoMessageListener`（默认 false） | `:78-83` |
| 7 | `idleEventHandler` | `IdleEventHandler` | **workerGroup** | 同上 | `:84-88` |
| 8 | 自定义 | `CompositeIsoMessageHandler`（`@Sharable`）等 vararg | **workerGroup** | 总是（bootstrap 里传入） | `:90-91` |
| 9 | 末尾 | 用户 `ConnectorConfigurer.configurePipeline` 追加的 handler | 取决于 addLast 形式 | 可选 | `:92` |

注意开关语义：`addEchoMessageListener` 这个名字同时控制了三件事——
构造期加入 `EchoMessageListener`（`AbstractIso8583Connector.kt:100-102`）、
pipeline 中的 `IdleStateHandler` 与 `IdleEventHandler`
（`Iso8583ChannelInitializer.kt:78-89`）。**【实现偶然】**：关掉它就同时失去
idle 心跳与 echo 应答，无法单独启用其中之一。

帧参数全部来自 `ConnectorConfiguration`：
`maxFrameLength=8192`、`frameLengthFieldLength=2`、`offset=0`、`adjust=0`
（`ConnectorConfiguration.kt:8-36,204-207`），`initialBytesToStrip` 恒等于
`frameLengthFieldLength`（`Iso8583ChannelInitializer.kt:134,126`），即解帧后
长度头被剥掉。

---

## 2. 每个 handler 看到的数据形态

### 2.1 Inbound（网络字节 → 业务 listener）

1. **socket 读入**：Netty 把任意一次 TCP 读到的字节（可能半帧、整帧、多帧拼包）
   交给 `lengthFieldFrameDecoder`。形态：原始累积 `ByteBuf`。
2. **`LengthFieldBasedFrameDecoder`**（Netty 类）/
   **`StringLengthFieldBasedFrameDecoder`**（`StringLengthFieldBasedFrameDecoder.kt:25-50`，
   仅重写 `getUnadjustedFrameLength`，把 N 字节按 US-ASCII 解析为十进制长度）：
   - 半帧：不输出任何消息，字节留在内部累积区，继续等；**【Netty 保证】**。
   - 完整帧：输出一个 `ByteBuf`，内容是**剥掉 2 字节长度头之后的 ISO 报文体**；
   - 多帧拼包：一次 channelRead 中循环输出多个独立帧 `ByteBuf`；
   - 长度超过 `maxFrameLength`（8192）抛 `TooLongFrameException`（是
     `DecoderException` 子类），该帧被丢弃。
3. **`Iso8583Decoder`**（`Iso8583Decoder.kt:19-43`）继承
   `ByteToMessageDecoder`：每次被调用时 `readableBytes()` 正好是一个完整报文体，
   全部读出为 `ByteArray` 调 `messageFactory.parseMessage(bytes, 0)`
   （`J8583MessageFactory.kt:54-58`），输出 `IsoMessage`。
   **【Netty 保证】**帧 decoder 与 `ByteToMessageDecoder` 串联后，业务层不会看到
   粘包/半包；**【实现偶然】**前提是两端长度头约定一致（2 字节、值=报文体长度）。
4. **`iso8583Encoder`** 是 outbound-only（`MessageToByteEncoder`），inbound 事件
   透传；**`logging`**（若开启）作为 `ChannelDuplexHandler` 在此处记录 inbound 的
   `IsoMessage`（`IsoMessageLoggingHandler.kt:82-91`，对 IsoMessage 做脱敏格式化）。
5. **`replyOnError`** 是 inbound，正常消息透传；只在 `exceptionCaught` 上动作
   （见 §4.3）。
6. **`idleState`** 是 inbound DuplexHandler，普通消息透传，并在每次 read/write
   时重置 idle 计时；**`idleEventHandler`** 只重写 `userEventTriggered`
   （`IdleEventHandler.kt:18-29`）。
7. **`CompositeIsoMessageHandler`**（`CompositeIsoMessageHandler.kt:13-112`）拿到
   `IsoMessage`，按注册顺序遍历 listener：先 `applies`，命中才 `onMessage`；
   `onMessage` 返回 `false` 即中断链（`:50-66`）。遍历结束后它还会
   `super.channelRead(ctx, msg)`（`:40`）把消息继续向 tail 传播，以便用户经
   `configurePipeline` 加在后面的 handler 也能收到。

### 2.2 Outbound（业务 IsoMessage → 网络字节）

业务/listener 调 `ctx.writeAndFlush(isoMessage)`（如
`EchoMessageListener.kt:26`、`ParseExceptionHandler.kt:32`、
`IdleEventHandler.kt:26-27`），事件从该 handler 的 ctx **反向（向 head）**传播：

1. 经过 `logging`（记录 outbound IsoMessage）、`idleState`（刷新 write 空闲计时）；
2. 到 **`Iso8583Encoder`**（`Iso8583Encoder.kt:17-40`）：
   - `lengthHeaderLength==0`：只写 `isoMessage.writeData()` 报文体；
   - string 模式：先写 ASCII 十进制长度（`%0Nd`，正好 N 位），再写报文体；
   - 默认二进制模式：`isoMessage.writeToBuffer(lengthHeaderLength)`，j8583 用
     2 字节大端写长度头 + 报文体。与 frame decoder 的参数严格对称；
3. 到 `lengthFieldFrameDecoder`（inbound-only，透传 outbound），最终 head 写 socket。

---

## 3. 五条时间线

线程标注：`EL` = channel 注册的 EventLoop（客户端为 bossGroup 中的一个线程，
服务端子 channel 为 workerGroup 中的一个线程）；`OFF` = 经 `addLast(workerGroup,…)`
offload 后执行 handler 的 worker executor 线程。**【Netty 保证】**每个 channel 的
inbound 事件在其 pipeline 上严格按序（即使 handler 落在不同 executor，
跨 executor 也是入队后 FIFO）；但相邻两个事件可能运行在不同线程上。

### 3.1 正常请求（client 发 0x0200，server 回 0x0210）

| # | 侧 | 线程 | 事件 | 位置 |
|---|---|---|---|---|
| 1 | client | 调用线程 | `sendAsync(msg)` → `channel.writeAndFlush(IsoMessage)`，做 `isWritable` 检查 | `Iso8583Client.kt:136-140` |
| 2 | client | EL | encoder：IsoMessage → `[2B 长度][报文体]` ByteBuf | `Iso8583Encoder.kt:17-40` |
| 3 | server | EL | frameDecoder 输出剥头报文体 ByteBuf；`Iso8583Decoder` 输出 IsoMessage | `Iso8583Decoder.kt:36-42` |
| 4 | server | OFF | logging（若开）记录 inbound | `Iso8583ChannelInitializer.kt:72-74` |
| 5 | server | OFF | `CompositeIsoMessageHandler` 顺序跑 listener：命中 0x0200 的 listener 用 `createResponse` 造 0x0210 并 `writeAndFlush`，返回 `false` 终止链 | `CompositeIsoMessageHandler.kt:43-66`；示例响应见 `ClientServerIT.java:39-54` |
| 6 | server | EL | encoder 序列化 0x0210（从 listener 所在 OFF 线程提交，最终在 EL 执行写出） | 同上 |
| 7 | client | EL→OFF | 解帧、解码 0x0210；`CompositeIsoMessageHandler` 跑用户 listener | 同上 |

listener 回调在哪个线程：**【实现偶然】**——handler 是用
`pipeline.addLast(workerGroup, …)` 注册的（`Iso8583ChannelInitializer.kt:73,76,79,84,91`），
所以 `CompositeIsoMessageHandler.channelRead` 及全部 `IsoMessageListener.onMessage`
都在 workerGroup 的 executor 上执行，而**不是** I/O EventLoop。客户端 channel 的
I/O 在 bossGroup，因此客户端一定发生线程切换；服务端子 channel 的 EL 本身就在
workerGroup，Netty 发现 executor 与 channel 的 EventLoop 同组时仍可能选到同组另
一线程执行（executor 选择按注册计数轮询）。用户 listener 必须按“可能在任意 worker
线程、与 I/O 线程不同”来写。

### 3.2 半帧分两次到达

假设报文全长 2+N，TCP 分成 `2+k` 与 `N-k` 两个 segment：

1. EL：frameDecoder 收到 `2+k`，长度头可读、指示 N 字节但数据不足 → 不输出，
   字节留在累积缓冲（**【Netty 保证】**，与 `Iso8583Decoder` 无关，它根本没被
   调用）。
2. EL：第二段到达，frameDecoder 拼成整帧 → 输出剥头后的 N 字节 ByteBuf。
3. EL：`Iso8583Decoder.decode` 只在此刻被调一次，`parseMessage` 成功，输出
   IsoMessage（`Iso8583Decoder.kt:39-42`）。
4. OFF：listener 处理——**业务侧观察不到“两次到达”**，这是**【Netty 保证】**。
多帧一次到达则对称：frameDecoder 在一次 channelRead 内输出多帧，
`ByteToMessageDecoder.callDecode` 循环对每帧各调一次 `decode` 并逐个
`fireChannelRead`（Netty `ByteToMessageDecoder.callDecode`，4.2.7 字节码已核验），
listener 按帧顺序被调用两次。

### 3.3 解析失败

前提：长度合法（能成帧），但报文体内容让 j8583 抛 `java.text.ParseException`
（例：MTI 声明的位图长度与实际不符）。

1. EL：frameDecoder 正常输出报文体 ByteBuf。
2. EL：`Iso8583Decoder.decode` → `parseMessage` 抛 `ParseException`
   （`Iso8583Decoder.kt:41`）。
3. **【Netty 保证】**`ByteToMessageDecoder.callDecode` 的异常表捕获一切
   `Exception`，若非 `DecoderException` 就包成 **`DecoderException`** 重抛
   （4.2.7 `BytetoMessageDecoder` 字节码：`callDecode` exception table
   `0..146 -> 154: catch Exception → new DecoderException(cause)`）。已消费的字节
   随累积缓冲一起释放，不会无限重放坏帧。
4. 异常沿 inbound 向后传，到达 `ParseExceptionHandler.exceptionCaught`
   （`ParseExceptionHandler.kt:26-35`）。**它判断的是
   `cause is ParseException`（`:30`）——此时 cause 实际是
   `DecoderException`，判断为 false，不发送 650 错误响应**，只执行
   `ctx.fireExceptionCaught(cause)`（`:34`）。这一点已用 EmbeddedChannel 实证：
   `CAUGHT = class io.netty.handler.codec.DecoderException
   cause=java.text.ParseException: Insufficient buffer length…`，
   outbound 消息数为 0（见 §4 风险 1）。
5. 异常继续到 pipeline tail，Netty 默认只打 DEBUG 日志，**channel 不关闭**
   （**【Netty 保证】**：`exceptionCaught` 默认不 close）。
6. 例外：帧级异常（`TooLongFrameException`、长度头声称的长度超过 8192）同样是
   `DecoderException` 子类，路径相同；坏帧之后的同一 TCP 流字节仍可继续按新帧
   解析（长度帧 decoder 是自同步的）。

### 3.4 远端关闭（连接抖动主路径）

1. EL：读到 FIN → `channelInactive`/`channelUnregistered` 沿 pipeline 传播
   （logging 会打印 `CLOSE`，测试日志中可见）。
2. EL：channel 的 `closeFuture` 完成，触发在其上注册的所有
   `ChannelFutureListener`（**【Netty 保证】**：closeFuture 监听者在该 channel 的
   EventLoop 上、按注册顺序执行）。
3. EL：`ReconnectOnCloseListener.operationComplete`
   （`ReconnectOnCloseListener.kt:28-33`）：`channel.disconnect()`（对已关闭的
   NioSocketChannel 是 no-op）后调 `scheduleReconnect()`。
4. EL→读标志：检查 `disconnectRequested`（`ReconnectOnCloseListener.kt:36`）；
   若 false，向 **bossEventLoopGroup** 调度
   `client.connectAsync()`，延迟 `reconnectInterval`（默认 100ms，
   `ClientConfiguration.kt:21,33`；`ReconnectOnCloseListener.kt:38-42`）。
5. bossGroup 线程：定时任务到期跑 `connectAsync()`（`Iso8583Client.kt:72-90`）：
   - 服务端还没起来 → connect 失败 → 回调里再 `scheduleReconnect()`
     （`Iso8583Client.kt:78-80`），形成 100ms 周期重试（`ClientReconnectIT.java:14-25`
     覆盖：先 `server.shutdown()`，sleep 3 秒，再启动 server 等重连成功）；
   - connect 成功 → `channelRef` 换成新 channel（`Iso8583Client.kt:82-87`），
     新 closeFuture 挂同一个 listener。每个 channel 恰好挂一次——
     **【实现偶然】**：依赖“每次成功 connect 才挂一次、失败不挂”的写法。

### 3.5 主动 stop（客户端）/ stop（服务端）

客户端 `disconnectAsync()`（`Iso8583Client.kt:118-123`）：
1. 调用线程：`requestDisconnect()` 把标志置 true（`ReconnectOnCloseListener.kt:24-26`）；
2. 调用线程：`channel.close()`；
3. EL：closeFuture 完成 → `operationComplete` → `scheduleReconnect` 读到 true →
   不调度（`ReconnectOnCloseListener.kt:36-43`）。
`disconnect()` 额外 `.await()` 关闭完成（`Iso8583Client.kt:125-128`）。
注意 `shutdown()`（`AbstractIso8583Connector.kt:61-64`）只
`shutdownGracefully()` 两个 group，**并不设置 disconnect 标志**——若关闭时正好
有在途的重连任务，quiet period（Netty 默认 2s）内到期的任务仍会执行一次
`connectAsync()`，此时 group 正在关闭，可能产生拒绝执行类噪音日志。
**【实现偶然】**，见风险 3。

服务端 `stop()`：见 §1.2，只关监听 socket（`Iso8583Server.kt:76-93`）。

---

## 4. idle、echo、业务 listener、logging、异常的相对顺序

### 4.1 idle 事件 → 心跳

- `IdleStateHandler(readerIdleTime=0, writerIdleTime=0, allIdleTime=idleTimeout)`
  （`Iso8583ChannelInitializer.kt:82`）：只在“既没读也没写”持续 `idleTimeout`
  秒（默认 30，`ConnectorConfiguration.kt:8`；测试配置为 2，
  `application-test.properties:3`）时触发一次 `ALL_IDLE`；它注册在 workerGroup
  上，超时检测任务也跑在该 handler 绑定的 executor 上（**【Netty 保证】**
  `IdleStateHandler` 的 `initialize` 在其 `handlerAdded` 所在 executor 调度）。
- `IdleEventHandler.userEventTriggered` 只处理 `READER_IDLE`/`ALL_IDLE`
  （`IdleEventHandler.kt:22-24`），用工厂造
  `NETWORK_MANAGEMENT + REQUEST`（1987 + OTHER 角色下实测 MTI=**0x0804**）
  并 `ctx.write` + `ctx.flush`（`IdleEventHandler.kt:25-27,31-35`）。
- 事件传播顺序：`IdleStateEvent` 由 idleState 产生 → fireUserEventTriggered 到
  idleEventHandler → 继续向 tail 传（业务自定义 handler 若感兴趣也能收到）。
  **【Netty 保证】**同一 channel 上 userEvent 与 channelRead 不并发，按提交顺序
  FIFO。心跳写出走 outbound 链：`ctx.write` 从 idleEventHandler 的 ctx 向 head，
  仍经过 encoder（但不经过 composite，后者是 inbound-only）。

### 4.2 echo 请求 → echo 应答（以及 listener 顺序）

- `EchoMessageListener` 在构造 `AbstractIso8583Connector` 时加入，因此**永远是
  listener 列表第 0 个**（`AbstractIso8583Connector.kt:99-103`），用户
  `addMessageListener` 只能排在后面。
- `applies` 判定：`type and 0x0800 != 0`（`EchoMessageListener.kt:12-13`）——
  匹配的是**整个网络管理类**，不是“仅请求”。`onMessage` 造响应并写出，固定
  返回 `false`（`EchoMessageListener.kt:21-27`），所以 echo 类消息到不了任何
  用户 listener。
- 实测（默认 j8583 工厂，1987/OTHER）：收到 0x0804 → `createResponse` 生成
  **0x0814**；对端若也启用了 echo listener，`0x0800 != 0` 仍然 applies → 再
  `createResponse` 生成 0x0824 → 0x0834 → …（实测闭环推进 10 轮 MTI 不断递增，
  永不收敛）。见风险 2。

### 4.3 logging 与异常的位置

- logging 在 codec 之后、业务 handler 之前（§1.3 表）。inbound 的
  IsoMessage 日志在 OFF 线程、业务 listener 之前打印；outbound 在 OFF/EL 线程、
  encoder 之前打印（LoggingHandler 是 duplex，**【Netty 保证】**它按事件流经它的
  时刻记录）。
- `ParseExceptionHandler` 位于 logging 之后、idle/composite 之前。异常按
  inbound 方向向后传，所以 **composite 或用户 handler 抛出的异常不会回头经过
  ParseExceptionHandler**；只有 decoder/frameDecoder 抛出的异常会到达它。它无论
  是否处理都 `fireExceptionCaught` 继续传播（`ParseExceptionHandler.kt:34`）。

---

## 5. 线程、listener 回调与 reconnect future 的所有权

| 对象 | 创建者 / 所在线程 | 关键性质 |
|---|---|---|
| bossEventLoopGroup（client） | `init()`，`AbstractIso8583Connector.kt:82` | 既是客户端 channel 的 I/O EventLoopGroup，又是重连调度器（`Iso8583Client.kt:95,113`）。**【实现偶然】**两种职责共用同一组线程 |
| workerEventLoopGroup | `init()`，`:84-91` | 客户端不做 I/O，仅作 pipeline offload executor；服务端既做 accept 子 channel 的 I/O，又做 offload |
| channelRef | `AtomicReference<Channel>`，`AbstractIso8583Connector.kt:32` | connect 成功回调里写（`Iso8583Client.kt:82-87`，通常 EL），`sendAsync`/`isConnected` 在任意调用线程读（`Iso8583Client.kt:137,162-166`） |
| closeFuture listener | 每次 connect 成功在回调中挂一次（`Iso8583Client.kt:85`） | 回调在该 channel 的 EL 执行（**【Netty 保证】**）；listener 实例跨重连复用 |
| 重连 `ScheduledFuture` | `ReconnectOnCloseListener.scheduleReconnect` 调 `bossGroup.schedule(...)`（`ReconnectOnCloseListener.kt:38-42`） | **返回值被丢弃**，没有任何字段持有 → 无法取消（**【实现偶然】**，风险 3 的根因） |
| `disconnectRequested` | `AtomicBoolean`，`ReconnectOnCloseListener.kt:18` | 唯一的重连闸门；`requestReconnect` 置 false、`requestDisconnect` 置 true，读在 scheduleReconnect |
| listener 列表 | `CopyOnWriteArrayList`，`CompositeIsoMessageHandler.kt:20` | handler 是 `@Sharable`，同一实例服务该 connector 的**所有 channel（含历次重连）**；dispatch 在 worker OFF 线程，add/remove 可在任意用户线程 |

**【Netty 保证】的顺序**
- 单个 channel 上，inbound/outbound/userEvent 事件按 pipeline 方向与提交顺序
  串行化，绝不并发执行同一个 handler；跨 executor 边界通过任务队列保持 FIFO。
- closeFuture 的完成监听者在 channel 的 EventLoop 上执行。
- 同一个 `EventExecutor` 上提交的任务 FIFO；`writeAndFlush` 的写出顺序即提交顺序。

**仅【实现偶然】的顺序**
- 业务 listener 先于/后于 I/O：仅因 `addLast(workerGroup,…)` 这样注册。
- echo listener 排第 0 位：仅因构造块先于用户 `addMessageListener`。
- 一次 close 只安排一次重连：仅因 `operationComplete` 对每个 closeFuture 触发一次
  且失败路径/关闭路径各自只调一次 `scheduleReconnect`；没有“最多一次”的显式状态
  机（没有 CAS 防护、没有 in-flight future 表）。
- 重连调度在 bossGroup、延迟 100ms：硬编码传入（`Iso8583Client.kt:109-114`，
  `ClientConfiguration.kt:21`）。

---

## 6. 四个现实风险（含复现步骤与现有覆盖）

> 以下均为**代码层面可复现的风险/缺陷**，不等同于已在线上发生的故障。风险 1、2
> 的关键行为已在分析阶段用临时 EmbeddedChannel 测试实测（临时代码已删除），未被
> 当作既成事故陈述。

### 风险 1：`replyOnError=true` 对 j8583 解析错误实际不产生错误应答

- **位置**：`ParseExceptionHandler.kt:30` 判断 `cause is ParseException`；
  但 `Iso8583Decoder` 继承的 `ByteToMessageDecoder.callDecode`（Netty 4.2.7）
  会把任何非 `DecoderException` 异常包成 `DecoderException`。
- **后果**：配置打开 `replyOnError` 后，收到一个能成帧但内容非法的 ISO 报文时，
  并不会按设计发送 field 24=650 的管理消息；异常只走到 tail 被 DEBUG 打印，对端
  只能靠超时发现。功能开关形同虚设（对 j8583 `ParseException` 这一最常见错误源
  而言）。
- **复现步骤（确定性，无需网络）**：
  1. 构造 EmbeddedChannel：`LengthFieldBasedFrameDecoder(8192,0,2,0,2)` →
     `Iso8583Decoder(J8583MessageFactory(V1987, OTHER))` →
     `ParseExceptionHandler(factory,true)` → 记录 `exceptionCaught` 的尾 handler；
  2. `writeInbuf`: 2 字节长度 `0x000A` + 10 个 0x00（MTI/位图长度不合法）；
  3. 观察：outboundMessages 为空；尾 handler 收到
     `DecoderException(cause=ParseException)`。
- **修复方向（仅建议，未改代码）**：判断处解包，
  `cause is ParseException || (cause is DecoderException && cause.cause is ParseException)`，
  或在 `Iso8583Decoder` 捕获后直接转成自定义、保留原始异常类型。
- **现有覆盖**：`ParseExceptionHandlerTest.java:47-72` 直接用手工构造的
  `ParseException` 调 handler，证明“直接喂 ParseException 时会回 650”，但
  **没有任何测试经过真实 decoder 链**，所以包装导致的失效没有被捕获。
  `Iso8583DecoderTest.java:38-44` 只覆盖空 buf。判定：**覆盖缺口，风险未被现有
  测试暴露**。

### 风险 2：两端同时启用 echo listener 时形成无限网络管理消息风暴

- **位置**：`EchoMessageListener.applies` 用类掩码
  `type and 0x0800 != 0`（`EchoMessageListener.kt:12-13`），对请求与响应一视
  同仁；`onMessage` 对任何匹配消息都 `createResponse`（`EchoMessageListener.kt:25`）。
- **后果（实测）**：默认工厂下 0x0804 → 响应 0x0814 → 对端再响应 0x0824 →
  0x0834 → … MTI 功能位每轮 +0x10，永不终止（EmbeddedChannel 闭环实测连续 10
  轮无收敛）。这正是“抖动后 listener 顺序/行为难以解释”的来源之一：idle 心跳
  与 echo 应答共享同一网络管理类，且 echo 排在 listener 0。
- **复现步骤**：
  1. 一个 EmbeddedChannel，pipeline 放带 `EchoMessageListener` 的
     `CompositeIsoMessageHandler`；
  2. 写入 0x0804（NETWORK_MANAGEMENT/REQUEST，`IdleEventHandler` 心跳同款）；
  3. 循环把 outbound 读回再 writeInbound，观察 MTI 0x0814/0x0824/… 无限递增。
  真实拓扑复现：client、server 都用 `.addEchoMessageListener(true)`，一端 idle
  到点（测试配置 2s）发 0x0804 即可引爆。
- **修复方向**：`applies` 只匹配请求功能位（`(type and 0x0010)==0` 之类，按版本
  约定），或对已有的响应 MTI 不再 `createResponse`。
- **现有覆盖**：`EchoFromClientIT.java:69-74` 只覆盖“server 主动发 0x0800、
  client 用户 listener 应答 0x0810”这一单向路径，且响应用户 listener 处理而非
  EchoMessageListener；**没有两端 echo 对开的测试**。判定：未覆盖。

### 风险 3：显式 disconnect/shutdown 无法撤销在途的重连任务，标志可被后续 connect 复位

- **位置**：`ReconnectOnCloseListener.kt:35-44` 调度后不保存
  `ScheduledFuture`；`Iso8583Client.connectAsync()` 入口无条件
  `requestReconnect()`（`Iso8583Client.kt:75`）；`shutdown()` 不设置停止标志
  （`AbstractIso8583Connector.kt:61-64`）。
- **后果（两类）**：
  1. **逃脱的重连**：close 已通过第 36 行检查、任务已入队（100ms 后跑），此时
     用户调 `disconnectAsync()` 把标志置 true——任务仍会执行 `connectAsync()`，
     入口第 75 行立刻把标志复位为 false 并真的建一条新连接；用户以为已停止，
     客户端却“复活”，后续关闭再自动重连。
  2. **shutdown 竞态**：`shutdownGracefully()` 的 quiet period 内到期的重连任务
     仍会执行，向正在关闭的 group 注册 channel，产生拒绝执行/关闭中断类噪音，
     极端情况下留下半初始化 channel。
- **复现步骤**：
  1. 起 server/client（参考 `AbstractIT`），把 `reconnectInterval` 调大（如
     2000ms）制造窗口；
  2. 拔连接（`server.stop()` 只关监听后再 `channel.close()` 或直接断网）；
  3. 在 close 回调调度之后、任务到期之前调 `client.disconnectAsync()`；
  4. 等待间隔到期观察 `client.isConnected` 重新变 true（复活）。
     基于 latch 的确定性交错见本次新增测试的 interleave C
     （`stopAfterCheckPassedStillSchedulesAtMostOne`，任务至少被记录一次且无法
     撤销）。
- **修复方向**：保存并在 `requestDisconnect()` 时 `cancel(false)` 在途 future；
  `connectAsync` 入口不应无条件清标志（应检查未停止）；`shutdown()` 先
  requestDisconnect 再关 group。
- **现有覆盖**：`ClientReconnectIT.java:14-25` 只覆盖“断了→自动重连成功”的
  happy path，并且依赖 `TimeUnit.SECONDS.sleep(3)`（`:20`），不覆盖 stop/close
  竞态、不覆盖取消语义。判定：**只有正向覆盖，竞态缺口**。

### 风险 4：共享 `@Sharable` 的 listener 列表在 dispatch 期间被增删时存在越界/顺序漂移

- **位置**：`CompositeIsoMessageHandler` 标 `@Sharable`
  （`CompositeIsoMessageHandler.kt:12`），同一实例被历次重连的所有 channel 复用
  （bootstrap 里传入，`Iso8583Client.kt:104`、`Iso8583Server.kt:54`）；
  `doHandleMessage` 先取 `size` 再按下标访问
  （`CompositeIsoMessageHandler.kt:48-51`）；列表是
  `CopyOnWriteArrayList`（`:20`），`addListener/removeListener` 可在任意线程调
  （`AbstractIso8583Connector.kt:40-46`，public API）。
- **后果**：
  1. 遍历开始时捕获 `size=k`，若另一线程恰好 `remove` 使数组变短，
     `messageListeners[i]` 对最新数组取下标会 `ArrayIndexOutOfBoundsException`。
     失效机制已确定性复现：对含 5 元素的 COW 列表先捕获 size=5，再删末元素，
     `get(4)` 抛 `Index 4 out of bounds for length 4`。生产代码里这条窗口是
     “size 读取”与“get(i)”两次独立 volatile 读之间的删尾操作。
  2. COW 迭代期间新增/删除还会让**同一批消息跨重连观察到不同 listener 顺序/
     集合**——连接抖动时动态注册 listener 即可能出现“有些消息走了新 listener、
     有些没走”的现象。
- **复现步骤（确定性机制证明）**：
  1. `CopyOnWriteArrayList l=[0,1,2,3,99]`；`int size=l.size();`
  2. 另一线程（或紧接其后）`l.remove(99)`；
  3. `for(i in 0 until size) l.get(i)` → i=4 时 AIOOBE。
     压测说明：纯调度级 2.7 亿次循环未自然命中（add/remove 背靠背 volatile 写，
     窗口极窄），因此这是**潜在并发缺陷而非已观察到的故障**；真实触发需要恰好在
     dispatch 与 remove 之间存在线程切换（抖动期 GC/调度更易出现）。
- **修复方向**：遍历时取一次快照 `for (l in listeners.toList())` 或用
  iterator/for-each（COW 的 iterator 基于固定快照，越界安全），不要把 size 与
  下标访问拆到两个独立读取上。
- **现有覆盖**：`CompositeIsoMessageHandlerTest.java:36-50` 的 remove 场景是
  **单线程**在 `channelRead` 之前完成删除；`LockDetectionIT` 做多客户端压测但不
  在运行期增删 listener。判定：未覆盖并发增删。

### 现有测试覆盖总览

| 测试 | 覆盖什么 | 不覆盖什么 |
|---|---|---|
| `Iso8583ChannelInitializerTest` | 各开关是否注册对应 handler（mock pipeline） | 真实数据流、handler 顺序在真实 channel 上的效果 |
| `StringLengthFieldBasedFrameDecoderTest` | ASCII 长度头解析 + 编码对称 | 半包/拼包（用 EmbeddedChannel 直接调方法） |
| `Iso8583DecoderTest` | 空 buf 不解码 | 真实解析成功/失败路径 |
| `ParseExceptionHandlerTest` | 直接喂 `ParseException` 时回 650 | **decoder 包装后的 DecoderException（风险 1）** |
| `CompositeIsoMessageHandlerTest` | 单线程 listener 链顺序、中断、failOnError | **并发增删（风险 4）** |
| `ClientServerIT` | 0x0200→0x0210 端到端 happy path | 抖动、半帧、异常帧 |
| `EchoFromClientIT` | 单向 0x0800 通知 | **两端 echo 对开风暴（风险 2）** |
| `ClientReconnectIT` | 断线后自动重连成功（含 3s sleep） | **stop/close 竞态、任务取消（风险 3）** |
| `LockDetectionIT` | 20 客户端×100 消息无死锁 | 运行期 listener 变更；标 `slow` tag |

---

## 7. 新增的最小并发测试

文件：
`src/test/java/com/github/kpavlov/jreactive8583/netty/pipeline/ReconnectOnCloseListenerConcurrencyTest.java`

**选定的终局不变量**：同一条连接的 channel-close 通知与显式
`disconnectAsync()` 并发时，为该 close 事件安排的重连任务数 ∈ {0,1}，绝不超过
一次。

为什么选它：`ReconnectOnCloseListener` 是抖动场景所有顺序问题的收口点
（`ReconnectOnCloseListener.kt:28-44`）；“最多一次重连”是当前实现**没有用显式
状态机保护、仅靠调用路径碰巧成立**的性质（§5），最值得用测试钉死。

如何做到确定性、不靠 sleep：
- 不启 socket、不建 Netty channel：`ChannelFuture`/`Channel` 用 Mockito mock，
  `operationComplete` 直接驱动（等价于 closeFuture 在 EL 上回调）。
- 用 `GatedCloseListener`（覆写 `operationComplete` 加双 latch）把 close 侧线程
  精确停在方法入口；用 recording executor 的 latch 把 close 侧停在
  `schedule()` 内部（即已通过 `disconnectRequested` 检查的那一刻），从而确定性
  覆盖三种交错：
  - `disconnectBeforeCloseSchedulesNoReconnect`：stop 先 → 0；
  - `closeBeforeDisconnectSchedulesExactlyOneReconnect` /
    `closeCompletesBeforeStopSchedulesOneReconnect`：close 先 → 1；
  - `stopSetWhileCloseHandlerParkedSchedulesNoReconnect`：close 停在入口时 stop
    介入 → 0，且 `schedule()` 根本没被调用；
  - `stopAfterCheckPassedStillSchedulesAtMostOne`：stop 落在检查之后 → 仍是 1，
    且晚到的 stop 既不能撤销也不能复制该任务（刻画风险 3 的“逃脱重连”）；
  - `freeForAllRaceNeverSchedulesMoreThanOneReconnect`：500 次共同放行的真并发，
    每次断言计数 ∈ [0,1]。
- `RecordingScheduledExecutor` 只记录 `schedule(Callable)`、不执行任务、不创建
  任何线程，因此 `client.connectAsync()` 永不会被触发。

**无残留保证**：测试不创建 EventLoopGroup / channel / ServerSocket / 线程池；
竞态中显式 `start()` 的两个线程每次迭代都 `join()`；recording executor 无状态。
运行后无 channel、无监听端口、无线程存活。

---

## 8. 复现实验记录（分析阶段，临时测试已删除）

下列结论在分析期间用 `src/test/java/scratch/*`（已删除）实测，供审阅者重复：

1. **DecoderException 包装**：EmbeddedChannel 串
   `LengthFieldBasedFrameDecoder → Iso8583Decoder → ParseExceptionHandler → 记录尾`，
   写入 `[00 0A] + 10×00`：输出
   `CAUGHT = io.netty.handler.codec.DecoderException;
   cause = java.text.ParseException: Insufficient buffer length, needs to be at
   least 20`，outbound=0。
2. **echo 风暴**：闭环回放，MTI 序列
   `0x0804 → 0x0814 → 0x0824 → … → 0x08A4`（实测 10 轮仍继续）。
3. **COW 越界机制**：size 快照后删尾元素，`get(size-1)` 抛
   `ArrayIndexOutOfBoundsException: Index 4 out of bounds for length 4`。
4. 调度级自然竞争压测（2.72 亿次 add/remove vs get）未自然命中越界——说明窗口
   窄、属于潜在缺陷，未夸大为已发生故障。

## 9. 构建与测试命令

```bash
./gradlew classes   # 编译主代码
./gradlew test      # 运行全部测试（含新增并发测试）
```

测试并行执行（`src/test/resources/junit-platform.properties` 开启 class 间并发）；
IT 固定使用 `127.0.0.1:9876`（`application-test.properties`），新增测试不占端口，
不会与并行 IT 冲突。
