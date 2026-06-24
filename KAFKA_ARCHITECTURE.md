# Apache Kafka 架构详解

## 1. 核心架构层次

Kafka 采用 **分布式发布-订阅模型**，由以下主要层次组成：

```
客户端层 (Producers, Consumers, Admin Clients)
    ↓
网络通信层 (NetworkClient, SocketServer, NIO Selectable)
    ↓
Broker 集群层 (KRaft Mode: 代理 + 控制器)
    ↓
元数据管理层 (Raft 共识、元数据缓存)
    ↓
存储层 (日志段、索引、副本管理)
```

---

## 2. 各层核心组件

### 📤 Producer 层（生产者）

- **ProducerRecord**：要发送的记录（主题、键、值、时间戳）
- **Partitioner**：确定记录发送到哪个分区
- **RecordAccumulator**：批量收集记录到内存缓冲区
- **Sender**：异步后台线程，将批次发送到 Broker
- **Compression**：支持 Snappy、LZ4、GZIP、Zstd 等压缩算法

**关键设计**：
- 批处理（Batching）提高吞吐量
- 异步发送降低延迟
- 支持事务保证（Exactly Once）

**Producer 接口定义**（clients/src/main/java/org/apache/kafka/clients/producer/Producer.java）：

```java
public interface Producer<K, V> extends Closeable {
    Future<RecordMetadata> send(ProducerRecord<K, V> record);
    Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
    void initTransactions();
    void beginTransaction() throws ProducerFencedException;
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  ConsumerGroupMetadata groupMetadata);
    void commitTransaction() throws ProducerFencedException;
    void abortTransaction() throws ProducerFencedException;
    void flush();
    void close();
}
```

**Producer Batch 机制**（clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerBatch.java）：

```java
public final class ProducerBatch {
    final long createdMs;
    final TopicPartition topicPartition;
    final ProduceRequestResult produceFuture;
    private final List<Thunk> thunks = new ArrayList<>();
    private final MemoryRecordsBuilder recordsBuilder;
    
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, 
                                          Header[] headers, Callback callback, long now) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            // 添加到批次
        }
    }
}
```

### 📥 Consumer 层（消费者）

- **ConsumerGroup**：多个消费者共同消费一个主题
- **GroupCoordinator**：协调消费者组（分配分区、处理重平衡）
- **Fetcher**：并行从多个分区获取消息
- **OffsetManager**：管理消费进度（偏移量）

**关键特性**：
- 消费者组内分布式消费
- 自动再平衡（Rebalancing）
- 偏移量自动/手动提交

**Consumer 接口定义**（clients/src/main/java/org/apache/kafka/clients/consumer/Consumer.java）：

```java
public interface Consumer<K, V> extends Closeable {
    Set<TopicPartition> assignment();
    Set<String> subscription();
    void subscribe(Collection<String> topics);
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);
    void assign(Collection<TopicPartition> partitions);
    ConsumerRecords<K, V> poll(Duration timeout);
    void commitSync();
    void commitAsync(OffsetCommitCallback callback);
    void seek(TopicPartition partition, long offset);
}
```

**NetworkClient 层 - 客户端通信**（clients/src/main/java/org/apache/kafka/clients/NetworkClient.java）：

```java
public class NetworkClient implements KafkaClient {
    private final Selectable selector;           // 网络选择器(NIO)
    private final MetadataUpdater metadataUpdater;  // 元数据更新器
    private final ClusterConnectionStates connectionStates;  // 连接状态管理
    private final InFlightRequests inFlightRequests;  // 飞行中的请求
    
    // 发送请求给代理
    public int send(ClientRequest request);
    // 处理网络事件
    public List<ClientResponse> poll(long timeoutMs, long now);
}
```

### 🖥️ Broker 层（代理服务器）

在 **KRaft 模式**（新架构）中，每个 Broker 可同时是 Controller 和 Broker：

```
┌─────────────────────────────────────┐
│      KafkaRaftServer               │
├─────────────────────────────────────┤
│ ┌──────────────┐  ┌──────────────┐ │
│ │ Controller   │  │   Broker     │ │
│ │ (leader)     │  │ (followers)  │ │
│ └──────┬───────┘  └──────┬───────┘ │
│        └──────────┬──────┘         │
│           ┌───────▼────────┐       │
│           │ RaftClient API │       │
│           └───────┬────────┘       │
│                   │                │
│        ┌──────────▼──────────┐     │
│        │   KRaftMetadata     │     │
│        │   Cache             │     │
│        └─────────────────────┘     │
└─────────────────────────────────────┘
```

**Broker 的 3 个核心组件**：

1. **ReplicaManager（副本管理器）**
   - 管理所有分区副本
   - 处理日志写入、读取
   - 维护 ISR（In-Sync Replicas）列表
   - 处理副本同步

   ```scala
   // ReplicaManager - core/src/main/scala/kafka/server/ReplicaManager.scala
   class ReplicaManager(
       val config: KafkaConfig,
       val metrics: Metrics,
       val time: Time,
       val logManager: LogManager,
       ...) {
     // 管理所有副本
     private val allReplicas = mutable.Map[TopicPartition, Replica]()
     
     // 追加写入
     def appendRecords(...)
     
     // 获取记录
     def fetchMessages(...)
     
     // 处理副本获取
     def handleReplicaFetch(...)
   }
   ```

2. **LogManager（日志管理器）**
   - 管理日志段（~100MB 大小）
   - 删除过期日志（基于时间或大小）
   - 维护索引（偏移索引、时间索引）

   ```java
   // LogSegment - 单个日志段 (约100MB)
   public class LogSegment implements Closeable {
       private final File baseOffset;  // 基础偏移量
       private final FileRecords log;  // 日志数据
       private final OffsetIndex offsetIndex;  // 偏移索引
       private final TimeIndex timeIndex;  // 时间索引
   }

   // LogManager - 日志管理器
   public class LogManager {
       private final Map<TopicPartition, UnifiedLog> logs;
       
       public void append(TopicPartition partition, MemoryRecords records);
       public LogFetchInfo read(TopicPartition partition, long offset);
   }
   ```

3. **GroupCoordinator/TransactionCoordinator（协调器）**
   - 处理消费者组管理
   - 处理事务协调
   - 维护消费者偏移量

   ```java
   // GroupCoordinatorService - group-coordinator/src/main/java/.../GroupCoordinatorService.java
   public class GroupCoordinatorService implements GroupCoordinator {
       private final GroupCoordinatorConfig config;
       
       // 处理消费者加入
       public CoordinationResult handleJoinGroup(JoinGroupRequestData data);
       
       // 处理同步
       public CoordinationResult handleSyncGroup(SyncGroupRequestData data);
       
       // 处理偏移提交
       public CoordinationResult handleOffsetCommit(OffsetCommitRequestData data);
       
       // 进行分区分配
       public Map<String, List<TopicPartition>> assignPartitions(
           List<ConsumerGroupPartitionAssignor> assignors);
   }
   ```

### 💾 存储层（Storage）

Kafka 使用 **Log-Structured Append-Only** 设计：

```
Topic: orders
├─ Partition 0
│  ├─ Segment 0 (offset 0-999)
│  │  ├─ 00000000000000000000.log (消息数据)
│  │  ├─ 00000000000000000000.index (偏移索引)
│  │  ├─ 00000000000000000000.timeindex (时间索引)
│  │  └─ leader-epoch-checkpoint (领导者纪元)
│  │
│  └─ Segment 1 (offset 1000-1999)
│     ├─ 00000000000000001000.log
│     ├─ 00000000000000001000.index
│     └─ ...
│
└─ Partition 1 (类似结构)
```

**查询流程**：
```
消费者请求 offset 500
    ↓
使用索引二分查找到对应位置
    ↓
读取消息数据块
    ↓
解压缩（如果有）
    ↓
返回消息批次
```

### 🧠 元数据层（Metadata & Consensus）

采用 **KRaft（Kafka Raft）** 共识机制：

```
KRaft 集群（3-5 个控制器）
    │
    ├─ Leader Controller（主控制器）
    │  └─ 维护集群元数据的单一真实来源
    │     ├─ 主题和分区配置
    │     ├─ ISR 列表
    │     ├─ Broker 注册表
    │     ├─ ACL 规则
    │     └─ 配额配置
    │
    └─ Follower Controllers
       └─ 复制和备份元数据
```

**RaftClient 接口**（raft/src/main/java/org/apache/kafka/raft/RaftClient.java）：

```java
public interface RaftClient<T> extends AutoCloseable {
    interface Listener<T> {
        void handleCommit(BatchReader<T> reader);        // 处理已提交的记录
        void handleLoadSnapshot(SnapshotReader<T> reader);  // 加载快照
        void handleLeaderChange(LeaderAndEpoch leader);  // 领导者变化
    }
    
    void register(Listener<T> listener);
    void unregister(Listener<T> listener);
    OptionalLong highWatermark();
    LeaderAndEpoch leaderAndEpoch();
    CompletionStage<Integer> append(int epoch, List<T> records);
}
```

**元数据管理**：

```java
// MetadataImage - 集群元数据快照
public interface MetadataImage {
    // 代表集群的完整元数据状态
    Cluster cluster();
    TopicsImage topics();
    PartitionsImage partitions();
    BrokerImage brokers();
    ConfigurationsImage configurations();
    AclsImage acls();
}

// MetadataCache - 代理中的元数据缓存
public class KRaftMetadataCache {
    public void update(MetadataDelta delta, MetadataImage newImage);
    public Cluster getClusterMetadata();
}
```

**元数据记录类型**：
- TopicRecord（主题元数据）
- PartitionRecord（分区配置）
- BrokerRecord（Broker 注册）
- AccessControlRecord（ACL 规则）
- ConfigRecord（配置信息）

---

## 3. 数据流向

### 写入流程（Producer → Broker）

```
┌─────────────┐
│  Producer   │
│  send(rec)  │
└──────┬──────┘
       │ 1. 序列化 key/value
       ▼
┌──────────────────────┐
│   Partitioner        │
│   确定目标分区 P     │
└──────┬───────────────┘
       │ 2. 添加到缓冲区
       ▼
┌─────────────────────────────────┐
│  RecordAccumulator              │
│  ├─ ProducerBatch for P[0]      │
│  ├─ ProducerBatch for P[1]      │
│  └─ ProducerBatch for P[n]      │
└──────┬────────────────────────┘
       │ 3. 后台线程定期发送
       │    (batch.size or linger.ms)
       ▼
┌──────────────────────┐
│   Sender Thread      │
│   ├─ Compression    │
│   ├─ Sign (if auth) │
│   └─ Encrypt (opt)  │
└──────┬───────────────┘
       │ 4. Produce Request
       ▼
┌────────────────────────────────┐
│   Broker ReplicaManager        │
│   1. 追加到 Leader 日志        │
│   2. 等待 ISR 副本同步         │
│   3. 生成 RecordMetadata       │
│   - offset                     │
│   - timestamp                  │
│   - partition                  │
└──────┬───────────────────────┘
       │ 5. ProduceResponse
       ▼
┌─────────────────────┐
│  Callback Triggered │
│  或 Future 完成     │
└─────────────────────┘
```

### 读取流程（Broker → Consumer）

```
┌──────────────┐
│   Consumer   │
│  poll(500ms) │
└──────┬───────┘
       │ 1. 获取分配的分区
       │    TopicPartition[] = [t0-p0, t0-p1, t1-p0]
       ▼
┌──────────────────────┐
│   Fetcher            │
│   ├─ 并行获取每个分区 │
│   └─ 批量 Fetch Request
└──────┬───────────────┘
       │ 2. 并发发送给各 Broker
       ▼
┌────────────────────────────┐
│   Broker Log Reader        │
│   1. 根据 offset 定位      │
│   2. 使用索引加速查询      │
│   3. 读取 LogSegment       │
│   4. 解压缩                │
└──────┬─────────────────────┘
       │ 3. FetchResponse
       │    (RecordBatch[])
       ▼
┌──────────────────────┐
│   Batches Buffer     │
│   (记录缓存)         │
└──────┬───────────────┘
       │ 4. 整理为 ConsumerRecords
       ▼
┌──────────────────────┐
│   Application        │
│   处理消息           │
└──────┬───────────────┘
       │
       ▼ 5. 提交偏移量
┌──────────────────────┐
│   GroupCoordinator   │
│   保存消费进度       │
└──────────────────────┘
```

---

## 4. 核心设计模式

| 设计模式 | 描述 | 用途 |
|---------|------|------|
| **批处理** | 合并多个记录成一个批次 | 提高网络和存储效率 |
| **异步 NIO** | 非阻塞网络 I/O | 高并发连接 |
| **生产者-消费者** | 解耦系统 | 灵活扩展 |
| **日志结构** | Append-only 日志 | 顺序 I/O，高效持久化 |
| **Raft 共识** | 分布式一致性 | 元数据高可用 |
| **消费者组** | 横向扩展消费 | 负载均衡 |
| **ISR 机制** | In-Sync Replicas | 数据可靠性和可用性权衡 |

---

## 5. Kafka Streams（可选流处理库）

Kafka Streams 在 Broker 之外，是一个 **库级流处理框架**：

```
Input Topics (Kafka)
    │
    ▼
┌─────────────────────┐
│  Topology           │
│  ├─ Source Nodes    │ (读取主题)
│  ├─ Stream Node     │ (map/filter/etc)
│  ├─ Table Node      │ (KTable 聚合)
│  ├─ Processor Node  │ (自定义逻辑)
│  └─ Sink Nodes      │ (输出到主题)
└─────────────────────┘
    │
    ▼
┌─────────────────────┐
│  State Stores       │
│  ├─ RocksDB         │ (本地存储)
│  ├─ Window Store    │ (时间窗口)
│  └─ Session Store   │ (会话)
└─────────────────────┘
    │
    ▼
Output Topics (Kafka)
```

**StreamsBuilder 示例**（streams/src/main/java/org/apache/kafka/streams/StreamsBuilder.java）：

```java
public class StreamsBuilder {
    public <K, V> KStream<K, V> stream(String topic);
    public <K, V> KStream<K, V> stream(String topic, Consumed<K, V> consumed);
    public <K, V> KTable<K, V> table(String topic);
    public <K, V> GlobalKTable<K, V> globalTable(String topic);
    
    public Topology build();
}

// Topology 示例:
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> source = builder.stream("input-topic");
source
    .mapValues(value -> String.valueOf(value.length()))
    .to("output-topic");

KafkaStreams streams = new KafkaStreams(builder.build(), props);
streams.start();
```

**KafkaStreams API**（streams/src/main/java/org/apache/kafka/streams/KafkaStreams.java）：

```java
public class KafkaStreams {
    public KafkaStreams(Topology topology, Properties props);
    
    public void start();
    public void close();
    public StreamsMetadata streamsMetadataForStore(String storeName);
    
    // 状态查询 API
    public <R> StateQueryResult<R> query(StateQueryRequest<R> request);
}
```

---

## 6. 总结：Kafka 的 3 个关键创新

1. **分布式日志**
   - 高吞吐的持久化存储
   - Log-structured append-only 设计
   - 高效的顺序 I/O

2. **消费者组**
   - 灵活的消费者扩展和重平衡
   - 支持多种消费模式（at-least-once, exactly-once）
   - 自动故障转移

3. **Raft 共识**（KRaft）
   - 替代 Zookeeper
   - 简化架构，提高性能
   - 实现强一致性的元数据管理

这个架构使 Kafka 能够处理**每秒百万级**的消息吞吐，同时保证高可用性和数据一致性。

---

## 参考目录结构

| 路径 | 说明 |
|-----|------|
| `clients/` | Producer, Consumer, Admin Client APIs |
| `core/` | Broker 运行时、日志、副本管理 |
| `server/` | Broker 和 Server 组件 |
| `metadata/` | 元数据管理 |
| `raft/` | Raft 共识实现 |
| `storage/` | 日志段、检查点、分层存储 |
| `group-coordinator/` | 消费者组协调器 |
| `transaction-coordinator/` | 事务协调器 |
| `streams/` | Kafka Streams 流处理库 |
| `connect/` | Kafka Connect 数据集成 |

