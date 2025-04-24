package redis.clients.jedis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.annots.Experimental;
import redis.clients.jedis.annots.Internal;
import redis.clients.jedis.csc.Cache;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.JedisCluster.INIT_NO_ERROR_PROPERTY;

@Internal
public class JedisClusterInfoCache {

  private static final Logger logger = LoggerFactory.getLogger(JedisClusterInfoCache.class);

  // 节点连接池
  private final Map<String, ConnectionPool> nodes = new HashMap<>();
  // 插槽连接池
  private final ConnectionPool[] slots = new ConnectionPool[Protocol.CLUSTER_HASHSLOTS];
  // 插槽节点
  private final HostAndPort[] slotNodes = new HostAndPort[Protocol.CLUSTER_HASHSLOTS];
  // 副本槽位
  private final List<ConnectionPool>[] replicaSlots;

  // 读写锁
  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
  private final Lock r = rwl.readLock();
  private final Lock w = rwl.writeLock();
  private final Lock rediscoverLock = new ReentrantLock();

  // 连接池配置
  private final GenericObjectPoolConfig<Connection> poolConfig;
  // 客户端配置
  private final JedisClientConfig clientConfig;
  // 客户端缓存
  private final Cache clientSideCache;
  // 起始节点
  private final Set<HostAndPort> startNodes;

  private static final int MASTER_NODE_INDEX = 2;

  /**
   * The single thread executor for the topology refresh task.
   */
  // 拓扑刷新任务的单线程执行器
  private ScheduledExecutorService topologyRefreshExecutor = null;

  // 拓扑刷新任务
  class TopologyRefreshTask implements Runnable {
    @Override
    public void run() {
      // 打印旧节点
      logger.debug("Cluster topology refresh run, old nodes: {}", nodes.keySet());
      // 刷新集群槽位
      renewClusterSlots(null);
      // 打印新节点
      logger.debug("Cluster topology refresh run, new nodes: {}", nodes.keySet());
    }
  }

  // 构造函数，用于初始化JedisClusterInfoCache对象
  public JedisClusterInfoCache(final JedisClientConfig clientConfig, final Set<HostAndPort> startNodes) {
    this(clientConfig, null, null, startNodes);
  }

  @Experimental
  // 构造函数，用于初始化JedisClusterInfoCache对象
  public JedisClusterInfoCache(final JedisClientConfig clientConfig, Cache clientSideCache,
      final Set<HostAndPort> startNodes) {
    this(clientConfig, clientSideCache, null, startNodes);
  }

  // 构造函数，用于初始化JedisClusterInfoCache对象
  public JedisClusterInfoCache(final JedisClientConfig clientConfig,
      final GenericObjectPoolConfig<Connection> poolConfig, final Set<HostAndPort> startNodes) {
    this(clientConfig, null, poolConfig, startNodes);
  }

  @Experimental
  // 构造函数，用于初始化JedisClusterInfoCache对象
  public JedisClusterInfoCache(final JedisClientConfig clientConfig, Cache clientSideCache,
      final GenericObjectPoolConfig<Connection> poolConfig, final Set<HostAndPort> startNodes) {
    this(clientConfig, clientSideCache, poolConfig, startNodes, null);
  }

  // 构造函数，用于初始化JedisClusterInfoCache对象
  public JedisClusterInfoCache(final JedisClientConfig clientConfig,
      final GenericObjectPoolConfig<Connection> poolConfig, final Set<HostAndPort> startNodes,
      final Duration topologyRefreshPeriod) {
    this(clientConfig, null, poolConfig, startNodes, topologyRefreshPeriod);
  }

  @Experimental
  // 构造函数，用于初始化JedisClusterInfoCache对象
  public JedisClusterInfoCache(final JedisClientConfig clientConfig, Cache clientSideCache,
      final GenericObjectPoolConfig<Connection> poolConfig, final Set<HostAndPort> startNodes,
      final Duration topologyRefreshPeriod) {
    // 初始化连接池配置
    this.poolConfig = poolConfig;
    // 初始化客户端配置
    this.clientConfig = clientConfig;
    // 初始化客户端缓存
    this.clientSideCache = clientSideCache;
    // 初始化起始节点
    this.startNodes = startNodes;
    // 如果客户端配置中有AuthXManager，则启动
    if (clientConfig.getAuthXManager() != null) {
      clientConfig.getAuthXManager().start();
    }
    // 如果拓扑刷新周期不为空，则启动拓扑刷新任务
    if (topologyRefreshPeriod != null) {
      // 记录集群拓扑刷新开始，周期为topologyRefreshPeriod，开始节点为startNodes
      logger.info("Cluster topology refresh start, period: {}, startNodes: {}", topologyRefreshPeriod, startNodes);
      // 创建一个单线程调度器
      topologyRefreshExecutor = Executors.newSingleThreadScheduledExecutor();
      // 使用固定延迟调度，每隔topologyRefreshPeriod.toMillis()毫秒执行一次TopologyRefreshTask
      topologyRefreshExecutor.scheduleWithFixedDelay(new TopologyRefreshTask(), topologyRefreshPeriod.toMillis(),
          topologyRefreshPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }
    // 如果客户端配置中启用了只读副本，则初始化副本槽位
    if (clientConfig.isReadOnlyForRedisClusterReplicas()) {
      // 创建一个长度为Protocol.CLUSTER_HASHSLOTS的ArrayList数组
      replicaSlots = new ArrayList[Protocol.CLUSTER_HASHSLOTS];
    } else {
      // 否则，副本槽位为空
      replicaSlots = null;
    }
  }

  /**
   * Check whether the number and order of slots in the cluster topology are equal to CLUSTER_HASHSLOTS
  // 检查集群拓扑中的插槽数量和顺序是否等于CLUSTER_HASHSLOTS
   * @param slotsInfo the cluster topology
   * @return if slots is ok, return true, elese return false.
   */
  private boolean checkClusterSlotSequence(List<Object> slotsInfo) {
    List<Integer> slots = new ArrayList<>();
    for (Object slotInfoObj : slotsInfo) {
      List<Object> slotInfo = (List<Object>)slotInfoObj;
      slots.addAll(getAssignedSlotArray(slotInfo));
    }
    Collections.sort(slots);
    if (slots.size() != Protocol.CLUSTER_HASHSLOTS) {
      return false;
    }
    for (int i = 0; i < Protocol.CLUSTER_HASHSLOTS; ++i) {
      if (i != slots.get(i)) {
        return false;
      }
    }
  // 执行集群插槽命令，获取插槽信息
    return true;
  }

  public void discoverClusterNodesAndSlots(Connection jedis) {
    // 执行集群插槽命令，获取插槽信息
    List<Object> slotsInfo = executeClusterSlots(jedis);
    // 如果没有设置INIT_NO_ERROR_PROPERTY属性，则进行以下操作
    if (System.getProperty(INIT_NO_ERROR_PROPERTY) == null) {
      // 如果插槽信息为空，则抛出异常
      if (slotsInfo.isEmpty()) {
        throw new JedisClusterOperationException("Cluster slots list is empty.");
      }
      // 如果插槽信息有空洞，则抛出异常
      if (!checkClusterSlotSequence(slotsInfo)) {
        throw new JedisClusterOperationException("Cluster slots have holes.");
      }
    }
    // 加锁
    w.lock();
    try {
      // 重置
      reset();
      // 遍历插槽信息
      for (Object slotInfoObj : slotsInfo) {
        List<Object> slotInfo = (List<Object>) slotInfoObj;

        // 如果插槽信息长度小于等于MASTER_NODE_INDEX，则跳过
        if (slotInfo.size() <= MASTER_NODE_INDEX) {
          continue;
        }

        // 获取分配的插槽数组
        List<Integer> slotNums = getAssignedSlotArray(slotInfo);

        // hostInfos
        int size = slotInfo.size();
        // 遍历插槽信息
        for (int i = MASTER_NODE_INDEX; i < size; i++) {
          List<Object> hostInfos = (List<Object>) slotInfo.get(i);
          // 如果hostInfos为空，则跳过
          if (hostInfos.isEmpty()) {
            continue;
          }

          // 生成HostAndPort对象
          HostAndPort targetNode = generateHostAndPort(hostInfos);
          // 如果节点不存在，则设置节点
          setupNodeIfNotExist(targetNode);
          // 如果是主节点，则分配插槽
          if (i == MASTER_NODE_INDEX) {
            assignSlotsToNode(slotNums, targetNode);
          // 如果是只读副本节点，则分配插槽
          } else if (clientConfig.isReadOnlyForRedisClusterReplicas()) {
            assignSlotsToReplicaNode(slotNums, targetNode);
          }
        }
      }
    } finally {
      // 解锁
      w.unlock();
  // 刷新集群插槽
    }
  }

  public void renewClusterSlots(Connection jedis) {
    // 如果重新发现已经在进行中 - 无需再开始一次相同的重新发现，只需返回即可
    if (rediscoverLock.tryLock()) {
      try {
        // 首先，如果 jedis 可用，请使用 jedis renew。
        if (jedis != null) {
          try {
            discoverClusterSlots(jedis);
            return;
          } catch (JedisException e) {
            // try nodes from all pools
          }
        }

        // 然后，我们使用 startNodes 来尝试，只要 startNodes 可用，
        // 无论是 VIP、域名还是物理 IP，都会成功。
        if (startNodes != null) {
          for (HostAndPort hostAndPort : startNodes) {
            try (Connection j = new Connection(hostAndPort, clientConfig)) {
              discoverClusterSlots(j);
              return;
            } catch (JedisException e) {
              // try next nodes
            }
          }
        }

        // 返回 ShuffledNodesPool 并尝试剩余的物理节点。
        for (ConnectionPool jp : getShuffledNodesPool()) {
          try (Connection j = jp.getResource()) {
            // 如果已在 startNodes 中尝试过，请跳过此节点。
            if (startNodes != null && startNodes.contains(j.getHostAndPort())) {
              continue;
            }
            discoverClusterSlots(j);
            return;
          } catch (JedisException e) {
            // try next nodes
          }
        }

      } finally {
        rediscoverLock.unlock();
      }
  // 执行集群插槽命令，获取插槽信息
    }
  }

  private void discoverClusterSlots(Connection jedis) {
    List<Object> slotsInfo = executeClusterSlots(jedis);
    if (System.getProperty(INIT_NO_ERROR_PROPERTY) == null) {
      if (slotsInfo.isEmpty()) {
        throw new JedisClusterOperationException("Cluster slots list is empty.");
      }
      if (!checkClusterSlotSequence(slotsInfo)) {
        throw new JedisClusterOperationException("Cluster slots have holes.");
      }
    }
    w.lock();
    try {
      Arrays.fill(slots, null);
      Arrays.fill(slotNodes, null);
      if (clientSideCache != null) {
        clientSideCache.flush();
      }
      Set<String> hostAndPortKeys = new HashSet<>();

      for (Object slotInfoObj : slotsInfo) {
        List<Object> slotInfo = (List<Object>) slotInfoObj;

        if (slotInfo.size() <= MASTER_NODE_INDEX) {
          continue;
        }

        List<Integer> slotNums = getAssignedSlotArray(slotInfo);

        int size = slotInfo.size();
        for (int i = MASTER_NODE_INDEX; i < size; i++) {
          List<Object> hostInfos = (List<Object>) slotInfo.get(i);
          if (hostInfos.isEmpty()) {
            continue;
          }

          HostAndPort targetNode = generateHostAndPort(hostInfos);
          hostAndPortKeys.add(getNodeKey(targetNode));
          setupNodeIfNotExist(targetNode);
          if (i == MASTER_NODE_INDEX) {
            assignSlotsToNode(slotNums, targetNode);
          } else if (clientConfig.isReadOnlyForRedisClusterReplicas()) {
            assignSlotsToReplicaNode(slotNums, targetNode);
          }
        }
      }

      // 根据最新查询删除死节点
      Iterator<Entry<String, ConnectionPool>> entryIt = nodes.entrySet().iterator();
      while (entryIt.hasNext()) {
        Entry<String, ConnectionPool> entry = entryIt.next();
        if (!hostAndPortKeys.contains(entry.getKey())) {
          ConnectionPool pool = entry.getValue();
          try {
            if (pool != null) {
              pool.destroy();
            }
          } catch (Exception e) {
            // pass, may be this node dead
          }
          entryIt.remove();
        }
      }
    } finally {
      w.unlock();
  // 生成HostAndPort对象
    }
  }

  private HostAndPort generateHostAndPort(List<Object> hostInfos) {
    String host = SafeEncoder.encode((byte[]) hostInfos.get(0));
    int port = ((Long) hostInfos.get(1)).intValue();
  // 如果节点不存在，则设置节点
    return new HostAndPort(host, port);
  }

  public ConnectionPool setupNodeIfNotExist(final HostAndPort node) {
    // 加锁
    w.lock();
    try {
      // 获取节点key
      String nodeKey = getNodeKey(node);
      // 获取已存在的连接池
      ConnectionPool existingPool = nodes.get(nodeKey);
      // 如果已存在，则返回
      if (existingPool != null) return existingPool;

      // 创建节点连接池
      ConnectionPool nodePool = createNodePool(node);
      // 将连接池放入nodes中
      nodes.put(nodeKey, nodePool);
      // 返回连接池
      return nodePool;
    } finally {
      // 解锁
      w.unlock();
  // 创建节点连接池
    }
  }

  private ConnectionPool createNodePool(HostAndPort node) {
    // 如果poolConfig为空
    if (poolConfig == null) {
      // 如果clientSideCache为空
      if (clientSideCache == null) {
        // 返回一个新的ConnectionPool对象，参数为node和clientConfig
        return new ConnectionPool(node, clientConfig);
      } else {
        // 返回一个新的ConnectionPool对象，参数为node、clientConfig和clientSideCache
        return new ConnectionPool(node, clientConfig, clientSideCache);
      }
    } else {
      // 如果clientSideCache为空
      if (clientSideCache == null) {
        // 返回一个新的ConnectionPool对象，参数为node、clientConfig和poolConfig
        return new ConnectionPool(node, clientConfig, poolConfig);
      } else {
        // 返回一个新的ConnectionPool对象，参数为node、clientConfig、clientSideCache和poolConfig
        return new ConnectionPool(node, clientConfig, clientSideCache, poolConfig);
      }
  // 分配插槽到节点
    }
  }

  public void assignSlotToNode(int slot, HostAndPort targetNode) {
    w.lock();
    try {
      ConnectionPool targetPool = setupNodeIfNotExist(targetNode);
      slots[slot] = targetPool;
      slotNodes[slot] = targetNode;
    } finally {
      w.unlock();
  // 分配插槽到节点
    }
  }

  public void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode) {
    // 加锁
    w.lock();
    try {
      // 如果目标节点不存在，则设置目标节点
      ConnectionPool targetPool = setupNodeIfNotExist(targetNode);
      // 遍历目标插槽
      for (Integer slot : targetSlots) {
        // 将插槽分配给目标节点
        slots[slot] = targetPool;
        // 将插槽节点设置为目标节点
        slotNodes[slot] = targetNode;
      }
    } finally {
      // 解锁
      w.unlock();
  // 分配插槽到只读副本节点
    }
  }

  public void assignSlotsToReplicaNode(List<Integer> targetSlots, HostAndPort targetNode) {
    w.lock();
    try {
      ConnectionPool targetPool = setupNodeIfNotExist(targetNode);
      for (Integer slot : targetSlots) {
        if (replicaSlots[slot] == null) {
          replicaSlots[slot] = new ArrayList<>();
        }
        replicaSlots[slot].add(targetPool);
      }
    } finally {
      w.unlock();
  // 获取节点连接池
    }
  }

  public ConnectionPool getNode(String nodeKey) {
    r.lock();
    try {
      return nodes.get(nodeKey);
    } finally {
      r.unlock();
  // 获取节点连接池
    }
  }

  public ConnectionPool getNode(HostAndPort node) {
  // 获取插槽连接池
    return getNode(getNodeKey(node));
  }

  public ConnectionPool getSlotPool(int slot) {
    r.lock();
    try {
      return slots[slot];
    } finally {
      r.unlock();
  // 获取插槽节点
    }
  }

  public HostAndPort getSlotNode(int slot) {
    r.lock();
    try {
      return slotNodes[slot];
    } finally {
      r.unlock();
  // 获取只读副本插槽连接池
    }
  }

  public List<ConnectionPool> getSlotReplicaPools(int slot) {
    r.lock();
    try {
      return replicaSlots[slot];
    } finally {
      r.unlock();
  // 获取所有节点连接池
    }
  }

  public Map<String, ConnectionPool> getNodes() {
    r.lock();
    try {
      return new HashMap<>(nodes);
    } finally {
      r.unlock();
  // 获取所有节点连接池
    }
  }

  public List<ConnectionPool> getShuffledNodesPool() {
    r.lock();
    try {
      List<ConnectionPool> pools = new ArrayList<>(nodes.values());
      Collections.shuffle(pools);
      return pools;
    } finally {
      r.unlock();
    }
  }

  // 清除发现的节点集合并释放分配的资源
  /**
   * Clear discovered nodes collections and gently release allocated resources
   */
  public void reset() {
    w.lock();
    try {
      for (ConnectionPool pool : nodes.values()) {
        try {
          if (pool != null) {
            pool.destroy();
          }
        } catch (RuntimeException e) {
          // pass
        }
      }
      nodes.clear();
      Arrays.fill(slots, null);
      Arrays.fill(slotNodes, null);
    } finally {
      w.unlock();
  // 关闭
    }
  }

  public void close() {
    reset();
    if (topologyRefreshExecutor != null) {
      logger.info("Cluster topology refresh shutdown, startNodes: {}", startNodes);
      topologyRefreshExecutor.shutdownNow();
  // 获取节点键
    }
  }

  public static String getNodeKey(HostAndPort hnp) {
    //return hnp.getHost() + ":" + hnp.getPort();
  // 执行集群插槽命令，获取插槽信息
    return hnp.toString();
  }

  private List<Object> executeClusterSlots(Connection jedis) {
    jedis.sendCommand(Protocol.Command.CLUSTER, "SLOTS");
  // 获取分配的插槽数组
    return jedis.getObjectMultiBulkReply();
  }

  private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
    List<Integer> slotNums = new ArrayList<>();
    for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
        .intValue(); slot++) {
      slotNums.add(slot);
    }
    return slotNums;
  }
}
