package redis.clients.jedis.providers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.ClusterCommandArguments;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.JedisClusterInfoCache;
import redis.clients.jedis.annots.Experimental;
import redis.clients.jedis.csc.Cache;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisException;

import static redis.clients.jedis.JedisCluster.INIT_NO_ERROR_PROPERTY;

public class ClusterConnectionProvider implements ConnectionProvider {

  protected final JedisClusterInfoCache cache;

  public ClusterConnectionProvider(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig) {
    this.cache = new JedisClusterInfoCache(clientConfig, clusterNodes);
    initializeSlotsCache(clusterNodes, clientConfig);
  }

  @Experimental
  public ClusterConnectionProvider(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig, Cache clientSideCache) {
    this.cache = new JedisClusterInfoCache(clientConfig, clientSideCache, clusterNodes);
    initializeSlotsCache(clusterNodes, clientConfig);
  }

// 构造函数，用于创建ClusterConnectionProvider对象
  public ClusterConnectionProvider(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig,
      GenericObjectPoolConfig<Connection> poolConfig) {
    // 创建JedisClusterInfoCache对象，用于缓存集群节点信息
    this.cache = new JedisClusterInfoCache(clientConfig, poolConfig, clusterNodes);
    // 初始化slots缓存
    initializeSlotsCache(clusterNodes, clientConfig);
  }

  @Experimental
  public ClusterConnectionProvider(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig, Cache clientSideCache,
      GenericObjectPoolConfig<Connection> poolConfig) {
    this.cache = new JedisClusterInfoCache(clientConfig, clientSideCache, poolConfig, clusterNodes);
    initializeSlotsCache(clusterNodes, clientConfig);
  }

  public ClusterConnectionProvider(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig,
      GenericObjectPoolConfig<Connection> poolConfig, Duration topologyRefreshPeriod) {
    this.cache = new JedisClusterInfoCache(clientConfig, poolConfig, clusterNodes, topologyRefreshPeriod);
    initializeSlotsCache(clusterNodes, clientConfig);
  }

  @Experimental
  public ClusterConnectionProvider(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig, Cache clientSideCache,
      GenericObjectPoolConfig<Connection> poolConfig, Duration topologyRefreshPeriod) {
    this.cache = new JedisClusterInfoCache(clientConfig, clientSideCache, poolConfig, clusterNodes, topologyRefreshPeriod);
    initializeSlotsCache(clusterNodes, clientConfig);
  }

  private void initializeSlotsCache(Set<HostAndPort> startNodes, JedisClientConfig clientConfig) {
    // 如果startNodes为空，则抛出异常
    if (startNodes.isEmpty()) {
      throw new JedisClusterOperationException("No nodes to initialize cluster slots cache.");
    }

    // 将startNodes转换为ArrayList，并打乱顺序
    ArrayList<HostAndPort> startNodeList = new ArrayList<>(startNodes);
    Collections.shuffle(startNodeList);

    // 定义一个JedisException变量，用于存储第一个异常
    JedisException firstException = null;
    // 遍历startNodeList
    for (HostAndPort hostAndPort : startNodeList) {
      try (Connection jedis = new Connection(hostAndPort, clientConfig)) {
        // 使用jedis连接，调用cache.discoverClusterNodesAndSlots方法，初始化集群槽位缓存
        cache.discoverClusterNodesAndSlots(jedis);
        // 如果成功，则返回
        return;
      } catch (JedisException e) {
        // 如果是第一个异常，则将其存储到firstException变量中
        if (firstException == null) {
          firstException = e;
        }
        // try next nodes
      }
    }

    if (System.getProperty(INIT_NO_ERROR_PROPERTY) != null) {
      return;
    }
    JedisClusterOperationException uninitializedException
        = new JedisClusterOperationException("Could not initialize cluster slots cache.");
    uninitializedException.addSuppressed(firstException);
    throw uninitializedException;
  }

  @Override
  public void close() {
    cache.close();
  }

  public void renewSlotCache() {
    cache.renewClusterSlots(null);
  }

  public void renewSlotCache(Connection jedis) {
    cache.renewClusterSlots(jedis);
  }

  public Map<String, ConnectionPool> getNodes() {
    return cache.getNodes();
  }

  public HostAndPort getNode(int slot) {
    return slot >= 0 ? cache.getSlotNode(slot) : null;
  }

  public Connection getConnection(HostAndPort node) {
    return node != null ? cache.setupNodeIfNotExist(node).getResource() : getConnection();
  }

  @Override
  public Connection getConnection(CommandArguments args) {
    final int slot = ((ClusterCommandArguments) args).getCommandHashSlot();
    return slot >= 0 ? getConnectionFromSlot(slot) : getConnection();
  }

  public Connection getReplicaConnection(CommandArguments args) {
    final int slot = ((ClusterCommandArguments) args).getCommandHashSlot();
    return slot >= 0 ? getReplicaConnectionFromSlot(slot) : getConnection();
  }

  @Override
  public Connection getConnection() {
    // In antirez's redis-rb-cluster implementation, getRandomConnection always return
    // valid connection (able to ping-pong) or exception if all connections are invalid

    List<ConnectionPool> pools = cache.getShuffledNodesPool();

    JedisException suppressed = null;
    for (ConnectionPool pool : pools) {
      Connection jedis = null;
      try {
        jedis = pool.getResource();
        if (jedis == null) {
          continue;
        }

        jedis.ping();
        return jedis;

      } catch (JedisException ex) {
        if (suppressed == null) { // remembering first suppressed exception
          suppressed = ex;
        }
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    JedisClusterOperationException noReachableNode = new JedisClusterOperationException("No reachable node in cluster.");
    if (suppressed != null) {
      noReachableNode.addSuppressed(suppressed);
    }
    throw noReachableNode;
  }

  public Connection getConnectionFromSlot(int slot) {
    // 从缓存中获取指定槽位的连接池
    ConnectionPool connectionPool = cache.getSlotPool(slot);
    if (connectionPool != null) {
      // 由于节点分配，无法保证获得有效的连接
      return connectionPool.getResource();
    } else {
      // 对于集群模式来说，没有插槽是很正常的。
      // 尝试重新发现状态
      renewSlotCache();
      connectionPool = cache.getSlotPool(slot);
      if (connectionPool != null) {
        return connectionPool.getResource();
      } else {
        // no choice, fallback to new connection to random node
        return getConnection();
      }
    }
  }

  public Connection getReplicaConnectionFromSlot(int slot) {
    List<ConnectionPool> connectionPools = cache.getSlotReplicaPools(slot);
    ThreadLocalRandom random = ThreadLocalRandom.current();
    if (connectionPools != null && !connectionPools.isEmpty()) {
      // pick up randomly a connection
      int idx = random.nextInt(connectionPools.size());
      return connectionPools.get(idx).getResource();
    }

    renewSlotCache();
    connectionPools = cache.getSlotReplicaPools(slot);
    if (connectionPools != null && !connectionPools.isEmpty()) {
      int idx = random.nextInt(connectionPools.size());
      return connectionPools.get(idx).getResource();
    }

    return getConnectionFromSlot(slot);
  }

  @Override
  public Map<String, ConnectionPool> getConnectionMap() {
    return Collections.unmodifiableMap(getNodes());
  }
}
