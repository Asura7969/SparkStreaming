package saveOffset.common;

import redis.clients.jedis.*;
import java.util.*;
import java.util.Map.Entry;

public class RedisClient {
  private JedisPool jedisPool;
  private static RedisClient redisClient =null;

  private RedisClient() {
    Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
  }

  public static RedisClient getInstance(Properties props) {
    if (redisClient == null) {
      synchronized (RedisClient.class) {
        if (redisClient == null) {
          redisClient = new RedisClient();
          redisClient.init(props);
        }
      }
    }

    if (redisClient.jedisPool == null || redisClient.jedisPool.isClosed()) {
      synchronized (RedisClient.class) {
        if (redisClient.jedisPool == null || redisClient.jedisPool.isClosed()) {
          redisClient.init(props);
        }
      }
    }

    return redisClient;
  }

  public class CleanWorkThread extends Thread{
    @Override
    public void run() {
      System.out.println("================================Destroy jedis pool");
      if (null != jedisPool){
        jedisPool.destroy();
        jedisPool = null;
      }
    }
  }

  private void init(Properties props) {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(Integer.parseInt(props.getProperty("redis.pool.maxActive")));
    jedisPoolConfig.setMaxIdle(Integer.parseInt(props.getProperty("redis.pool.maxIdle")));
    jedisPoolConfig.setMinIdle(Integer.parseInt(props.getProperty("redis.pool.minIdle")));
    jedisPoolConfig.setMaxWaitMillis(Integer.parseInt(props.getProperty("redis.pool.maxWait")));
    jedisPoolConfig.setTestOnBorrow(true);
    jedisPoolConfig.setTestOnReturn(true);

    jedisPool = new JedisPool(jedisPoolConfig,
                              props.getProperty("redis.host"),
                              Integer.parseInt(props.getProperty("redis.port")),
                              Integer.parseInt(props.getProperty("redis.timeout")));
  }

  public JedisPool getJedisPool()
  {
    return jedisPool;
  }

  public Jedis getJedis()
  {
    return jedisPool.getResource();
  }

  public String hget(String key, String field)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hget(key, field);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,"hget fail,key=" + key + ",field=" + field);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long hlen(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hlen(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "hlen fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  /**
   * 设置简单Key-value值.
   *
   * @param key
   * @param value
   * @return
   */
  public String set(String key, String value)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.set(key, value);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "set fail,key=" + key + ",value=" + value);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  public String set(String key, String value, String nxxx, String expx,
      long time)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.set(key, value, nxxx, expx, time);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "set fail,key=" + key + ",value=" + value);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  /**
   * 设置简单Key-value值，并包括设置过期时间
   *
   * @param key
   * @param value
   * @param seconds
   *          过期时间
   * @return
   */
  public String setex(String key, int seconds, String value)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.setex(key, seconds, value);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "setex fail,key=" + key + ",value=" + value);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  /**
   * 获取简单的key对应的Value值
   *
   * @param key
   * @return
   */
  public String get(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.get(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "get fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 模糊获取key
   *
   * @param pattern
   * @return
   */
  public Set<String> keys(String pattern)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.keys(pattern);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "get fail,pattern=" + pattern);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 获取过期时间
   *
   * @param key
   * @return
   */
  public long ttl(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.ttl(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "ttl fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  /**
   * 判断key是否存在.
   *
   * @param key
   *          键值
   * @return
   */
  public boolean exist(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.exists(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "exist fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   *
   *
   * @param key
   * @param unixTime
   *          毫秒级时间戳
   * @return
   */
  public long expireAt(String key, long unixTime)
  {
    Jedis jedis = getJedis();
    try {
      unixTime = unixTime / 1000;
      return jedis.expireAt(key, unixTime);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "set key expireAt failed ,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  /**
   * 递增.
   *
   * @param key
   */
  public Long increment(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.incr(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "incr fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 递增.
   *
   * @param key
   */
  public Long incrementBy(String key, long value)
  {
    Jedis jedis = getJedis();
    try {
      jedis.incrBy(key, value);
      return jedis.incr(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "incr by fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 从sorted set获取size大小
   *
   * @param key
   * @return
   */
  public Long zcard(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zcard(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zcard fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Long zrank(String key, String member)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zrank(key, member);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zrank fail,key=" + key + ",member=" + member);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Long zrevrank(String key, String member)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zrevrank(key, member);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zrank zrevrank,key=" + key + ",member=" + member);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Set<Tuple> zrangeWithScores(String key, long start, long end)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zrangeWithScores(key, start, end);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zrangeWithScores fail,key=" + key + ",start=" + start + ", end="
              + end);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Set<Tuple> zrangeByScoreWithScores(String key, String start,
      String end)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zrangeByScoreWithScores(key, start, end);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zrangeByScoreWithScores fail,key=" + key + ",start=" + start
              + ", end=" + end);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Long zremrangeByRank(String key, long start, long end)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zremrangeByRank(key, start, end);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zremrangeByRank fail,key=" + key + ",start=" + start + ", end="
              + end);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Long zremrangeByScore(String key, double start, double end)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zremrangeByScore(key, start, end);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zremrangeByScore fail,key=" + key + ",start=" + start + ", end="
              + end);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Long zremrangeByScore(String key, String start, String end)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zremrangeByScore(key, start, end);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zremrangeByRank fail,key=" + key + ",start=" + start + ", end="
              + end);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Set<Tuple> zrevrangeWithScores(String key, long start, long end)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zrevrangeWithScores(key, start, end);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zrevrangeWithScores fail,key=" + key + ",start=" + start + ", end="
              + end);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Long zadd(String key, double score, String memeber)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zadd(key, score, memeber);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zadd fail,key=" + key + ",score=" + score + ", memeber=" + memeber);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long zadd(String key, Map<String, Double> members)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zadd(key, members);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "aadd failed ,key=" + key + ", members=" + members);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Double zincrby(String key, double score, String memeber)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zincrby(key, score, memeber);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zincrby fail,key=" + key + ",score=" + score + ", memeber="
              + memeber);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 从sorted 删除评论
   *
   * @param key
   * @param members
   *          成员
   * @return
   */
  public Long zrem(String key, String... members)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.zrem(key, members);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "zrem fail,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public boolean hexists(String key, String field)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hexists(key, field);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "judge lenovo_id and package_name exists failed,key=" + key
              + ", field=" + field);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long hset(String key, String field, String value)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hset(key, field, value);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "update userid 2 packageName mapping failed,key=" + key + ", field="
              + field);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public String hmset(String key, Map<String, String> hashes)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hmset(key, hashes);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "hmset failed,key=" + key + ", hashes=" + hashes);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long hdel(String key, String field)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hdel(key, field);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "update userid 2 packageName mapping failed,key=" + key + ", field="
              + field);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long hincrby(String key, String field, long val)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hincrBy(key, field, val);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "incrby hash failed,key=" + key + ", field=" + field);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public List<String> hmget(String key, String... fields)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hmget(key, fields);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "incrby hash failed,key=" + key + ", fields="
              + Arrays.toString(fields));
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public Map<String, String> hgetall(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hgetAll(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "hgetall hash failed,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }
  }

  /**
   * 检查对象是否在集合中
   *
   * @param key
   * @param member
   * @return
   */
  public boolean sismember(String key, String member)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.sismember(key, member);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "check key and member failed ,key=" + key + ", member=" + member);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long sadd(String key, String member)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.sadd(key, member);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "sadd failed ,key=" + key + ", member=" + member);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long srem(String key, String member)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.srem(key, member);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "srem failed ,key=" + key + ", member=" + member);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 返回键值数量
   *
   * @param key
   * @return
   */
  public long scard(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.scard(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "set key expire failed ,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 返回所有键值
   *
   * @param key
   * @return
   */
  public Set<String> smembers(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.smembers(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "set key expire failed ,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  /**
   * 设置key的过期时间
   *
   * @param key
   * @param seconds
   * @return
   */
  public long expire(String key, int seconds)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.expire(key, seconds);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "set key expire failed ,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long setnx(String key, String val)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.setnx(key, val);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "setnx failed,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long del(String key)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.del(key);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "del failed,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long decrBy(String key, int val)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.decrBy(key, val);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "decrBy failed,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public long incrBy(String key, int val)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.incrBy(key, val);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "incrBy failed,key=" + key);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public ScanResult<Entry<String, String>> hscan(String key,
      String cursor)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.hscan(key, cursor);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "hscan failed,key=" + key + ", cursor=" + cursor);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }

  public String setNxPx(String key, long ms, String value)
  {
    Jedis jedis = getJedis();
    try {
      return jedis.set(key, value, "NX", "PX", ms);
    } catch (Exception e) {
      throw new RedisOperException("REDIS_ERROR", e,
          "setNxEx fail,key=" + key + ",value=" + value + ", ms:" + ms);
    } finally {
      jedisPool.returnResourceObject(jedis);
    }

  }
}
