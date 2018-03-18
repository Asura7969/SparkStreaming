package saveOffset.service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import saveOffset.common.RedisClient;
import saveOffset.model.UrlMinuteIpsModel;

import java.util.Properties;

public class RedisStatService extends BaseService
{
  private static RedisStatService service;

  public static RedisStatService getInstance(Properties props) {
    if (service == null) {
      synchronized (RedisStatService.class) {
        if (service == null) {
          service = new RedisStatService();
          service.redisClient = RedisClient.getInstance(props);
        }
      }
    }

    return service;
  }

  /**
   * 获取每个url每分钟的uv
   */
  public long getUrlMinuteUv(UrlMinuteIpsModel model) {
    String key = "ng:set:ips:"+ model.getBizType()+":"+ model.getUrl()+":"+ model.getMinute();

    JedisPool jedisPool = redisClient.getJedisPool();
    Jedis jedis = jedisPool.getResource();
    Pipeline pipelined = jedis.pipelined();

    String[] ips = model.getIps().split("#");

    for (String ip : ips) {
      pipelined.sadd(key, ip);
    }

    pipelined.sync();
    jedisPool.returnResourceObject(jedis);

    setRedisKeyExpire(key,"2-day");

    return redisClient.scard(key);
  }
}
