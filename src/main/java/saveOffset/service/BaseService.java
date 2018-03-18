package saveOffset.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import saveOffset.common.RedisClient;

public class BaseService {
  Log log = LogFactory.getLog(BaseService.class);

  public RedisClient redisClient;

  /*
  * 获取过期时间
  * */
  public int getRedisTimeOut(String timeOutFlag)
  {
    String[] s = timeOutFlag.split("-");
    int dateNum = Integer.parseInt(s[0]);
    String dateFlag = s[1];

    if ("day".equals(dateFlag)) {
      return dateNum * 24 * 60 * 60;
    } else if ("hour".equals("")) {
      return dateNum * 60 * 60;
    } else {
      return 0;
    }
  }

  /*
   * 设置key过期时间
   */
  public void setRedisKeyExpire(String key, String timeOutFlag)
  {
    try {
      if (redisClient.ttl(key) < 0) {
        int timeOut = getRedisTimeOut(timeOutFlag);

        if (timeOut == 0) {
          return;
        }

        redisClient.expire(key, timeOut);
      }
    } catch (Exception e) {
      log.error("setRedisKeyExpire error", e);
    }
  }
}
