package saveOffset.service;

import org.apache.log4j.Logger;
import saveOffset.model.HbaseUrlMinutePvUvModel;
import saveOffset.model.UrlMinuteIpsModel;
import saveOffset.model.UrlMinutePvModel;

import java.util.Properties;

public class StatService
{
  public static Logger log = Logger.getLogger(StatService.class);
  private Properties props;

  public StatService(Properties props) {
    this.props = props;
  }

  /**
   * 写PV
   */
  public void addPV(UrlMinutePvModel model) {
    //写HBASE
    HBaseStatService hBaseStatService = HBaseStatService.getInstance(props);

    //hbase添加每个url的pv
    HbaseUrlMinutePvUvModel hbaseUrlMinuteSecondPartPvUvModel = new HbaseUrlMinutePvUvModel();
    hbaseUrlMinuteSecondPartPvUvModel.setBizType(model.getBizType());
    hbaseUrlMinuteSecondPartPvUvModel.setMinute(model.getMinute());
    hbaseUrlMinuteSecondPartPvUvModel.setUrl(model.getUrl());
    hbaseUrlMinuteSecondPartPvUvModel.setPv(model.getPv());
    hBaseStatService.addMinutePv(hbaseUrlMinuteSecondPartPvUvModel);
  }

  /**
   * 写url的UV
   */
  public void addUrlUV(UrlMinuteIpsModel model) {
    //从redis获取uv
    RedisStatService redisStatService = RedisStatService.getInstance(props);
    long uv = redisStatService.getUrlMinuteUv(model);

    //写hbase
    HbaseUrlMinutePvUvModel hm = new HbaseUrlMinutePvUvModel();
    hm.setMinute(model.getMinute());
    hm.setBizType(model.getBizType());
    hm.setUrl(model.getUrl());
    hm.setUv(uv);
    HBaseStatService.getInstance(props).addUrlUv(hm);
  }
}
