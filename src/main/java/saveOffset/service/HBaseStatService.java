package saveOffset.service;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import saveOffset.common.DateUtils;
import saveOffset.common.HBaseClient;
import saveOffset.model.HbaseUrlMinutePvUvModel;

import java.util.Properties;

public class HBaseStatService
{
  public static Logger log = Logger.getLogger(HBaseStatService.class);

  private Properties props;
  private static HBaseStatService service;

  public static HBaseStatService getInstance(Properties props) {
    if (service == null) {
      synchronized (HBaseStatService.class) {
        if (service == null) {
          service = new HBaseStatService();
          service.props = props;
        }
      }
    }

    return service;
  }

  /**
   * 每个url每分钟的pv
   */
  public void addMinutePv(HbaseUrlMinutePvUvModel model) {
    Table table = null;
    try {
      String tableName = "mon_url_pvuv_" + DateUtils.getYearByMinute(model.getMinute());
      table = HBaseClient.getInstance(this.props).getTable(tableName);
      String rowKey = model.getBizType() + ":" + DateUtils.getDayByMinute(model.getMinute()) + ":" + model.getUrl();
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("pv"), Bytes.toBytes(DateUtils.getOnlyHourAndMinuteByMinute(model.getMinute())), model.getPv());
    } catch (Exception ex) {
      log.error("addMinutePv error", ex);
    } finally {
      HBaseClient.closeTable(table);
    }
  }

  /*
  * 每个url的uv
  * */
  public void addUrlUv(HbaseUrlMinutePvUvModel model) {
    Table table = null;
    try {
      String tableName = "mon_url_pvuv_" + DateUtils.getYearByMinute(model.getMinute());
      table = HBaseClient.getInstance(this.props).getTable(tableName);
      String rowKey = model.getBizType() + ":" + DateUtils.getDayByMinute(model.getMinute()) + ":" + model.getUrl();
      Put uvPut = new Put(Bytes.toBytes(rowKey));
      uvPut.addColumn(Bytes.toBytes("uv"), Bytes.toBytes(DateUtils.getOnlyHourAndMinuteByMinute(model.getMinute())), Bytes.toBytes(model.getUv()));
      table.put(uvPut);
    } catch (Exception ex) {
      log.error("addMinuteUv error", ex);
    } finally {
      HBaseClient.closeTable(table);
    }
  }
}
