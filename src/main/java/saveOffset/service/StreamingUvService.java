package saveOffset.service;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import saveOffset.key.BizMinuteUrlIpKey;
import saveOffset.key.BizMinuteUrlKey;
import saveOffset.model.NginxLogModel;
import saveOffset.model.UrlMinuteIpsModel;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Properties;

public class StreamingUvService
{
  /**
   * 计算每个url每分钟的uv
   */
  public static void doUv(JavaDStream<NginxLogModel> nginxLogDStream, final Properties serverProps) {
    //设置将要对url去重的列
    JavaDStream<BizMinuteUrlIpKey> nginxLogUrlDisColumnDStream = nginxLogDStream.map(
      new Function<NginxLogModel, BizMinuteUrlIpKey>()
      {
        @Override
        public BizMinuteUrlIpKey call(NginxLogModel model) throws Exception
        {
          BizMinuteUrlIpKey key = new BizMinuteUrlIpKey();
          key.setBizType(model.getBizType());
          key.setMinute(model.getMinute());
          key.setUrl(model.getUrl());
          key.setIp(model.getIp());
          return key;
        }
      }
    );

    //去重
    JavaDStream<BizMinuteUrlIpKey> nginxLogUrlDisTranDStream = nginxLogUrlDisColumnDStream.transform(new Function<JavaRDD<BizMinuteUrlIpKey>, JavaRDD<BizMinuteUrlIpKey>>()
    {
      @Override
      public JavaRDD<BizMinuteUrlIpKey> call(JavaRDD<BizMinuteUrlIpKey> stringJavaRDD) throws Exception
      {
        return stringJavaRDD.distinct();
      }
    });

    //计算每个url的ip列表,ip以#号分隔
    JavaPairDStream<BizMinuteUrlKey,String> urlIpsPairDStream = nginxLogUrlDisTranDStream.mapToPair(
      new PairFunction<BizMinuteUrlIpKey, BizMinuteUrlKey, String>()
      {
        @Override
        public Tuple2<BizMinuteUrlKey, String> call(BizMinuteUrlIpKey s) throws Exception
        {
          BizMinuteUrlKey key = new BizMinuteUrlKey();
          key.setBizType(s.getBizType());
          key.setMinute(s.getMinute());
          key.setUrl(s.getUrl());

          return new Tuple2(key,s.getIp());
        }
      }
    ).reduceByKey(
      new Function2<String, String, String>()
      {
        @Override
        public String call(String ip1, String ip2) throws Exception
        {
          return ip1+"#"+ip2;
        }
      }
    );

    urlIpsPairDStream.print();

    //每个url对应的ip列表写入redis集合再计算每个url每小时每天的uv
    urlIpsPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<BizMinuteUrlKey, String>>()
    {
      @Override
      public void call(JavaPairRDD<BizMinuteUrlKey, String> rdd) throws Exception
      {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<BizMinuteUrlKey, String>>>()
        {
          @Override
          public void call(Iterator<Tuple2<BizMinuteUrlKey, String>> it) throws Exception
          {
            StatService service = new StatService(serverProps);

            while (it.hasNext()) {
              Tuple2<BizMinuteUrlKey, String> t = it.next();
              BizMinuteUrlKey key = t._1();

              UrlMinuteIpsModel model = new UrlMinuteIpsModel();
              model.setBizType(key.getBizType());
              model.setUrl(key.getUrl());
              model.setMinute(key.getMinute());
              model.setIps(t._2());

              service.addUrlUV(model);
            }
          }
        });
      }
    });
  }
}
