package saveOffset.service;


import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import saveOffset.key.BizMinuteUrlKey;
import saveOffset.model.NginxLogModel;
import saveOffset.model.UrlMinutePvModel;
import scala.Tuple2;
import java.util.Iterator;
import java.util.Properties;

public class StreamingPvService
{
  public static Logger log = Logger.getLogger(StreamingPvService.class);

  public static void doPv(JavaDStream<NginxLogModel> nginxLogDStream, final Properties serverProps) {
    //计算每个url的pv
    JavaPairDStream<BizMinuteUrlKey, Long> pvPairDStream = nginxLogDStream.mapToPair(
      new PairFunction<NginxLogModel, BizMinuteUrlKey, Long>()
      {
        @Override
        public Tuple2<BizMinuteUrlKey, Long> call(NginxLogModel model)
        {
          BizMinuteUrlKey key = new BizMinuteUrlKey();
          key.setBizType(model.getBizType());
          key.setMinute(model.getMinute());
          key.setUrl(model.getUrl());

          return new Tuple2(key, 1L);
        }
      }
    ).reduceByKey(
      new Function2<Long, Long, Long>()
      {
        @Override
        public Long call(Long i1, Long i2)
        {
          return i1 + i2;
        }
      }
    );

    pvPairDStream.print();

    pvPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<BizMinuteUrlKey, Long>>()
    {
      @Override
      public void call(JavaPairRDD<BizMinuteUrlKey, Long> rdd) throws Exception
      {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<BizMinuteUrlKey, Long>>>()
        {
          @Override
          public void call(Iterator<Tuple2<BizMinuteUrlKey, Long>> it) throws Exception
          {
            StatService service = new StatService(serverProps);

            while (it.hasNext()) {
              Tuple2<BizMinuteUrlKey, Long> t = it.next();
              BizMinuteUrlKey key = t._1();

              UrlMinutePvModel model = new UrlMinutePvModel();
              model.setBizType(key.getBizType());
              model.setMinute(key.getMinute());
              model.setUrl(key.getUrl());
              model.setPv(t._2());

              service.addPV(model);
            }
          }
        });
      }
    });
  }
}
