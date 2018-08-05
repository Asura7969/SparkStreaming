package saveOffset;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import saveOffset.common.PropertiesUtil;
import saveOffset.factory.ModelFactory;
import saveOffset.model.NginxLogModel;
import saveOffset.monitor.MonitorStopThread;
import saveOffset.service.StreamingPvService;
import saveOffset.service.StreamingUvService;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
/**
 * java版保存kafka消费偏移量
 */
public class SaveOffset {

    public static Logger log = Logger.getLogger(SaveOffset.class);

    public static void main(String[] args) throws Exception
    {
        if (args.length < 1) {
            System.err.println("Usage: NginxLogStreaming ConfigFile");
            System.exit(1);
        }

        String configFile = args[0];
        final Properties serverProps = PropertiesUtil.getProperties(configFile);
        printConfig(serverProps);

        String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");

        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointPath, createContext(serverProps));
        javaStreamingContext.start();

        log.info("MonitorStopThread start");
        Thread thread = new Thread(new MonitorStopThread(javaStreamingContext, serverProps));
        thread.start();
        log.info("MonitorStopThread started");

        javaStreamingContext.awaitTermination();
    }

    public static Function0<JavaStreamingContext> createContext(final Properties serverProps) {

        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>()
        {
            @Override
            public JavaStreamingContext call() throws Exception
            {
                String topicStr = serverProps.getProperty("kafka.topic");
                Set<String> topicSet = new HashSet(Arrays.asList(topicStr.split(",")));

                final String groupId = serverProps.getProperty("kafka.groupId");
                final Long streamingInterval = Long.parseLong(serverProps.getProperty("streaming.interval"));
                final String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
                final String metadataBrokerList = serverProps.getProperty("kafka.metadata.broker.list");

                final Map<String, String> kafkaParams = new HashMap();
                kafkaParams.put("metadata.broker.list", metadataBrokerList);
                kafkaParams.put("group.id",groupId);

                final KafkaCluster kafkaCluster = getKafkaCluster(kafkaParams);
                Map<TopicAndPartition, Long> consumerOffsetsLong = SaveOffset.getConsumerOffsets(kafkaCluster,groupId,topicSet);

                SparkConf sparkConf = new SparkConf().setAppName("NginxLogStreaming");
                sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                sparkConf.set("spark.kryo.registrator", "com.djt.spark.registrator.MyKryoRegistrator");
                sparkConf.set("spark.streaming.kafka.maxRatePerPartition","100");

                JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(streamingInterval));
                javaStreamingContext.checkpoint(checkpointPath);

                JavaInputDStream<String> kafkaMessageDStream = KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        String.class,
                        kafkaParams,
                        consumerOffsetsLong,
                        new Function<MessageAndMetadata<String, String>, String>() {
                            @Override
                            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                                return v1.message();
                            }
                        }
                );

                // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
                final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference();

                JavaDStream<String> kafkaMessageDStreamTransform = kafkaMessageDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {

                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);

                        for (OffsetRange o : offsetRanges.get()) {
                            log.info("topic="+o.topic()
                                    + ",partition=" + o.partition()
                                    + ",fromOffset=" + o.fromOffset()
                                    + ",untilOffset=" + o.untilOffset()
                                    + ",rddpartitions=" + rdd.getNumPartitions()
                                    + ",isempty=" + rdd.isEmpty()
                                    + ",name=" + rdd.name()
                                    + ",id=" + rdd.id());
                        }

                        return rdd;
                    }
                });

                kafkaMessageDStreamTransform.print();

                //将nginx日志转换为对象
                JavaDStream<NginxLogModel> nginxLogDStreamObject = kafkaMessageDStreamTransform.map(new Function<String, NginxLogModel>() {
                    @Override
                    public NginxLogModel call(String message) {
                        return ModelFactory.getModel(message);
                    }
                });

                nginxLogDStreamObject.print();

                //过滤空对象
                JavaDStream<NginxLogModel> nginxLogDStream = nginxLogDStreamObject.filter(new Function<NginxLogModel, Boolean>() {
                    @Override
                    public Boolean call(NginxLogModel model)
                    {
                        if (model == null
                                || StringUtils.isEmpty(model.getIp())
                                || StringUtils.isEmpty(model.getMinute())
                                || StringUtils.isEmpty(model.getUrl())) {
                            return false;
                        }

                        return true;
                    }
                });

                nginxLogDStream.print();

                nginxLogDStream.cache();

                //计算每个url的pv
                StreamingPvService.doPv(nginxLogDStream, serverProps);

                //计算url和业务的uv
                StreamingUvService.doUv(nginxLogDStream, serverProps);

                //kafka offset写入zk
                offsetToZk(kafkaCluster, kafkaMessageDStreamTransform, offsetRanges, groupId);

                return javaStreamingContext;
            }
        };

        return createContextFunc;
    }

    /**
     * 获取kafka每个分区消费到的offset,以便继续消费
     * @param kafkaCluster
     * @param groupId
     * @param topicSet
     * @return
     */
    public static Map<TopicAndPartition, Long> getConsumerOffsets(KafkaCluster kafkaCluster, String groupId, Set<String> topicSet) {
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = (scala.collection.immutable.Set<TopicAndPartition>) kafkaCluster.getPartitions(immutableTopics).right().get();

        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap();

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).isLeft()) {
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }

            return consumerOffsetsLong;
        } else { // offset已存在, 使用保存的offset
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
                    (scala.collection.immutable.Map<TopicAndPartition, Object>) kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }
        }

        return consumerOffsetsLong;
    }

    /**
     * 将offset写入zk
     * @param kafkaCluster
     * @param javaDStream
     * @param offsetRanges
     * @param groupId
     */
    public static void offsetToZk(final KafkaCluster kafkaCluster,
                                  JavaDStream javaDStream,
                                  final AtomicReference<OffsetRange[]> offsetRanges,
                                  final String groupId) {
        //offset写入zk
        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception
            {
                for (OffsetRange o : offsetRanges.get()) {
                    // 封装topic.partition 与 offset对应关系 java Map
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap();
                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

                    // 转换java map to scala immutable.map
                    scala.collection.mutable.Map<TopicAndPartition, Object> testMap = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                            testMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>()
                            {
                                public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1)
                                {
                                    return v1;
                                }
                            });

                    // 更新offset到kafkaCluster
                    kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
                }
            }
        });
    }

    public static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
        // transform java Map to scala immutable.map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> scalaKafkaParam =
                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>()
                {
                    public Tuple2<String, String> apply(Tuple2<String, String> v1)
                    {
                        return v1;
                    }
                });

        return new KafkaCluster(scalaKafkaParam);
    }

    public static void printConfig(Properties serverProps) {
        Iterator<Map.Entry<Object, Object>> it1 = serverProps.entrySet().iterator();
        while (it1.hasNext()) {
            Map.Entry<Object, Object> entry = it1.next();
            log.info(entry.getKey().toString()+"="+entry.getValue().toString());
        }
    }
}
