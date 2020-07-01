package stream.dataprocess.launch

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import stream.common.util.jedis.PropertiesUtil

/**
  * @Author liaojincheng
  * @Date 2020/6/30 21:05
  * @Version 1.0
  * @Description
  * 运行数据处理的主类
  */
object DataProcessLauncher {

  /**
    * 业务处理
    * @param spark
    * @param kafkaParams
    * @param topics
    */
  def setupSsc(spark: SparkSession, kafkaParams: Map[String, Object], topics: Set[String]) = {
    //创建Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(3))
    val inputStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      //本地化策略
      //一般都是这样写,他会将分区数据尽可能的均匀分布给可用的Executor
      LocationStrategies.PreferConsistent,
      //消费者策略
      //Subscribe:不可以动态的更改消费的分区,一般都使用在开始读取数据的时候
      //Assign:他可以消费固定的topic的partition(集合)
      //SubscribePartition:可以用在消费过程中增加分区
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    //打印
    inputStream.map(_.value()).foreachRDD(rdd => rdd.foreach(println))
    ssc
  }

  def main(args: Array[String]): Unit = {
    //当应用被停止的时候,进行如下设置可以保证当前执行批次执行完成之后再停止应用
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //初始化上下文
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("streaming-dataProcess")
      .getOrCreate()
    //设置消费者组
    val groupId = "test"
    //配置kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"),
      //反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      //从头消费
      "auto.offset.reset" -> "earliest"
    )
    //Topic
    val topics = Set(PropertiesUtil.getStringByKey("source.nginx.topic", "kafkaConfig.properties"))
    //创建流
    val ssc = setupSsc(spark, kafkaParams, topics)
    //启动关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
