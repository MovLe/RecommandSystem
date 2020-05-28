import com.mongodb.casbah.commons.{MongoDBObject, MongoDBObjectBeanInfo}
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
 * @ClassName StreamingRecommender2
 * @MethodDesc: TODO StreamingRecommender2功能介绍
 * @Author Movle
 * @Date 5/27/20 9:39 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object StreamingRecommender2 {

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[3]",
      "kafka.topic"->"recom"
    )

    val sparkConf = new SparkConf().setAppName("StreamingReCommender").setMaster(config("spark.cores"))

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(3))

    val kafkaPara=Map(
      "bootstrap.servers" -> "192.168.31.141:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recomgroup"
    )

    val kafkaStream = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))

    val ratingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)

    }
    ratingStream.foreachRDD{
      rdd=>
        rdd.map{
          case(uid,mid,score,timestamp) =>
            println("get data from kafka")
        }.collect()
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
