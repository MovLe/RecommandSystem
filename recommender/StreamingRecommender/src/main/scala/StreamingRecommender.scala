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
object ConnHelper {
  lazy val jedis = new Jedis("192.168.31.141")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.31.141:27017/recom"))
}

/**
 * Created by root on 2019/12/14
 *
 * 实时推荐引擎.
 *
 *
 */
object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20

  val MAX_SIM_MOVIES_NUM = 20

  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  val MONGODB_RATING_COLLECTION = "Rating"

  val MONGODB_STREAM_RECS_COLLECTION="StreamRecs"



  def main(args: Array[String]): Unit = {

    //声明spark环境
    val config = Map(
      "spark.cores" -> "local[3]",
      "kafka.topic" -> "recom",
      "mongo.uri"->"mongodb://192.168.31.141:27017/recom",
      "mongo.db"->"recom"
    )

    val sparkConf = new SparkConf().setAppName("StreamingRecommender")
      .setMaster(config("spark.cores")).setExecutorEnv("spark.executor.memory","2g")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(2))
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //制作共享变量
    val simMovieMatrix = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        bean =>
          (bean.mid,bean.recs.map(x => (x.mid,x.r)).toMap)
      }.collectAsMap()

    val simMoviesMatrixBroadCast = sc.broadcast(simMovieMatrix)

    val abc = sc.makeRDD(1 to 2)
    abc.map(x => simMoviesMatrixBroadCast.value.get(1)).count

    val kafkaPara = Map(
      "bootstrap.servers" -> "192.168.31.141:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recomgroup"
    )
    //连接Kafka
    //https://blog.csdn.net/Dax1n/article/details/61917718
    val kafkaStream = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))


    //接收评分流  UID | MID | Score | TIMESTAMP
    //kafka数据：1|2|5.0|1564412033
    val ratingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        println("get data from kafka --- ratingStream ")
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }

    ratingStream.foreachRDD{
      rdd =>
        rdd.map{
          case (uid,mid,score,timestamp) =>
            println("get data from kafka --- ratingStreamNext ")

            //实时计算逻辑实现
            //从redis中获取当前最近的M次评分
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)

            //获取电影P最相似的K个电影 共享变量
            val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadCast.value)

            //计算待选电影的推荐优先级

            val streamRecs = computMovieScores(simMoviesMatrixBroadCast.value,userRecentlyRatings,simMovies)


            //将数据保存到MongoDB中
            saveRecsToMongoDB(uid,streamRecs)

        }.count()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig)= {

    val streamRecsCollect = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollect.findAndRemove(MongoDBObject("uid"->uid))
    //(Int, Double)(Int, Double)(Int, Double)(Int, Double)(Int, Double)
    //Int:Double|Int:Double|Int:Double|Int:Double|Int:Double|Int:Double
    streamRecsCollect.insert(MongoDBObject("uid"->uid,"recs"->streamRecs.map(x => x._1 + ":" + x._2).mkString("|")))

    println("save to mongoDB")

  }




  /**
   * 计算待选电影的推荐分数
   *
   * @param simMovies          电影的相似度矩阵
   * @param userRecentlyRatings  用户最近k次评分
   * @param topSimMovies       当前电影最相近的k个电影
   */
  def computMovieScores(simMovies: collection.Map[Int, Map[Int, Double]],
                        userRecentlyRatings: Array[(Int, Double)],
                        topSimMovies: Array[Int]) = {

    //保存每一个待选电影和最近评分的每个电影的权重得分
    val score = ArrayBuffer[(Int,Double)]()

    //保存每一个电影的增强因子数
    val increMap = mutable.HashMap[Int,Int]()

    //保存每一个电影的减弱因子数
    val decreMap = mutable.HashMap[Int,Int]()

    //topSimMovies 对应图中 A（B）    userRecentlyRatings 对应图中 X Y Z
    for (topSimMovie <- topSimMovies ; userRecentlyRating <- userRecentlyRatings){

      val simScore = getMoviesSimScore(simMovies,userRecentlyRating._1,topSimMovie)

      if (simScore > 0.6){

        score += ((topSimMovie,simScore*userRecentlyRating._2))

        if (userRecentlyRating._2 > 3){
          //增强因子起作用
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        } else {
          //减弱因子起作用
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie,0) + 1
        }
      }
    }

    score.groupBy(_._1).map{
      case (mid,sims) =>
        (mid,
          sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray

  }

  def log(m:Int) = {
    math.log(m) / math.log(2)
  }
  /**
   *   获取电影之间的相似度
   * @param simMovies
   * @param userRatingMovie
   * @param topSimMovie
   */
  def getMoviesSimScore(simMovies: collection.Map[Int, Map[Int, Double]],
                        userRatingMovie: Int,
                        topSimMovie: Int) = {

    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }

  }


  /**
   * 获取电影之间的相似度
   * @param num
   * @param mid
   * @param uid
   * @param simMovies
   */
  def getTopSimMovies(num: Int,
                      mid: Int, uid: Int,
                      simMovies: collection.Map[Int, Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig)= {

    //从共享变量的电影相似度矩阵中，获取当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray

    //获取用户已经观看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid"->uid)).toArray.map{
      item =>
        item.get("mid").toString.toInt
    }

    //过滤掉已经评分过的电影 排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num)
      .map(x => x._1)

  }

  /**
   * 从redis中获取数据
   * @param num  评分的个数
   * @param uid  谁的评分
   * @param jedis  连接redis工具
   */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis):Array[(Int,Double)] = {

    //redis中保存的数据格式
    //lpush uid:1 1129:2.0 1172:4.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
    //需要注意 import scala.collection.JavaConversions._
    jedis.lrange("uid:" + uid.toString, 0, num).map {
      item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray

  }

  /**
   * 我们不能改变世界，但我们可以在有限的空间内改变自己。
   *
   * 在春种秋收之时，储备足够过冬的粮食，技能，和人脉，同时调整好自己的心态。
   *
   * 未来没有那么好，但其实也没有那么糟。答案总会有，只要你耐心寻找。
   */
}
