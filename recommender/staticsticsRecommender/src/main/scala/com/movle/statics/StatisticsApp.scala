package com.movle.statics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName StaticsticsApp
 * @MethodDesc: 离线统计的main方法
 * @Author Movle
 * @Date 5/26/20 3:16 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 *
 * 离线统计的main方法
 *
 * 数据流程：
 * spark读取mongoDB中的数据，离线统计后，将统计结果写入mongodb
 *
 *
 * 统计值：
 *
 * 1、评分最多电影
 *
 * 获取所有历史数据中，评分个数最多的电影集合，统计每个电影评分个数  -->   RateMoreMovies
 *
 * 2、近期热门电影
 *
 * 按照月来统计，这个月中评分最多的电影我们认为是热门电影，统计每个月中每个电影的评分总量  -->  RateMoreRecentlyMovie
 *
 * 3、电影的平均得分
 *
 * 把每个电影，所有用户评分进行平均，计算出每个电影的平均得分 --> AverageMovies
 *
 * 4、统计出每种类别电影的Top10
 *
 * 将每种类别的电影中，评分最高的10个电影计算出来  -->  GenresTopMovies
 **/
object StatisticsApp extends App {

  val RATING_COLLECTION_NAME = "Rating"
  val MOVIE_COLLECTION_NAME="Movie"

  val params = scala.collection.mutable.Map[String,Any]()

  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://192.168.31.141:27017/recom"
  params += "mongo.db" -> "recom"


  val conf = new SparkConf().setAppName("StatisticsApp")
    .setMaster(params("spark.cores").asInstanceOf[String])

  val spark = SparkSession.builder().config(conf).getOrCreate()

  implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
    params("mongo.db").asInstanceOf[String])
  //读取mongodb中的数据
  import spark.implicits._
  val ratings = spark.read
    .option("uri",mongoConfig.uri)
    .option("collection",RATING_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache

  val movies = spark.read
    .option("uri",mongoConfig.uri)
    .option("collection",MOVIE_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache

  ratings.createOrReplaceTempView("ratings")

  //统计 评分最多电影
  staticsRecommender.rateMore(spark)

  //近期热门电影
//  staticsRecommender.rateMoreRecently(spark)

//  staticsRecommender.genresTop10(spark)(movies)

  ratings.unpersist()
  movies.unpersist()

  spark.close()
}
