package com.movle.offlineRecommender

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
/**
 * @ClassName OfflineRecommender
 * @MethodDesc: 离线推荐算法
 * @Author Movle
 * @Date 5/26/20 5:55 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object OfflineRecommender {

  val MONGODB_RATING_COLLECTION="Rating"
  val MONGODB_MOVIE_COLLECTION="Movie"
  val USER_MAX_RECOMMENDATION=10
  val MONGODB_USER_RECS="UserRecs"

  val MONGO_MOVIE_RECS="MovieRecs"

  def main(args: Array[String]): Unit = {

    val conf = Map(
      "spark.core" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.31.141:27017/recom",
      "mongo.db" -> "recom"
    )

    val sparkConf = new SparkConf().setAppName("OfflineRecommender")
      .setMaster(conf("spark.core"))
      .set("spark.executor.memory","6G")
      .set("spark.driver.memory","2G")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //获取mongodb中的数据
    val mongoConfig = MongoConfig(conf("mongo.uri"),conf("mongo.db"))
    import spark.implicits._
    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid,rating.mid,rating.score)).cache

    val movieRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache

    //训练ALS模型
    /**
     * ALS模型需要4个参数
     *
     * trainData
     * 训练数据
     * Rating对象的RDD，包含：用户ID，物品ID，偏好值
     *
     * rank
     * 特征维度：50
     *
     * iterations
     * 迭代次数：5
     *
     * lambda：0.01
     * 步长
     */


    //构建训练数据集合
    val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))

    //    val rank = 50
    //    val iterations = 5
    //    val lambda = 0.01

    val (rank,iterations,lambda) = (50,5,0.01)

    //模型
    //rank指的是矩阵分解Matrix Factorization 时，将原本A(m x n)矩阵分解为，矩阵X(m x rank)与矩阵Y(rank x n)
    //iterations 为ALS算法重复计算次数
    //Lambda为建议值
    val model = ALS.train(trainData,rank,iterations,lambda)

    //计算离线用户推荐电影矩阵

    val userRDD = ratingRDD.map(_._1).distinct().cache()

    val userMovies = userRDD.cartesian(movieRDD)

    //预测结果
    val preRatings = model.predict(userMovies)


    //userRecs用户矩阵
    val userRecs = preRatings
      .filter(_.rating > 0)
      .map(rating123 => (rating123.user,(rating123.product,rating123.rating)))
      .groupByKey()
      .map{
        case (uid,recs) =>
          UserRecs(uid,recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
            .map(x => Recommendation(x._1,x._2)))
      }.toDF

    //把预测结果写回mongodb
//    userRecs.write
//      .option("uri",mongoConfig.uri)
//      .option("collection",MONGODB_USER_RECS)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()

    //计算离线电影相似度矩阵

    //获取电影的特征矩阵
    val movieFeatures = model.productFeatures.map{
      case (mid,features) => (mid,new DoubleMatrix(features))
    }

    //cartesian笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)// RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))]
      .filter{
        case (a,b) => a._1 != b._1 //过滤掉自己与自己的笛卡尔积
      }.map{
      case (a,b) =>
        val simScore = this.consinSim(a._2,b._2)//电影相似性评分
        (a._1,(b._1,simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()//(Int, Iterable[(Int, Double)])
      .map{
        case (mid,items) =>
          MovieRecs(mid,items.toList.map(x=> Recommendation(x._1,x._2)))
      }.toDF

    movieRecs
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGO_MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.close()
  }

  //计算两个电影间的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
