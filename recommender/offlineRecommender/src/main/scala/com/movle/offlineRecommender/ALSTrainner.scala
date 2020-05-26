package com.movle.offlineRecommender

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
 * @ClassName ALSTrainner
 * @MethodDesc:  找到ALS算法中 最优参数
 * @Author Movle
 * @Date 5/26/20 5:54 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 *
 * val (rank,iterations,lambda) = (50,5,0.01)
 *
 * 原理：遍历所有业务范围内的取值情况，找到最优模型
 *
 * 模型评价：预测值和实际值误差最小
 **/
object ALSTrainner {

  def main(args: Array[String]): Unit = {
    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.31.141:27017/recom",
      "mongo.db" -> "recom"
    )

    val sparkConf = new SparkConf().setAppName("ALSTrainner").setMaster(conf("spark.cores"))

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //加载评分数据
    val mongoConfig = MongoConfig(conf("mongo.uri"),conf("mongo.db"))
    import spark.implicits._
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score)).cache()

    //输出最优参数
    adjuctALSParams(ratingRDD)

    spark.close()
  }


  //输出最优参数
  def adjuctALSParams(ratingRDD: RDD[Rating]) = {
    val result = for (rank <- Array(30,40,50,60,70);lambda<- Array(1,0.1,0.01))
      yield {
        val model = ALS.train(ratingRDD,rank,5,lambda)
        //获取模型误差
        val rmse = getRmse(model,ratingRDD)
        (rank,lambda,rmse)
      }

    print(result.sortBy(_._3).head)


  }

  def getRmse(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]) = {
    //需要构造userProductsRDD，通过真实值，和预测值来测误差
    val userMovies = ratingRDD.map(item => (item.user,item.product))
    val predictRating = model.predict(userMovies)

    val real = ratingRDD.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    //计算误差
    sqrt(
      real.join(predict)//(int,int),(double ,double)
        .map{
          case ((uid,mid),(real,pre)) =>
            val err = real - pre
            err * err
        }.mean()
    )
  }
}
