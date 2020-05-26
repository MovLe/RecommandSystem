package com.movle.test

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
/**
 * @ClassName DataLoader
 * @MethodDesc: 将Data倒入ES和Mongdb
 * @Author Movle
 * @Date 5/26/20 10:24 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object DataLoader {

  //MongoDB 中的表 Collection
  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"

  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  //ES TYPE 名称
  val ES_TAG_TYPE_NAME = "Movie"

  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r


  def main(args: Array[String]): Unit = {

    val DATAFILE_MOVIES = "/Users/macbook/TestInfo/reco_data/movies.csv"

    val DATAFILE_RATINGS = "/Users/macbook/TestInfo/reco_data/ratings.csv"

    val DATAFILE_TAGS = "/Users/macbook/TestInfo/reco_data/tags.csv"

    val params = scala.collection.mutable.Map[String,Any]()
    params += "spark.cores" -> "local"
    params += "spark.name" -> "DataLoader"
    params += "mongo.uri" -> "mongodb://192.168.31.141:27017/recom"
    params += "mongo.db" -> "recom"
    params += "es.httpHosts" -> "192.168.31.141:9200"
    params += "es.transportHosts" -> "192.168.31.141:9300"
    params += "es.index" -> "recom"
    params += "es.cluster.name" -> "my-application"

    //定义MongoDB配置对象(隐式参数)
    implicit val mongoConf = new MongoConfig(params("mongo.uri").asInstanceOf[String],
      params("mongo.db").asInstanceOf[String])

    //定义ES配置对象（隐式参数）
    implicit val esConf = new ESConfig(params("es.httpHosts").asInstanceOf[String],
      params("es.transportHosts").asInstanceOf[String],
      params("es.index").asInstanceOf[String],
      params("es.cluster.name").asInstanceOf[String])

    //声明spark环境
    val config = new SparkConf().setAppName(params("spark.name").asInstanceOf[String])
      .setMaster(params("spark.cores").asInstanceOf[String])

    val  spark = SparkSession.builder().config(config).getOrCreate()

    //加载数据集
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)

    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)

    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)

    //将RDD转换成DataFrame
    import spark.implicits._

    val movieDF = movieRDD.map(
      line => {
        val x = line.split("\\^")
        Movie(x(0).trim.toInt,x(1).trim,x(2).trim
          ,x(3).trim,x(4).trim,x(5).trim
          ,x(6).trim,x(7).trim,x(8).trim,x(9).trim)
      }
    ).toDF()

    val ratingDF = ratingRDD.map(
      line => {
        val x = line.split(",")
        Rating(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toDouble,x(3).trim.toInt)
      }
    ).toDF()

    val tagDF = tagRDD.map(
      line => {
        val x = line.split(",")
        Tag(x(0).trim.toInt,x(1).trim.toInt,x(2).trim,x(3).trim.toInt)
      }
    ).toDF()

    //将数据写入到MongoDB
    storeDataInMongo(movieDF,ratingDF,tagDF)

//    //引入内置函数库
//    import org.apache.spark.sql.functions._
//    //将数据保存到ES中
//    movieDF.cache()
//    tagDF.cache()
//
//    val tagCollectDF = tagDF.groupBy("mid").agg(concat_ws("|",collect_set("tag")).as("tags"))
//
//    val esMovieDF = movieDF.join(tagCollectDF,Seq("mid","mid"),"left")
//      .select("mid","name","descri","timelong","issue","shoot","language","genres","actors","directors","tags")
//
//    //    esMovieDF.show(30)
//
//    storeDataInES(esMovieDF)
//
//    movieDF.unpersist()
//    tagDF.unpersist()

    spark.close()

  }

  def storeDataInES(esMovieDF: DataFrame)(implicit esConf:ESConfig) : Unit = {

    //要操作Index的名称
    val indexName = esConf.index

    //连接ES的配置
    val settings = Settings.builder().put("cluster.name",esConf.clusterName).build()

    //连接ES的客户端
    val esClient = new PreBuiltTransportClient(settings)

    // ESConfig 对象中 transportHosts 属性保存所有es节点信息
    //以下方法要将所有节点遍历出来
    //"192.168.109.141:9300,192.168.109.142:9300,192.168.109.143:9300"
    esConf.transportHosts.split(",")
      .foreach{
        //192.168.109.141:9300
        case ES_HOST_PORT_REGEX(host:String,port:String) =>
          esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }

    //判断如果Index存在，则删除
    if (esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists){
      //存在此index 则删除
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }

    //创建Index
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()

    val movieOption = Map("es.nodes" -> esConf.httpHosts,
      "es.http.timeout"->"100m",
      "es.mapping.id" -> "mid")

    val movieTypeName = s"$indexName/$ES_TAG_TYPE_NAME"

    esMovieDF.write
      .options(movieOption)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(movieTypeName)


  }


  def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig:MongoConfig): Unit = {

    //创建到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //删除表
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()

    //写入mongodb
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //创建mongodb索引
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid"->1))

    mongoClient.close()
  }
}
