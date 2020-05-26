package com.movle.test

case class Movie(val mid:Int,val name:String,val descri:String,
                 val timelong:String,val issue:String,val shoot:String,
                 val language:String,val genres:String,
                 val actors:String,val directors:String)

/**
 * 1,31,2.5,1260759144
    用户对电影的评分数据集
 */
case class Rating(val uid:Int,val mid:Int,val score:Double,val timestamp:Int)

/**
 * 15,7478,Cambodia,1170560997
 *
 * 用户对电影的标签数据集
 */
case class Tag(val uid:Int,val mid:Int,val tag:String,val timestamp:Int)

/**
 * mongoDB 配置对象
 */
case class MongoConfig(val uri:String,val db:String)

/**
 * ES配置对象
 */
case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)
