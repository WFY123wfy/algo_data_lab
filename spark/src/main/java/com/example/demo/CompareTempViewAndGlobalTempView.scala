package com.example.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CompareTempViewAndGlobalTempView {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val dataPath = "./spark/ml-1m/"
    // ID、性别（F、M分别表示女性、男性）、年龄（使用7个年龄段标记）、职业和邮编
    val usersRDD = sc.textFile(dataPath + "users.dat")
    // 电影ID、电影名称和电影类型
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    // 用户ID、电影ID、评分（满分是5分）和时间戳
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    println("通过DataFrame实现某部电影观看者中男性和女性不同年龄人数：")

    // Users数据格式化：在RDD的基础上增加数据的元数据信息
    val schemaForUsers = StructType(
      "UserID::Gender::Age::OccupationID::zip-code"
        .split("::")
        .map(column => StructField(column, StringType, true))
    )
    // 把每一条数据转换成以Row为单位的数据(1::F::1::10::48067)
    val usersRDDRows = usersRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    // 基于RDD创建DataFrame, 此时RDD有了元数据信息的描述
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    // ============= temp view: 生命周期为当前session ===========
    //    usersDataFrame.createTempView("users")
//    val sql_local = "SELECT * from users limit 5"
    // ============= global temp view: 生命周期为当前driver, session间可以共享, table前需要加上global_temp ===========
    usersDataFrame.createGlobalTempView("users")
    // 写SQL语句并执行
    val sql_local = "SELECT * from global_temp.users limit 5"
    spark.sql(sql_local).show()
//    spark.stop() 不能提前停止，会报No active SparkContext

    val spark1 = spark.newSession()
    val sc1 = spark1.sparkContext
    sc1.setLogLevel("warn")
    spark1.sql(sql_local).show()

    spark.stop()
    spark1.stop()
  }
}