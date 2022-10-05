package org.llm.spark.chapter02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import java.util.Properties

/**
 * @description: TODO 
 * @author ming
 * @email 2107873642@qq.com
 * @date 2022/9/30 21:16
 * @version 1.0
 */
object Test02 {

  System.setProperty("HADOOP_USER_NAME", "root")
  System.setProperty("hadoop.home.dir", "E:\\winutils-master\\hadoop-2.6.1")
  System.load("E:\\winutils-master\\hadoop-2.6.1\\bin\\hadoop.dll")

  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "123456")
  val url = "jdbc:mysql://master:3306/result?useSSL=false&useUnicode=true&characterEncoding=utf8"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.sql.shuffle.partitions", "8")
      .set("spark.sql.warehouse.dir", "E:\\JavaCode\\BigData\\SparkDemo\\")
      // .set("hive.metastore.uris", "thrift://master:9083")
      // .set("hive.warehouse.dir", "hdfs://master:9000/hive/user/warehouse")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf)
      // .enableHiveSupport()
      .getOrCreate()

    // spark.sql("show databases").show()
    // Thread.sleep(Integer.MAX_VALUE)
    val sc = spark.sparkContext
    // 导入隐式转换
    import spark.implicits._

    val dataDF = spark.read.option("header", "true").csv("hdfs://master:9000/hotelsparktask2")

    // 注册成临时表（查询和HQL大致一样）
    dataDF.createOrReplaceTempView("hotelsparktask2")

    val resultDF1 = dataDF.select("省份", "城市", "酒店", "房间数")
      .where(expr("if(`房间数` == 'NULL' or `房间数` is null, false, true)"))
      .orderBy("省份", "城市", "酒店", "房间数")
      .select(
        $"省份".alias("province"),
        $"城市".alias("city"),
        count("酒店").over(Window.partitionBy("省份", "城市")).alias("hotel_num"),
        sum($"房间数".cast(IntegerType)).over(Window.partitionBy("省份", "城市")).alias("room_num")
      ).distinct().orderBy(desc("room_num")).limit(10).cache()

    resultDF1
//      .show(10, false)

    // 假设写入MySQL中
    // resultDF1.write.mode(SaveMode.Overwrite).jdbc(url, "table3_1", properties)
    resultDF1.unpersist()

    /*
    * HQL实现(其实思路是一样的, RDD <=> DataFrame <=> Spark SQL(HQL), 可以随便转换)
    * 类型转换：
    * 低版本Hive只支持：CAST(value AS anytype)
    * 高版本Hive支持：anytype(value)
    * */
    spark.sql(
      """
        |SELECT distinct
        |    `省份` province,
        |    `城市` city,
        |    size(collect_set(`酒店`) over(partition by `省份`, `城市`)) hotel_num,
        |    cast(sum(`房间数`) over(partition by `省份`, `城市`) as int) room_num
        |FROM hotelsparktask2
        |WHERE `房间数` != 'NULL' and `房间数` is not null
        |ORDER BY room_num desc
        |LIMIT 10
        |""".stripMargin)
//      .show(10, false)

    val resultDF2 = dataDF.select(
      $"省份",
      $"城市",
      regexp_extract($"城市直销拒单率", "\\d+\\.\\d+", 0).cast(DoubleType).alias("城市直销拒单率")
    ).distinct()
      .groupBy($"省份".alias("province"))
      .agg(
        regexp_extract(sum($"城市直销拒单率"), "\\d+\\.\\d{0,6}", 0).alias("norate")
      ).orderBy($"norate".cast(DoubleType)).limit(10).cache()

    /*
    * Spark SQL 实现
    * */
    spark.sql(
      """
        |FROM
        |    (SELECT
        |        `省份` province,
        |        cast(regexp_extract(first_value(`城市直销拒单率`), "\\d+\\.\\d+", 0) as double) city_norate
        |    FROM hotelsparktask2
        |    GROUP BY `省份`, `城市`) t1
        |SELECT province, cast(regexp_extract(sum(city_norate), "\\d+\\.\\d{0,6}", 0) as double) norate
        |GROUP BY province
        |ORDER BY norate
        |LIMIT 10
        |""".stripMargin).show(10, false)


    resultDF2
      .show(10, false)

    // resultDF2.write.mode(SaveMode.Overwrite).jdbc(url, "table3_2", properties)

    resultDF2.unpersist()

    spark.stop()
  }
}