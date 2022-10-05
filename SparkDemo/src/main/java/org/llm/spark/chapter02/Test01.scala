package org.llm.spark.chapter02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @description: TODO 
 * @author ming
 * @email 2107873642@qq.com
 * @date 2022/9/30 20:26
 * @version 1.0
 */
object Test01 {

  System.setProperty("HADOOP_USER_NAME", "root")
  System.setProperty("hadoop.home.dir", "E:\\winutils-master\\hadoop-2.6.1")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.sql.shuffle.partitions", "8")
      .set("spark.sql.warehouse.dir", "E:\\JavaCode\\BigData\\SparkDemo\\")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext
    // 导入隐式转换
    import spark.implicits._

    // 加载数据
    val dataRDD = sc.textFile("file:///E:/JavaCode/BigData/SparkDemo/data/mysql.csv", 1).cache()

    /**
     * 按照题目要求剔除缺失数据信息（n=3），并以打印语句输出删除条数
     * 程序打包并在hadoop平台运行，结果输出至 hdfs文件系统中//master:9000/hotelsparktask1
     */
    // "NULL"是否是缺失值呢？ 我们发现其实是有真正的缺失值的, 但是不处理也不合理
    val dropNullGt3 = dataRDD.map(_.split(",").toList)
      .filter(_.count(attr => attr == "" || attr == "NULL") <= 3)
      .repartition(1)
      .map(_.mkString(",")).cache()
    // 写出
    // dropNullGt3.saveAsTextFile("hdfs://master:9000/hotelsparktask1")
    println("第一题剔除的条数为: " + (dataRDD.count() - dropNullGt3.count()))
    dropNullGt3.unpersist()

    /**
     * 运行代码，将字段{星级、评论数、评分}中任意字段为空的数据删除，并打印输出删除条目数。
     * 使用第一部输出的数据吧
     */
    val dropAnyColDF = spark.read.option("header", "true").option("nullValue", null)
      .csv("hdfs://master:9000/hotelsparktask1")
      .withColumn("星级", expr("if (`星级` == 'NULL', null, `星级`)"))
      .withColumn("评论数", expr("if (`评论数` == 'NULL', null, `评论数`)"))
      .withColumn("评分", expr("if (`评分` == 'NULL', null, `评分`)"))
      .na.drop("any", Seq("星级", "评论数", "评分"))
      .cache()
    dropAnyColDF.repartition(1)
      .write.option("header", true)
      .mode(SaveMode.Overwrite)
      .csv("hdfs://master:9000/hotelsparktask2")
    println("第二题剔除的条数为: " + (dropNullGt3.count() - dropAnyColDF.count()))
    dropAnyColDF.unpersist()

    spark.stop()
  }
}
