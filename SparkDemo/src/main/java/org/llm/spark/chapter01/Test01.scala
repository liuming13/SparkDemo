package org.llm.spark.chapter01

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf}

/**
 * @description: TODO 
 * @author ming
 * @date 2022/9/30 7:55
 * @version 1.0
 */
object Test01 {
  // 经测试这样运行时设置用户好像并不能修改访问HDFS时的用户, 但是以前也未遇到此种情况, 此次是通过配置环境变量来设置
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    // 设置本地Hadoop环境位置
    System.setProperty("hadoop.home.dir", "E:\\winutils-master\\hadoop-2.6.1")

    // 创建运行时环境
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
    val dataRDD = sc.textFile("file:///E:/JavaCode/BigData/SparkDemo/data/智联招聘_关键词_java_城市_北京.csv", 1).cache()
    val dataDF = spark.read
      .option("header", true)
      .option("nullValue", null)
      .csv("data\\智联招聘_关键词_java_城市_北京.csv").cache()

    /*
    * 删除数据源中缺失值大于3个字段的数据
    * 打印输出删除条目数
    * 结果输出至data / out / out111目录
    * */
    // 拿到缺失值<=3的所有数据
    val dropNaGt3 = dataRDD.map(_.split(",").toList)
      .filter(_.count(_ == "") <= 3).cache()
    // 计算缺失值大于3的数据条数
    val diffNum1 = dataRDD.count() - dropNaGt3.count()
    dropNaGt3.unpersist()
    println("删除的条数为: " + diffNum1)
    // 将存储数据到hdfs
    // dropNaGt3.map(_.mkString(",")).saveAsTextFile("hdfs://master:9000/data/out/out111")
    // 释放缓存
    dropNaGt3.unpersist()


    /*
    * 运行代码，将字段{公司地点,学历要求,公司福利}中任意字段为空的数据删除，并打印输出删除条目数。
    * 输出的结果至data/out/out112目录。
    *
    * If how is "any", then drop rows containing any null or NaN values in the specified columns. If how is "all", then
    * drop rows only if every specified column is null or NaN for that row.
    * 翻译: 如果how为“any”，则删除指定列中包含任何null或NaN值的行。如果how为“all”，则仅当该行的每个指定列都为null或NaN时才删除行。
    * */

    /*
    * 一般情况下我们发现这样操作并不可以, 通过debug跟踪源码发现, spark在读取csv文件时, 默认的空值是空串
    * org.apache.spark.sql.execution.datasources.csv.CSVOptions
    *   ......
    *   val nullValue = parameters.getOrElse("nullValue", "")
    *   val nanValue = parameters.getOrElse("nanValue", "NaN")
    *   ......
    *
    * 因此我们需要在读取的时候调整参数, 将空值设置为null
    * */
    val dropAnyDF = dataDF.na.drop("any", Seq("公司地点", "学历要求", "公司福利")).cache()
    // 写出
    dropAnyDF.repartition(1)
      .write.mode(SaveMode.Overwrite)
      .option("sep", ",")
      .option("header", "true")
      .csv("hdfs://master:9000/data/out/out112")

    val diffNum2 = dataDF.count() - dropAnyDF.count()
    println("删除的条目数为: " + diffNum2)
    // 释放缓存
    dropAnyDF.unpersist()

    /*
    * 二、数据分析
    *  1.步骤一(Lx121.scala)
    *   1)运行代码，统计各城市的酒店数量和房间数量，以城市房间数量降序排列，并打印输出前10条统计结果。
    *   2）结果保存在data/out/out121
    *  2.步骤二(Lx122.scala)
    *   1) 统计各省拒单率，将统计的拒单率升序排列并将前10条统计结果
    *   2) 将结果输出至data/out/out122目录
    *
    * 这玩意要的就是分组聚合, 因此我们根据实际数据, 以学历为维度进行数据分析
    * */
    // 先做预处理, 如果学历要求列为null, 那么我们统一填充学历不限
    dataDF
      .na.fill("学历不限", Seq("学历要求"))
      .groupBy("学历要求")
      .agg(
        count(lit(1)).alias("count") // 也不知道查啥了, 就记个数吧, 取前3
      ).orderBy(desc("count")).limit(3).show(30, false)
    // 随便爬本科都要这么多...


    /*
    * 最后来根据此数据情况来计算一下, 学历之间的平均工资, 就不使用函数了, 演示一下UDF和UDAF
    * 根据分析我们发现智联招聘上的薪资还是非常有格式的, 不像51job(一会以万为单位一会以千为单位, 动不动15薪), 因此非常好分析
    * */

    // 定义UDF函数, 取出每个岗位的中值月薪
    // 一个简单的截取数字的正则表达式
    val nullPrice = -1.0  // 用它来描述薪资的空值
    val pattern = "\\d+".r
    val midValue = spark.udf.register("getMidValue", (info: String) => {
      if (info == null) {
        nullPrice
      } else {
        val temp = pattern.findAllIn(info).toList
        temp.map(_.toDouble).sum * 1000 / temp.length
      }
    })

    // 向Spark注册UDAF函数
    val myAvg = spark.udf.register("myAvg", new MyAvg)
    // 将薪资为空的列填充为平均薪资
    // 计算平均薪资(先去除空值)
    val avgPrice = dataDF.select("薪资").na.drop()
      .select(myAvg(midValue($"薪资")))
      // 取出第一行数据
      .first()
      .getDouble(0)

    println(avgPrice)

    // 填充空值
    dataDF.select(expr("if(`学历要求` is null, '学历不限', `学历要求`)").alias("学历要求"), midValue($"薪资").alias("mid薪资"))
      .withColumn("mid薪资", when($"mid薪资" === nullPrice, avgPrice).otherwise($"mid薪资"))
      .groupBy("学历要求")
      .agg(
        // 此处调用函数保留两位小数, 就不自己实现了
        regexp_extract(myAvg($"mid薪资"), "\\d+\\.\\d{0,2}", 0).alias("avg薪资")
      )
      .show(50)

    // 以上所有逻辑都可以使用Spark SQL(HQL)来做实现 -> spark.sql("")
    spark.stop()
  }

  // 定义平均值函数(此函数需要实现一个抽象类)
  class MyAvg extends UserDefinedAggregateFunction {
    // 输入的数据格式
    override def inputSchema: StructType = StructType(StructField("in", DoubleType) :: Nil)

    // buffer用来存储中间数据
    override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("num", LongType) :: Nil)

    // 最终返回的数据类型
    override def dataType: DataType = DoubleType

    // 确保一致性 一般设置为true
    override def deterministic: Boolean = true

    // 初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0.0
      buffer(1) = 0L
    }

    // 更新中间结果
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1L
    }

    // 合并中间结果
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算最终结果
    override def evaluate(buffer: Row) = {
      // avg = sum / num
      buffer.getDouble(0) / buffer.getLong(1)
    }
  }
}
