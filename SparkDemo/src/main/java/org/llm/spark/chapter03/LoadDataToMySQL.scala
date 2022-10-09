package org.llm.spark.chapter03

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.InputStreamReader
import java.net.URL
import java.util.Properties

/**
 * @description: TODO 
 * @author ming
 * @email 2107873642@qq.com
 * @date 2022/10/9 9:48
 * @version 1.0
 */
object LoadDataToMySQL {

  val url = "jdbc:mysql://master:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8"
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "123456")

  def clearedData(): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read
      .option("header", "true")
      .option("nullValue", null)
      .csv("E:\\JavaCode\\BigData\\SparkDemo\\data\\mysql.csv")

    val columns = data.columns
    // 使用翻译后的列名重写原来的中文列
    val resDF = data.toDF(
      columns.map(
        col => translateByYouDao(col).toLowerCase.replace("the ", "").replace(" ", "_")
      ): _*
    )

    // 写入MySQL
    resDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "hotel_info", properties)

    spark.stop()
  }

  /**
   * 调用有道云API翻译
   *
   * @param searchValue 查询的值
   * @return 返回翻译后的值
   */
  def translateByYouDao(searchValue: String) = {
    // 阻塞当前线程100ms
    Thread.sleep(100L)
    import java.io.BufferedReader
    try {
      val url = new URL("https://fanyi.youdao.com/translate?&doctype=json&type=AUTO&i=" + searchValue)
      // 打开一个输入流
      val is = url.openStream()
      // 包装流
      val br = new BufferedReader(new InputStreamReader(is))
      // 解析第一行数据，返回翻译值
      val res = JSON.parseObject(
        br.readLine().trim
      ).getJSONArray("translateResult")
        .getJSONArray(0)
        .getJSONObject(0)
        .getString("tgt")
      // 关闭资源
      br.close()
      is.close()
      res
    } catch {
      case e: Exception =>
        e.printStackTrace()
        "翻译失败！！！"
    }
  }

  def main(args: Array[String]): Unit = {
    // clearedData()
    var n = 0
    while (true) {
      n += 1
      println(n + translateByYouDao("翻译失败"))
    }
  }
}
