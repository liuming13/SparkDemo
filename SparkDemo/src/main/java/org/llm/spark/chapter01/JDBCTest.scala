package org.llm.spark.chapter01

import java.sql.DriverManager
import java.util.Properties

/**
 * @description: TODO 
 * @author ming
 * @email 2107873642@qq.com
 * @date 2022/9/30 12:36
 * @version 1.0
 */

object JDBCTest {
  // 1. mysql 地址
  val url = "jdbc:mysql://master:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8"
  // 2. 连接配置
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "123456")
  // 3. 获取连接
  val conn = DriverManager.getConnection(url, properties)

  def main(args: Array[String]): Unit = {
    select()

    conn.close()
  }

  def select() = {
    val sql = "SELECT * FROM person"
    // 4. 获取Statement对象
    val stat = conn.createStatement()

    // 5. 如果多次执行相同的sql语句, 可以使用prepareStatement对象, 会快一点
    // conn.prepareStatement("")
    val rs = stat.executeQuery(sql)
    while (rs.next()) {
      println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4))
    }
  }

  def update(): Unit = {
    val sql = "SELECT  "
  }
}
