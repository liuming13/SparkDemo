package org.llm.spark.chapter01

import java.sql.DriverManager
import java.util.Properties

/**
 * @description: TODO 
 * @author ming
 * @date 2022/9/30 11:03
 * @version 1.0
 */
object Test02 {

//  create table person(pid int auto_increment, pname varchar(20), sex char(1), email varchar(30), PRIMARY KEY (pid) ) charset=utf8;
  def main(args: Array[String]): Unit = {
    val pattern = "\\d+".r
    val iterator = pattern.findAllIn("23k-89k")
    println(iterator.toList)

    //
    val url = "jdbc:mysql://master:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")

    val conn =  DriverManager.getConnection(url, prop)

    val stmt = conn.createStatement()
    val sql = "SELECT * FROM student"
    val resultSet = stmt.executeQuery(sql)

    while (resultSet.next()) {
      val sid = resultSet.getInt(1)
      val sName = resultSet.getString(2)
      println(sid + " => " + sName)
    }

    conn.close()
  }
}
