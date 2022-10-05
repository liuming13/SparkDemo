package org.llm.spark.chapter02

import org.apache.spark.sql.SparkSession
import org.dom4j.{DocumentHelper, Element}

import java.util.function.Consumer
import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
 * @description: TODO 
 * @author ming
 * @email 2107873642@qq.com
 * @date 2022/10/5 15:30
 * @version 1.0
 */
object ParseXMLAndJson {

  def main(args: Array[String]): Unit = {
    // parseXML()
    // parseJson()
    useSparkParseJson()
  }

  def parseXML() = {

    val xmlStr =
      """
        |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        |<configuration>
        |    <property>
        |        <name>javax.jdo.option.ConnectionURL</name>
        |        <value>jdbc:mysql://master:3306/metastore?useSSL=false</value>
        |    </property>
        |</configuration>
        |""".stripMargin

    // 使用scala原生XML解析
    val xmlObj = XML.loadString(xmlStr)
    // 遍历所有的一级节点得到所有的property
    val propertySeq = xmlObj \ "property"
    val map = propertySeq.map(node => {
      // \\是得到所有级别下的; \是一级
      ((node \ "name") (0).text, (node \\ "value") (0).text)
    }).toMap
    println(map)

    // dom4j
    val resMap = scala.collection.mutable.Map[String, String]()
    DocumentHelper.parseText(xmlStr).getRootElement.elements().asInstanceOf[java.util.List[Element]].forEach(new Consumer[Element] {
      override def accept(element: Element): Unit = {
        resMap.put(element.element("name").getText, element.element("value").getStringValue)
      }
    })

    // 不习惯使用这种函数式编程也可以拿到其底层的迭代器, 用法也是一样的
    val elements = DocumentHelper.parseText(xmlStr).getRootElement.elements().iterator()
    while (elements.hasNext) {
      val element = elements.next().asInstanceOf[Element]
      resMap.put(element.element("name").getText, element.element("value").getStringValue)
    }
    println(resMap)
  }


  def parseJson() = {
    val jsonStr =
      """
        |{"username":"Ricky", "attribute":{"age":21, "weight": 60}}
        |""".stripMargin

    //    val jsonValue = JSON.parseFull(jsonStr)
    //    val jsonObj = jsonValue match {
    //      case Some(map: Map[String, Any]) => map.asInstanceOf[Map[String, Map[String, String]]]
    //    }
    //    val attrMap = jsonObj.get("attribute").get
    //    println(jsonObj.get("username").get, attrMap.get("age").get, attrMap.get("weight").get)

    val jsonObj = com.alibaba.fastjson.JSON.parseObject(jsonStr)
    val username = jsonObj.getString("username")
    val attrObj = jsonObj.getJSONObject("attribute")
    val age = attrObj.getIntValue("age")
    val weight = attrObj.getIntValue("weight")
    println(username, age, weight)
  }

  def useSparkParseJson() = {
    // 创建Spark会话
    val spark = SparkSession.builder().appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // 创建一个json列用于模拟真实数据
    val jsonDF = spark.range(1).selectExpr(
      """
        |'{"username":"Ricky", "attribute":{"age":21, "weight": 60, "hobbies":["sing", "jump", "rap"]}}' as jsonStr
        |""".stripMargin)

    // 关键函数 -> org.apache.spark.sql.functions.get_json_object()
    // DataFrame的实现
    // 这个spark版本太低了, 因此想要还原json的数组还需要自己实现, 高版本里面可以直接使用from_json和to_json的
    val parseJsonArr = spark.udf.register("parseJsonArr", (jsonArrStr: String) => {
      // 这个函数就用于解析全部元素都是字符的情况
      val r = "\"(.+)\"".r
      // 这里水平不够, 只能用这种笨办法了
      jsonArrStr.split(",").map(r.findFirstIn(_).get).map(x => x.substring(1, x.length - 1))
    })

    val resultDF = jsonDF.select(
      get_json_object($"jsonStr", "$.username").alias("username"),
      get_json_object($"jsonStr", "$.attribute.age").alias("age"),
      parseJsonArr(get_json_object($"jsonStr", "$.attribute.hobbies")).alias("hobbies")
    )

    resultDF.show(false)
    resultDF.printSchema()

    // 很遗憾, 这个Hive版本也过低, 要解析JSON也比较麻烦, 有时间再补充Hive自定义函数,
    // 其实json结构比较简单的话, 可以直接利用regexp_extract()函数来使用正则解析
    // 关闭连接
    spark.stop()
  }
}
