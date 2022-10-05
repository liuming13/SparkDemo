package org.llm.spark.chapter02

import org.apache.hadoop.hbase.{CellUtil, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Delete, Get, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Properties
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer

/**
 * @description: TODO 
 * @author ming
 * @email 2107873642@qq.com
 * @date 2022/10/5 7:48
 * @version 1.0
 */
object ConnectTest {

  /*
  public static Configuration addHbaseResources(Configuration conf) {
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    checkDefaultsVersion(conf);
    HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(conf);
    return conf;
  }
  在其源码中会默认通过hbase-default.xml和hbase-site.xml来加载配置项，因此我们把文件copy过来就行了

  当然也可以自己配置，主要配置项是hbase.zookeeper.quorum
  */
  val HBASE_CONN = ConnectionFactory.createConnection()

  def main(args: Array[String]): Unit = {
    // testMySQLConn()
    testHBaseConn()
  }

  def testMySQLConn() = {
    // 连接url
    val url = "jdbc:mysql://master:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8"
    // 连接配置
    val info = new Properties()
    info.setProperty("user", "root")
    info.setProperty("password", "123456")

    // 一. 原生JDBC
    // 1. 获取连接对象
    val conn = DriverManager.getConnection(url, info)
    /*
    * 创建用于向数据库发送SQL语句的Statement对象。没有参数的SQL语句通常使用Statement对象执行。
    * 如果多次执行同一SQL语句，则使用PreparedStatement对象可能会更有效。
    * */
    val stm = conn.createStatement()
    // conn.prepareStatement("")
    /*
    * mysql> select * from person;
    +-----+--------+------+----------------+
    | pid | pname  | sex  | email          |
    +-----+--------+------+----------------+
    |   1 | lili   | 男   | 21078@qq.com   |
    |   2 | wangwu | 男   | 234234@qq.com  |
    |   3 | maria  | 女   | d621381@qq.com |
    +-----+--------+------+----------------+
    3 rows in set (0.00 sec)
    * */
    // query
    val rs = stm.executeQuery("SELECT * FROM person")
    val listBuffer = new ListBuffer[(Int, String, String, String)]
    while (rs.next()) {
      val pid = rs.getInt(1)
      val pName = rs.getString(2)
      val sex = rs.getString(3)
      val email = rs.getString(4)
      // 此处用这种写法需要两个括号
      listBuffer += ((pid, pName, sex, email))
    }
    println(listBuffer.mkString("\n"))

    // update
    val updateRes = stm.executeUpdate("UPDATE person SET email = '123456@gmail.com' WHERE pid = 1")
    val insertRes = stm.executeUpdate("INSERT INTO person(pname, sex, email) VALUES ('stu1', '男', 'stu1111@qq.com')")
    val deleteRes = stm.executeUpdate("DELETE FROM person WHERE pid > 3")
    // 防止未提及, 可以主动提交, 当然在连接关闭前也会提交所有
    // conn.commit()
    // 关闭连接
    conn.close()
    // 拓展: 多了解conn.prepareStatement(""), 此方式可以预编译SQL语句, 执行时只需要填充占位符即可, 可以有效避免一部分SQL注入, 但是此处能用就行...

    // 二. Spark连接MySQL
    // 创建Spark会话
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    // 1. 使用Spark已经封装好的API
    // 此处的table可以直接写表名，也可以直接写一段查询，写查询必须给查询取别名
    val table = "person"
    spark.read.jdbc(url, table, info).show(10, false)

    // 2. 自己设置连接
    spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", url)
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", table)
      .load().show(10, false)

    spark.stop()
  }

  def testHBaseConn() = {
    // 因为HBase的连接是重量级的，因此我们只在前面维护一个
    // DDL -> admin
    // 创建命名空间
    def createNamespace(namespace: String) = {
      val admin = HBASE_CONN.getAdmin
      val descriptor = NamespaceDescriptor.create(namespace).build()
      admin.createNamespace(descriptor)
      // 关闭连接
      admin.close()
    }

    // 判断表格是否存在
    def isTableExists(namespace: String, tableName: String): Boolean = {
      val admin = HBASE_CONN.getAdmin
      val res = admin.tableExists(TableName.valueOf(namespace, tableName))
      admin.close()
      res
    }
    // ----- DDL其实不是重点，因此就先写两个

    // DML
    // put
    def putCell(namespace: String, tableName: String,
                rowKey: String, columnFamily: String, columnName: String, value: String) = {
      val table = HBASE_CONN.getTable(TableName.valueOf(namespace, tableName))
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))
      table.put(put)
      table.close()
    }
    // putCell("test", "person", "3333", "info", "name", "maria")
    /*
    hbase(main):007:0> scan 'test:person'
    ROW                                            COLUMN+CELL
     2003                                          column=info:name, timestamp=1664488351966, value=zhang
     2005                                          column=info:name, timestamp=1664488351968, value=zhang
     2101                                          column=info:name, timestamp=1664488351973, value=zhang
     3001                                          column=info:name, timestamp=1664488351971, value=zhang
     3333                                          column=info:name, timestamp=1664962709668, value=maria
    5 row(s) in 0.0220 seconds
     */

    // getCells
    def getCells(namespace: String, tableName: String, rowKey: String,
                 columnFamily: String, columnName: String) = {
      val table = HBASE_CONN.getTable(TableName.valueOf(namespace, tableName))
      // 如果直接调用get方法读取数据 此时读取一整行数据
      val get = new Get(Bytes.toBytes(rowKey))
      // 也可以指定列族和列名
      get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
      val result = table.get(get)
      // 此处就输出看看吧 -- 真实情况肯定是返回指定格式数据
      // 拿到所有的cell
      val cells = result.rawCells()
      cells.foreach(cell => {
        // println(cell) => 2003/info:name/1664488351966/Put/vlen=5/seqid=0
        // 因为cell存储比较底层, 可以直接使用HBase提供的工具类来拿到我们想要的数据(假设我们想拿到行号和值)
        println(new String(CellUtil.cloneRow(cell)), new String(CellUtil.cloneValue(cell)))
      })
    }
    // getCells("test", "person", "2003", "info", "name")

    // 按照行号范围来扫描数据 (也可以添加其他条件的, 此处只是举例)
    def scanRows(namespace: String, tableName: String, startRow: String, stopRow: String) = {
      val table = HBASE_CONN.getTable(TableName.valueOf(namespace, tableName))
      val scan = new Scan()

      // 在此处也可以添加过滤器(就不再单独写一个方法了)
//      val filterList = new FilterList()
//      val filter = new SingleColumnValueFilter(
//        // 列族名称
//        Bytes.toBytes(""),
//        // 列名
//        Bytes.toBytes(""),
//        // 比较关系
//        CompareFilter.CompareOp.EQUAL,
//        // 值
//        Bytes.toBytes("")
//      )
//      filterList.addFilter(filter)
//      scan.setFilter(filter)

      scan.setStartRow(Bytes.toBytes(startRow))
      scan.setStopRow(Bytes.toBytes(stopRow))
      val scanner = table.getScanner(scan)
      scanner.forEach(new Consumer[Result] {
        override def accept(t: Result): Unit = {
          val cells = t.rawCells()
          cells.foreach(cell => {
            println(new String(CellUtil.cloneRow(cell)) + " -> " + new String(CellUtil.cloneValue(cell)) + "\t")
          })
          println()
        }
      })
      table.close()
    }
    // scanRows("test", "person", "0000", "9999")

    // delete
    def deleteColumn(namespace: String, tableName: String,
                     rowKey:String, columnFamily:String, columnName:String) = {
      val table = HBASE_CONN.getTable(TableName.valueOf(namespace, tableName))
      val delete = new Delete(Bytes.toBytes(rowKey))

      // 删除最近一个版本
      // delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
      // 删除所有版本
      delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))

      table.delete(delete)
    }
    HBASE_CONN.close()
  }
  // 整合Spark, HBase, Hive较复杂, 依赖过多, 因此先不实现
}
