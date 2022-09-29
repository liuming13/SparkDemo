package org.llm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {

    public static Connection connection = null;

    static {
        try {
            // 使用读取本地文件的方式来添加参数
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeConnection() throws IOException {
        if (connection != null)
            connection.close();
    }

//    public static void main(String[] args) throws IOException {

//        // 1. 创建连接配置对象
//        Configuration conf = new Configuration();
//        // 2. 添加配置参数
//        conf.set("hbase.zookeeper.quorum", "master");
//        // 3. 创建连接
//        // 默认使用同步连接
//        Connection connection = ConnectionFactory.createConnection();
//
//        // 可以使用异步连接(这个低版本好像默认无此功能)
//
//        // 4. 使用连接
//        System.out.println(connection);
//
//        // 5. 关闭连接
//        connection.close();

        // 使用创建好的连接
//        System.out.println(HBaseConnection.connection);
//        HBaseConnection.closeConnection();
//    }


}
