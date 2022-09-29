package org.llm.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {
    private static final Connection connection = HBaseConnection.connection;

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     */
    public static void createNamespace(String namespace) throws IOException {
        System.out.println(connection);
        // Admin -> DDL     Table -> DML
        // 别瞎抛异常, 等待方法写完, 再统一处理
        // admin的连接是轻量级的 不是线程安全的 不推荐进行池化或者缓存这个连接
        Admin admin = connection.getAdmin();
        System.out.println(admin);
        // 调用方法创建命名空间
        // 创建命名空间描述
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace)
                .build();

        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            System.out.println("命名空间已经存在！");
            e.printStackTrace();
        }

        // 关闭admin
        admin.close();
    }

    /**
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return
     */
    public static boolean isTableExists(String namespace, String tableName) throws IOException {
        // 1. 获取admin
        Admin admin = connection.getAdmin();

        // 2. 判断表格是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

        return b;
    }

    /**
     * @param namespace      命名空间名称
     * @param tableName      表格名称
     * @param columnFamilies 列族名称
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {
        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族！");
            return;
        } else if (isTableExists(namespace, tableName)) {
            System.out.println("表格已经存在！");
            return;
        }
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(namespace, tableName));

        for (String columnFamily : columnFamilies) {
            hTableDescriptor.addFamily(new HColumnDescriptor(new HColumnDescriptor(columnFamily).setMaxVersions(1)));
        }
        // 调用方法创建表格
        try {
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    /**
     * 修改表格中一个列族的版本
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param columnFamily 列族名称
     * @param version      版本
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {
        Admin admin = connection.getAdmin();

        if (isTableExists(namespace, tableName)) {
            System.out.println("表格已经存在！");
            return;
        }

        // 如果使用TableName.valueOf(namespace, tableName) 相当于创建了一个新的表格描述 没有之前的信息因此修改会抛出异常
        // HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(namespace, tableName));

        // 如果想要修改之前的信息 必须调用方法填写一个旧的表格描述
        HTableDescriptor htd = new HTableDescriptor(admin.getTableDescriptor(TableName.valueOf(namespace, tableName)));

        // 需要填写旧的列族描述 不然除修改的值外，其他值都会被初始化
        htd.modifyFamily(htd.getFamily(Bytes.toBytes(columnFamily)).setMaxVersions(version));
        try {
            admin.modifyTable(TableName.valueOf(namespace, tableName), htd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        // 1.
        if (isTableExists(namespace, tableName)) {
            return false;
        }

        Admin admin = connection.getAdmin();

        try {
            TableName name = TableName.valueOf(namespace, tableName);
            admin.disableTable(name);
            admin.deleteTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

        return true;
    }

    public static void main(String[] args) throws IOException {
        // 应该先保证连接没有问题 再来调用相关的方法
//        createNamespace("test");
        System.out.println(isTableExists("test", "student"));

        createTable("test", "person", "info", "msg");
        modifyTable("test", "person", "info", 2);
        System.out.println("Other Code!");
        HBaseConnection.closeConnection();
    }
}
