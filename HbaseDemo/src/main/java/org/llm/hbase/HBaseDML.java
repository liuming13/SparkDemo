package org.llm.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDML {

    private static Connection connection = HBaseConnection.connection;

    /**
     * 插入数据
     *
     * @param namespace    命名空间
     * @param tableName    表格名称
     * @param rowKey       行键
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        值
     */
    public static void putCell(String namespace, String tableName, String rowKey,
                               String columnFamily, String columnName, String value)
            throws IOException {
        // 1. 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 调用相关方法
        Put put = new Put(Bytes.toBytes(rowKey));
        // 给Put对象添加属性
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //
        table.close();
    }

    /**
     * 读取单行数据
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @throws IOException
     */
    public static void getCells(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        // 如果直接调用get方法读取数据 此时读取一整行数据
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 处理数据
        Cell[] cells = result.rawCells();

        // 测试方法直接将数据打印到控制台
        for (Cell cell : cells) {
            // cell 存储数据比较底层
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println(value);
        }
    }

    /**
     * 扫描数据
     *
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));

        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Result 来记录一行数据
        // ResultScanner 来记录多行数据Result数组
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + " -> " + new String(CellUtil.cloneValue(cell)) + "\t");
            }
            System.out.println();
        }

        table.close();
    }

    /**
     * 过滤扫描
     *
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamily
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void filterScan(String namespace, String tableName, String startRow,
                                  String stopRow, String columnFamily, String columnName, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Scan scan = new Scan();

        // 可以添加多个过滤
        FilterList filterList = new FilterList();

        // 创建过滤器
        // (1) 结果只保留当前列数据

        // (2) 结果保留整行数据 结果会保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // 列族名称
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareFilter.CompareOp.EQUAL,
                // 值
                Bytes.toBytes(value)
        );
        filterList.addFilter(singleColumnValueFilter);

        scan.setFilter(filterList);

        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));

        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Result 来记录一行数据
        // ResultScanner 来记录多行数据Result数组
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + " -> " + new String(CellUtil.cloneValue(cell)) + "\t");
            }
            System.out.println();
        }

        table.close();
    }

    public static void deleteColumn(String namespace, String tableName,
                                    String rowKey, String columnFamily, String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 添加列信息
        // addColumn删除一个版本
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        // 删除所有版本
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    public static void main(String[] args) throws IOException {
//        putCell("test", "person", "2001", "info", "name", "zhang");
//        putCell("test", "person", "2003", "info", "name", "zhang");
//        putCell("test", "person", "2005", "info", "name", "zhang");
//        putCell("test", "person", "3001", "info", "name", "zhang");
//        putCell("test", "person", "2101", "info", "name", "zhang");

        System.out.println();
        scanRows("test", "person", "1111", "2222");
        System.out.println();

        filterScan("test", "person", "0000", "2999", "info", "name", "zhang");

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        getCells("test", "person", "2001", "info", "name");
        deleteColumn("test", "person", "2001", "info", "name");
        getCells("test", "person", "2001", "info", "name");

        System.out.println("Other code!");

        HBaseConnection.closeConnection();
    }
}
