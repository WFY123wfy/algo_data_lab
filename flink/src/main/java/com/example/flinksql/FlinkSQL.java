package com.example.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL测试
 */
public class FlinkSQL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建输入表: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/datagen/
        String tableSource = "CREATE TABLE source_table (" +
                " order_id STRING," +
                " price INT," +
                " message STRING," +
                " order_time TIMESTAMP(3)" +
                ") WITH (" +
                " 'connector' = 'datagen'," +
                " 'rows-per-second' = '10'," +
                " 'fields.order_id.length' = '10'," +
                " 'fields.price.min' = '1'," +
                " 'fields.price.max' = '100'" +
                ")";
        tableEnv.executeSql(tableSource);

        // 创建输出表: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/print/
        String tableSink = "CREATE TABLE print_table (" +
                "  order_id STRING," +
                "  price INT," +
                "  order_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'print'" +
                ");";
        tableEnv.executeSql(tableSink);

        // 查询并写入目标表
        tableEnv.sqlQuery("SELECT order_id, price, order_time FROM source_table")
                .executeInsert("print_table");
    }
}