package com.example;

import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*
  内存sql使用
 */
public class MemorySQL {
    private Map<String, PreparedStatement> statementMap = Maps.newHashMap();

    public static void main(String[] args) throws InterruptedException, SQLException {
        MemorySQL memorySQL = new MemorySQL();
        CalciteConnection connection = memorySQL.getConnection();


        for (int i = 0; i < 100; i++) {
            long startTime = System.currentTimeMillis();
            List<DataModel> dataModelList = new ArrayList<>();
            Map<String, Table> tables = Maps.newHashMap();
            DataModel model = new DataModel();
            model.setMsg("success_" + i);
            dataModelList.add(model);

            // 待调试
//            Table orderTable = new ListTransientTable("TABLE_NAME", dataModelList);
//            name2table.put("TABLE_NAME", orderTable);
//            DynamicSchema ds = new DynamicSchema(name2table); // DynamicSchema extends AbstractSchema
//            SchemaPlus schemaPlus = connection.getRootSchema();
//            for(String table:ds.getTableMap().keySet()) {
//                schemaPlus.add(table, ds.getTable(table));
//            }
            PreparedStatement statement = memorySQL.getPreparedStatement(connection, "select * from TABLE_NAME");
            ResultSet resultSet = statement.executeQuery();
            System.out.println(String.format("第%s次执行耗时: %s 毫秒, data", i, (System.currentTimeMillis() - startTime)));
            // 遍历结果集, 打印数据
            while (resultSet.next()) {
                System.out.println("data: " + resultSet.getString("status"));
            }
            TimeUnit.SECONDS.sleep(2);
        }
    }

    private CalciteConnection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("lex","JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:",properties);
        return connection.unwrap(CalciteConnection.class);
    }

    private PreparedStatement getPreparedStatement(CalciteConnection conn, String sql) throws SQLException {
        if (statementMap.get(sql) != null) {
            return statementMap.get(sql);
        }
        PreparedStatement statement = conn.prepareStatement(sql);
        statementMap.put(sql, statement);
        return statementMap.get(sql);
    }
}

@Data
class DataModel {
    public int id;
    public String msg;
}
