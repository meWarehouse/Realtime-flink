package com.at.utils;

import com.at.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zero
 * @create 2021-05-22 20:25
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getJdbcSink(String sql){
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement statement, T t) throws SQLException {
                        fieldAssignment(statement,t);
                    }
                },
                //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性赋值，执行执行批次大小
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                //构建者设计模式，JdbcConnectionOptions，给连接相关的属性进行赋值
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:clickhouse://hadoop102:8123/default")
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .build()

        );

    }

    /**
     * 给占位符赋值
     * @param statement
     * @param t
     * @param <T>
     */
    private static <T> void fieldAssignment(PreparedStatement statement, T t) {

        Field[] fields = t.getClass().getDeclaredFields();

        int isJump = 0;

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            TransientSink annotation = field.getAnnotation(TransientSink.class);
            if(annotation != null) {
                isJump++;
                continue;
            }

            try {
                statement.setObject(i+1-isJump,field.get(t));
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("插入 clickhouse 异常 ：" + e.getMessage());
            }

        }

    }

}
