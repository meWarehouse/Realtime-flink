package com.at.utils;

import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zero
 * @create 2021-05-14 19:54
 */
@Slf4j
public class SqlUtil {

    private static Lock lock = new ReentrantLock();

    /**
     * 解析sql数据
     *
     * @param resultSet
     * @param clz
     * @param underScoreToCamel
     * @param <T>
     * @return
     */
    public static <T> List<T> parseDate(ResultSet resultSet, Class<T> clz, boolean underScoreToCamel) {

        List<T> list = new ArrayList<>();

        try {
            while (resultSet.next()) {
                T t = clz.newInstance();
                ResultSetMetaData metaData = resultSet.getMetaData();

                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = columnName;

                    if (columnName.contains("_") && underScoreToCamel) {
                        //将下滑线后面的字符转化为大写
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    BeanUtils.setProperty(t, propertyName, resultSet.getObject(i));

                }
                list.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("数据解析异常：" + e.getMessage());
        }

//        System.out.println(Thread.currentThread().getName() + ":" + list);

        return list;

    }

    /**
     * 不需要返回值得都可以
     *
     * @param sql
     * @param conn
     * @throws SQLException
     */
    public static void executeSql(String sql, Connection conn) throws SQLException {

        PreparedStatement pre = null;


        lock.lock();
        try {

            System.out.println("执行的sql:>>>>>: " + sql);
            pre = conn.prepareStatement(sql);
            pre.execute();
            conn.commit();

        } finally {
            lock.unlock();
            close(null, pre, null);
        }

    }

    /**
     * 关闭数据库连接
     *
     * @param conn
     * @param pre
     * @param resultSet
     */
    public static void close(Connection conn, PreparedStatement pre, ResultSet resultSet) {

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.error("数据库连接关闭异常：" + throwables.getMessage());
            }
        }

        if (pre != null) {
            try {
                pre.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.error("数据库连接关闭异常：" + throwables.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.error("数据库连接关闭异常：" + throwables.getMessage());
            }
        }


    }


    public static int insertOrUpdate(Connection conn, String sql) {
        return 0;
    }


}
