package com.at.retime.utils;



import com.at.retime.bean.TableProcess;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @author zero
 * @create 2021-05-14 13:32
 */
@Slf4j
public class MySQLUtil {

    /**
     * Mysql 链接四要素
     * 1.驱动
     * 2.地址
     * 3.账户
     * 4.密码
     * <p>
     * 获取连接
     * 使用连接准备sql语句
     * 执行语句
     * 封装结果
     */

    private static Lock lock = new ReentrantLock();

    private static Connection mConn;

    public static Connection getConnection() {

        if (mConn == null) {
            synchronized (MySQLUtil.class) {
                if (mConn == null) {
                    try {
                        Class.forName("com.mysql.jdbc.Driver");
                        mConn = DriverManager.getConnection(
                                "jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false",
                                "root", "root"
                        );
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("mysql 连接异常：" + e.getMessage());
                    }

                }
            }
        }
        return mConn;
    }



    /**
     * @param sql
     * @param clz
     * @param underScoreToCamel
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {

        if(mConn == null){
            mConn = getConnection();
        }


        PreparedStatement pre = null;
        ResultSet resultSet = null;

        try {

            pre = mConn.prepareStatement(sql);
            resultSet = pre.executeQuery();

            return resultSet == null ? null : SqlUtil.parseDate(resultSet, clz, underScoreToCamel);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("数据查询异常：" + e.getMessage());
        } finally {
            SqlUtil.close(null, pre, resultSet);

        }

        return null;

    }


    public static void main(String[] args) {

        List<TableProcess> res = queryList("SELECT * FROM table_process", TableProcess.class, true);

        if (res != null) {
            for (TableProcess re : res) {
                System.out.println(re);
            }

        }

    }


}
