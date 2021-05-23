package com.at.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zero
 * @create 2021-05-15 13:35
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance() {
        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    pool = new ThreadPoolExecutor(
                            4,
                            14,
                            10,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(500)
                    );
                }
            }
        }
        return pool;
    }


}
