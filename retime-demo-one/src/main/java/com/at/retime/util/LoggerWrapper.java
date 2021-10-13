package com.at.retime.util;

import org.slf4j.LoggerFactory;

/**
 * @create 2021-10-12
 */
public class LoggerWrapper {

    public static void info(Class clazz, String msg){
        LoggerFactory.getLogger(clazz).info(msg);
    }
}
