package com.at.loggercreate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zero
 * @create 2021-06-26 17:37
 */
public class LogUtil {
    private static final Logger log = LoggerFactory.getLogger(LogUtil.class);

    public LogUtil() {
    }

    public static void log(String logString) {
        log.info(logString);
    }
}
