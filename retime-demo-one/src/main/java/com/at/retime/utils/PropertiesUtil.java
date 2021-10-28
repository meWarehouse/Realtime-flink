package com.at.retime.utils;


import java.io.File;
import java.io.IOException;

import java.util.Properties;

/**
 * @create 2021-10-28
 */
public class PropertiesUtil {

    public static Properties getResource(String resourceName) throws IOException {

        Properties properties = new Properties();

        properties.load(Thread.currentThread().getContextClassLoader().getResource(resourceName).openStream());

        return properties;
    }

    public static void main(String[] args) throws IOException {

        Properties properties = PropertiesUtil.getResource("config.properties");

        properties.forEach((k,v) -> System.out.println(k + " -> " + v));


    }

}
