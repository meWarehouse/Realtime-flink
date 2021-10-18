package com.at.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zero
 * @create 2021-06-26 15:54
 */
public class Tes {
    public static void main(String[] args) throws ParseException {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = format.parse("2021-06-26 15:00:00");

        System.out.println(date.getTime());


    }
}
