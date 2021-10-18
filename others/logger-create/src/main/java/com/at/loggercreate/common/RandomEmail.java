package com.at.loggercreate.common;

/**
 * @author zero
 * @create 2021-06-26 17:43
 */
public class RandomEmail {
    private static final String[] email_suffix = "@gmail.com,@yahoo.com,@msn.com,@hotmail.com,@aol.com,@ask.com,@live.com,@qq.com,@0355.net,@163.com,@163.net,@263.net,@3721.net,@yeah.net,@googlemail.com,@126.com,@sina.com,@sohu.com,@yahoo.com.cn".split(",");
    public static String base = "abcdefghijklmnopqrstuvwxyz0123456789";

    public RandomEmail() {
    }

    public static int getNum(int start, int end) {
        return (int)(Math.random() * (double)(end - start + 1) + (double)start);
    }

    public static String getEmail(int lMin, int lMax) {
        int length = getNum(lMin, lMax);
        StringBuffer sb = new StringBuffer();

        for(int i = 0; i < length; ++i) {
            int number = (int)(Math.random() * (double)base.length());
            sb.append(base.charAt(number));
        }

        sb.append(email_suffix[(int)(Math.random() * (double)email_suffix.length)]);
        return sb.toString();
    }

    public static void main(String[] args) {
        for(int i = 0; i < 10; ++i) {
            String email = getEmail(1, i);
            System.out.println(email);
        }

    }
}
