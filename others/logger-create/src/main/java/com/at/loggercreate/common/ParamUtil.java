package com.at.loggercreate.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zero
 * @create 2021-06-26 17:42
 */
public class ParamUtil {
    private static final Logger log = LoggerFactory.getLogger(ParamUtil.class);

    public ParamUtil() {
    }

    public static final Integer checkRatioNum(String rate) {
        try {
            Integer rateNum = Integer.valueOf(rate);
            if (rateNum >= 0 && rateNum <= 100) {
                return rateNum;
            } else {
                throw new RuntimeException("输入的比率必须为0 - 100 的数字");
            }
        } catch (Exception var2) {
            throw new RuntimeException("输入的比率必须为0 - 100 的数字");
        }
    }

    public static final Date checkDate(String dateString) {
        new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            String timeString = timeFormat.format(new Date());
            String datetimeString = dateString + " " + timeString;
            Date date = datetimeFormat.parse(datetimeString);
            return date;
        } catch (ParseException var7) {
            throw new RuntimeException("必须为日期型格式 例如： 2020-02-02");
        }
    }

    public static final Boolean checkBoolean(String bool) {
        if (!bool.equals("1") && !bool.equals("true")) {
            if (!bool.equals("0") && !bool.equals("false")) {
                throw new RuntimeException("是非型参数请填写：1或0 ， true 或 false");
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public static final Integer[] checkRate(String rateString, int rateCount) {
        try {
            String[] rateArray = rateString.split(":");
            if (rateArray.length != rateCount) {
                throw new RuntimeException("请按比例个数不足 ");
            } else {
                Integer[] rateNumArr = new Integer[rateArray.length];

                for(int i = 0; i < rateArray.length; ++i) {
                    Integer rate = checkRatioNum(rateArray[i]);
                    rateNumArr[i] = rate;
                }

                return rateNumArr;
            }
        } catch (Exception var6) {
            throw new RuntimeException("请按比例填写 如   75:10:15");
        }
    }

    public static final String[] checkArray(String str) {
        if (str == null) {
            throw new RuntimeException("搜索词为空");
        } else {
            String[] split = str.split(",");
            return split;
        }
    }

    public static final Integer checkCount(String count) {
        try {
            if (count == null) {
                return 0;
            } else {
                Integer rateNum = Integer.valueOf(count.trim());
                return rateNum;
            }
        } catch (Exception var2) {
            throw new RuntimeException("输入的数据必须为数字");
        }
    }

    public static void main(String[] args) {
        System.out.println(checkDate("2019-13-1123"));
        System.out.println("ok");
    }
}
