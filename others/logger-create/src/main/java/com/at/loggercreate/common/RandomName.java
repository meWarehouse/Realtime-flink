package com.at.loggercreate.common;

import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * @author zero
 * @create 2021-06-26 17:44
 */
public class RandomName {
    public RandomName() {
    }

    public static String getChineseFamilyName() {
        String str = null;
        Random random = new Random();
        String[] Surname = new String[]{"赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许", "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜", "戚", "谢", "邹", "柏", "窦", "苏", "潘", "葛", "范", "彭", "鲁", "韦", "马", "苗", "凤", "方", "俞", "任", "袁", "柳", "鲍", "史", "唐", "费", "岑", "薛", "雷", "贺", "汤", "滕", "殷", "罗", "毕", "郝", "邬", "安", "常", "乐", "于", "时", "傅", "卞", "齐", "康", "伍", "余", "元", "卜", "顾", "孟", "平", "黄", "和", "穆", "萧", "尹", "姚", "汪", "祁", "毛", "狄", "臧", "计", "伏", "成", "戴", "宋", "茅", "庞", "熊", "纪", "舒", "司马", "上官", "欧阳", "夏侯", "诸葛", "闻人", "东方", "尉迟", "濮阳", "淳于", "单于", "公孙", "轩辕", "令狐", "钟离", "宇文", "长孙", "慕容", "司徒", "司空", "南门", "呼延", "百里", "东郭", "西门", "南宫", "独孤", "南宫"};
        int index = random.nextInt(Surname.length - 1);
        str = Surname[index];
        return str;
    }

    public static String getChineseGivenName() {
        String str = null;
        Random random = new Random();
        int highPos = 176 + Math.abs(random.nextInt(71));
        random = new Random();
        int lowPos = 161 + Math.abs(random.nextInt(94));
        byte[] bArr = new byte[]{(new Integer(highPos)).byteValue(), (new Integer(lowPos)).byteValue()};

        try {
            str = new String(bArr, "GB2312");
        } catch (UnsupportedEncodingException var6) {
            var6.printStackTrace();
        }

        return str;
    }

    public static String getNickName(String gender, String lastName) {
        if (lastName.length() == 1) {
            return gender.equals("M") ? "阿" + lastName : lastName + lastName;
        } else {
            return lastName;
        }
    }

    public static String insideLastName(String gender) {
        String name_sex = "";
        String boyName = "伟刚勇毅俊峰强军平保东文辉力明永健世广志义兴良海山仁波宁贵福生龙元全国胜学祥才发武新利清飞彬富顺信子杰涛昌成康星光天达安岩中茂进林有坚和彪博诚先敬震振壮会思群豪心邦承乐绍功松善厚庆磊民友裕河哲江超浩亮政谦亨奇固之轮翰朗伯宏言若鸣朋斌梁栋维启克伦翔旭鹏泽晨辰士以建家致树炎德行时泰盛雄琛钧冠策腾楠榕风航弘";
        String girlName = "秀娟英华慧巧美娜静淑惠珠翠雅芝玉萍玲芬芳燕彩春菊兰凤洁梅琳素云莲真环雪荣爱妹霞香月莺媛艳瑞凡佳嘉琼勤珍贞莉桂娣叶璧璐娅琦晶妍茜秋珊莎锦黛青倩婷姣婉娴瑾颖露瑶怡婵雁蓓纨仪荷丹蓉眉君琴蕊薇菁梦岚艺咏卿聪澜纯毓悦昭冰爽琬茗羽希宁欣飘育滢馥筠柔竹霭凝晓欢霄枫芸菲寒伊亚宜可姬舒影荔枝思丽韶涵予馨艺欣";
        String str = "";
        int length = boyName.length();
        if (gender.equals("F")) {
            str = girlName;
            length = girlName.length();
        } else {
            str = boyName;
            length = boyName.length();
        }

        int nameCount = RandomNum.getRandInt(1, 2);
        int index = RandomNum.getRandInt(0, length - nameCount);
        return str.substring(index, index + nameCount);
    }

    public static String genName() {
        return genName((new RandomOptionGroup(new String[]{"F", "M"})).getRandStringValue());
    }

    public static String genName(String gender) {
        String name = getChineseFamilyName();
        String lastName = insideLastName(gender);
        return name + lastName;
    }

    public static void main(String[] args) {
        System.out.println(genName("F"));
    }
}
