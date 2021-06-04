package com.at.utils;


import java.util.ArrayList;
import java.util.List;

/**
 * @author zero
 * @create 2021-05-23 19:15
 * <p>
 * 使用 ik 分词器分词
 */
public class KeywordUtil {

    public static List<String> analyze(String text) {
//
        List<String> list = new ArrayList<>();
//
//        StringReader input = new StringReader(text);
//        IKSegmenter ikSegmenter = new IKSegmenter(input, true);
//
//        Lexeme next = null;
//
//        while (true) {
//            try {
//                if ((next = ikSegmenter.next()) != null) {
//                    list.add(next.getLexemeText());
//                } else {
//                    break;
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//                System.out.println("分词失败：" + e.getMessage());
//            }
//        }
//
        return list;

    }

    public static void main(String[] args) {
        String text = "Flink 实时数仓项目";
        System.out.println(KeywordUtil.analyze(text));

    }

}
