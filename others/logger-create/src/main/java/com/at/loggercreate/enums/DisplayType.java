package com.at.loggercreate.enums;

/**
 * @author zero
 * @create 2021-06-26 17:32
 */
public enum DisplayType {
    promotion("商品推广"),
    recommend("算法推荐商品"),
    query("查询结果商品"),
    activity("促销活动");

    private String desc;

    private DisplayType(final String desc) {
        this.desc = desc;
    }
}
