package com.at.loggercreate.bean;

import com.at.loggercreate.common.RandomNum;
import com.at.loggercreate.common.RandomNumString;
import com.at.loggercreate.common.RandomOptionGroup;
import com.at.loggercreate.config.AppConfig;
import com.at.loggercreate.enums.DisplayType;
import com.at.loggercreate.enums.ItemType;
import com.at.loggercreate.enums.PageId;

/**
 * @author zero
 * @create 2021-06-26 17:53
 */
public class AppPage {
    PageId last_page_id;
    PageId page_id;
    ItemType item_type;
    String item;
    Integer during_time;
    String extend1;
    String extend2;
    DisplayType source_type;

    public static AppPage build(PageId pageId, PageId lastPageId, Integer duringTime) {
        ItemType itemType = null;
        String item = null;
        String extend1 = null;
        String extend2 = null;
        DisplayType sourceType = null;
        RandomOptionGroup<DisplayType> sourceTypeGroup = RandomOptionGroup.builder().add(DisplayType.query, AppConfig.sourceTypeRate[0]).add(DisplayType.promotion, AppConfig.sourceTypeRate[1]).add(DisplayType.recommend, AppConfig.sourceTypeRate[2]).add(DisplayType.activity, AppConfig.sourceTypeRate[3]).build();
        if (pageId != PageId.good_detail && pageId != PageId.good_spec && pageId != PageId.comment && pageId != PageId.comment_list) {
            if (pageId == PageId.good_list) {
                itemType = ItemType.keyword;
                item = (new RandomOptionGroup(AppConfig.searchKeywords)).getRandStringValue();
            } else if (pageId == PageId.trade || pageId == PageId.payment || pageId == PageId.payment_done) {
                itemType = ItemType.sku_ids;
                item = RandomNumString.getRandNumString(1, AppConfig.max_sku_id, RandomNum.getRandInt(1, 3), ",", false);
            }
        } else {
            sourceType = (DisplayType)sourceTypeGroup.getValue();
            itemType = ItemType.sku_id;
            item = RandomNum.getRandInt(1, AppConfig.max_sku_id) + "";
        }

        return new AppPage(lastPageId, pageId, itemType, item, duringTime, (String)extend1, (String)extend2, sourceType);
    }

    public PageId getLast_page_id() {
        return this.last_page_id;
    }

    public PageId getPage_id() {
        return this.page_id;
    }

    public ItemType getItem_type() {
        return this.item_type;
    }

    public String getItem() {
        return this.item;
    }

    public Integer getDuring_time() {
        return this.during_time;
    }

    public String getExtend1() {
        return this.extend1;
    }

    public String getExtend2() {
        return this.extend2;
    }

    public DisplayType getSource_type() {
        return this.source_type;
    }

    public void setLast_page_id(final PageId last_page_id) {
        this.last_page_id = last_page_id;
    }

    public void setPage_id(final PageId page_id) {
        this.page_id = page_id;
    }

    public void setItem_type(final ItemType item_type) {
        this.item_type = item_type;
    }

    public void setItem(final String item) {
        this.item = item;
    }

    public void setDuring_time(final Integer during_time) {
        this.during_time = during_time;
    }

    public void setExtend1(final String extend1) {
        this.extend1 = extend1;
    }

    public void setExtend2(final String extend2) {
        this.extend2 = extend2;
    }

    public void setSource_type(final DisplayType source_type) {
        this.source_type = source_type;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AppPage)) {
            return false;
        } else {
            AppPage other = (AppPage)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                label107: {
                    Object this$last_page_id = this.getLast_page_id();
                    Object other$last_page_id = other.getLast_page_id();
                    if (this$last_page_id == null) {
                        if (other$last_page_id == null) {
                            break label107;
                        }
                    } else if (this$last_page_id.equals(other$last_page_id)) {
                        break label107;
                    }

                    return false;
                }

                Object this$page_id = this.getPage_id();
                Object other$page_id = other.getPage_id();
                if (this$page_id == null) {
                    if (other$page_id != null) {
                        return false;
                    }
                } else if (!this$page_id.equals(other$page_id)) {
                    return false;
                }

                Object this$item_type = this.getItem_type();
                Object other$item_type = other.getItem_type();
                if (this$item_type == null) {
                    if (other$item_type != null) {
                        return false;
                    }
                } else if (!this$item_type.equals(other$item_type)) {
                    return false;
                }

                label86: {
                    Object this$item = this.getItem();
                    Object other$item = other.getItem();
                    if (this$item == null) {
                        if (other$item == null) {
                            break label86;
                        }
                    } else if (this$item.equals(other$item)) {
                        break label86;
                    }

                    return false;
                }

                label79: {
                    Object this$during_time = this.getDuring_time();
                    Object other$during_time = other.getDuring_time();
                    if (this$during_time == null) {
                        if (other$during_time == null) {
                            break label79;
                        }
                    } else if (this$during_time.equals(other$during_time)) {
                        break label79;
                    }

                    return false;
                }

                label72: {
                    Object this$extend1 = this.getExtend1();
                    Object other$extend1 = other.getExtend1();
                    if (this$extend1 == null) {
                        if (other$extend1 == null) {
                            break label72;
                        }
                    } else if (this$extend1.equals(other$extend1)) {
                        break label72;
                    }

                    return false;
                }

                Object this$extend2 = this.getExtend2();
                Object other$extend2 = other.getExtend2();
                if (this$extend2 == null) {
                    if (other$extend2 != null) {
                        return false;
                    }
                } else if (!this$extend2.equals(other$extend2)) {
                    return false;
                }

                Object this$source_type = this.getSource_type();
                Object other$source_type = other.getSource_type();
                if (this$source_type == null) {
                    if (other$source_type != null) {
                        return false;
                    }
                } else if (!this$source_type.equals(other$source_type)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof AppPage;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $last_page_id = this.getLast_page_id();
        result = result * 59 + ($last_page_id == null ? 43 : $last_page_id.hashCode());
        Object $page_id = this.getPage_id();
        result = result * 59 + ($page_id == null ? 43 : $page_id.hashCode());
        Object $item_type = this.getItem_type();
        result = result * 59 + ($item_type == null ? 43 : $item_type.hashCode());
        Object $item = this.getItem();
        result = result * 59 + ($item == null ? 43 : $item.hashCode());
        Object $during_time = this.getDuring_time();
        result = result * 59 + ($during_time == null ? 43 : $during_time.hashCode());
        Object $extend1 = this.getExtend1();
        result = result * 59 + ($extend1 == null ? 43 : $extend1.hashCode());
        Object $extend2 = this.getExtend2();
        result = result * 59 + ($extend2 == null ? 43 : $extend2.hashCode());
        Object $source_type = this.getSource_type();
        result = result * 59 + ($source_type == null ? 43 : $source_type.hashCode());
        return result;
    }

    public String toString() {
        return "AppPage(last_page_id=" + this.getLast_page_id() + ", page_id=" + this.getPage_id() + ", item_type=" + this.getItem_type() + ", item=" + this.getItem() + ", during_time=" + this.getDuring_time() + ", extend1=" + this.getExtend1() + ", extend2=" + this.getExtend2() + ", source_type=" + this.getSource_type() + ")";
    }

    public AppPage(final PageId last_page_id, final PageId page_id, final ItemType item_type, final String item, final Integer during_time, final String extend1, final String extend2, final DisplayType source_type) {
        this.last_page_id = last_page_id;
        this.page_id = page_id;
        this.item_type = item_type;
        this.item = item;
        this.during_time = during_time;
        this.extend1 = extend1;
        this.extend2 = extend2;
        this.source_type = source_type;
    }
}
