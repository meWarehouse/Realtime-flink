package com.at.loggercreate.bean;

import com.at.loggercreate.common.RandomNum;
import com.at.loggercreate.common.RandomOptionGroup;
import com.at.loggercreate.config.AppConfig;
import com.at.loggercreate.enums.DisplayType;
import com.at.loggercreate.enums.ItemType;
import com.at.loggercreate.enums.PageId;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zero
 * @create 2021-06-26 17:52
 */
public class AppDisplay {
    ItemType item_type;
    String item;
    DisplayType display_type;
    Integer order;
    Integer pos_id;

    public static List<AppDisplay> buildList(AppPage appPage) {
        List<AppDisplay> displayList = new ArrayList();
        int displayCount;
        int activityCount;
        int i;
        int skuId;
        if (appPage.page_id == PageId.home || appPage.page_id == PageId.discovery || appPage.page_id == PageId.category) {
            displayCount = RandomNum.getRandInt(1, AppConfig.max_activity_count);
            activityCount = RandomNum.getRandInt(1, AppConfig.max_pos_id);

            for(i = 1; i <= displayCount; ++i) {
                skuId = RandomNum.getRandInt(1, AppConfig.max_activity_count);
                AppDisplay appDisplay = new AppDisplay(ItemType.activity_id, skuId + "", DisplayType.activity, i, activityCount);
                displayList.add(appDisplay);
            }
        }

        if (appPage.page_id == PageId.good_detail || appPage.page_id == PageId.home || appPage.page_id == PageId.category || appPage.page_id == PageId.activity || appPage.page_id == PageId.good_spec || appPage.page_id == PageId.good_list || appPage.page_id == PageId.discovery) {
            displayCount = RandomNum.getRandInt(AppConfig.min_display_count, AppConfig.max_display_count);
            activityCount = displayList.size();

            for(i = 1 + activityCount; i <= displayCount + activityCount; ++i) {
                skuId = RandomNum.getRandInt(1, AppConfig.max_sku_id);
                int pos_id = RandomNum.getRandInt(1, AppConfig.max_pos_id);
                RandomOptionGroup<DisplayType> dispTypeGroup = RandomOptionGroup.builder().add(DisplayType.promotion, 30).add(DisplayType.query, 60).add(DisplayType.recommend, 10).build();
                DisplayType displayType = (DisplayType)dispTypeGroup.getValue();
                AppDisplay appDisplay = new AppDisplay(ItemType.sku_id, skuId + "", displayType, i, pos_id);
                displayList.add(appDisplay);
            }
        }

        return displayList;
    }

    public ItemType getItem_type() {
        return this.item_type;
    }

    public String getItem() {
        return this.item;
    }

    public DisplayType getDisplay_type() {
        return this.display_type;
    }

    public Integer getOrder() {
        return this.order;
    }

    public Integer getPos_id() {
        return this.pos_id;
    }

    public void setItem_type(final ItemType item_type) {
        this.item_type = item_type;
    }

    public void setItem(final String item) {
        this.item = item;
    }

    public void setDisplay_type(final DisplayType display_type) {
        this.display_type = display_type;
    }

    public void setOrder(final Integer order) {
        this.order = order;
    }

    public void setPos_id(final Integer pos_id) {
        this.pos_id = pos_id;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AppDisplay)) {
            return false;
        } else {
            AppDisplay other = (AppDisplay)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                label71: {
                    Object this$item_type = this.getItem_type();
                    Object other$item_type = other.getItem_type();
                    if (this$item_type == null) {
                        if (other$item_type == null) {
                            break label71;
                        }
                    } else if (this$item_type.equals(other$item_type)) {
                        break label71;
                    }

                    return false;
                }

                Object this$item = this.getItem();
                Object other$item = other.getItem();
                if (this$item == null) {
                    if (other$item != null) {
                        return false;
                    }
                } else if (!this$item.equals(other$item)) {
                    return false;
                }

                label57: {
                    Object this$display_type = this.getDisplay_type();
                    Object other$display_type = other.getDisplay_type();
                    if (this$display_type == null) {
                        if (other$display_type == null) {
                            break label57;
                        }
                    } else if (this$display_type.equals(other$display_type)) {
                        break label57;
                    }

                    return false;
                }

                Object this$order = this.getOrder();
                Object other$order = other.getOrder();
                if (this$order == null) {
                    if (other$order != null) {
                        return false;
                    }
                } else if (!this$order.equals(other$order)) {
                    return false;
                }

                Object this$pos_id = this.getPos_id();
                Object other$pos_id = other.getPos_id();
                if (this$pos_id == null) {
                    if (other$pos_id == null) {
                        return true;
                    }
                } else if (this$pos_id.equals(other$pos_id)) {
                    return true;
                }

                return false;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof AppDisplay;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $item_type = this.getItem_type();
         result = result * 59 + ($item_type == null ? 43 : $item_type.hashCode());
        Object $item = this.getItem();
        result = result * 59 + ($item == null ? 43 : $item.hashCode());
        Object $display_type = this.getDisplay_type();
        result = result * 59 + ($display_type == null ? 43 : $display_type.hashCode());
        Object $order = this.getOrder();
        result = result * 59 + ($order == null ? 43 : $order.hashCode());
        Object $pos_id = this.getPos_id();
        result = result * 59 + ($pos_id == null ? 43 : $pos_id.hashCode());
        return result;
    }

    public String toString() {
        return "AppDisplay(item_type=" + this.getItem_type() + ", item=" + this.getItem() + ", display_type=" + this.getDisplay_type() + ", order=" + this.getOrder() + ", pos_id=" + this.getPos_id() + ")";
    }

    public AppDisplay(final ItemType item_type, final String item, final DisplayType display_type, final Integer order, final Integer pos_id) {
        this.item_type = item_type;
        this.item = item;
        this.display_type = display_type;
        this.order = order;
        this.pos_id = pos_id;
    }
}
