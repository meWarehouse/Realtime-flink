package com.at.loggercreate.bean;

import com.at.loggercreate.common.RanOpt;
import com.at.loggercreate.common.RandomNum;
import com.at.loggercreate.common.RandomOptionGroup;

/**
 * @author zero
 * @create 2021-06-26 17:54
 */
public class AppStart {

    private String entry;
    private Long open_ad_id;
    private Integer open_ad_ms;
    private Integer open_ad_skip_ms;
    private Integer loading_time;

    public String getEntry() {
        return this.entry;
    }

    public Long getOpen_ad_id() {
        return this.open_ad_id;
    }

    public Integer getOpen_ad_ms() {
        return this.open_ad_ms;
    }

    public Integer getOpen_ad_skip_ms() {
        return this.open_ad_skip_ms;
    }

    public Integer getLoading_time() {
        return this.loading_time;
    }

    public void setEntry(final String entry) {
        this.entry = entry;
    }

    public void setOpen_ad_id(final Long open_ad_id) {
        this.open_ad_id = open_ad_id;
    }

    public void setOpen_ad_ms(final Integer open_ad_ms) {
        this.open_ad_ms = open_ad_ms;
    }

    public void setOpen_ad_skip_ms(final Integer open_ad_skip_ms) {
        this.open_ad_skip_ms = open_ad_skip_ms;
    }

    public void setLoading_time(final Integer loading_time) {
        this.loading_time = loading_time;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AppStart)) {
            return false;
        } else {
            AppStart other = (AppStart)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                label71: {
                    Object this$entry = this.getEntry();
                    Object other$entry = other.getEntry();
                    if (this$entry == null) {
                        if (other$entry == null) {
                            break label71;
                        }
                    } else if (this$entry.equals(other$entry)) {
                        break label71;
                    }

                    return false;
                }

                Object this$open_ad_id = this.getOpen_ad_id();
                Object other$open_ad_id = other.getOpen_ad_id();
                if (this$open_ad_id == null) {
                    if (other$open_ad_id != null) {
                        return false;
                    }
                } else if (!this$open_ad_id.equals(other$open_ad_id)) {
                    return false;
                }

                label57: {
                    Object this$open_ad_ms = this.getOpen_ad_ms();
                    Object other$open_ad_ms = other.getOpen_ad_ms();
                    if (this$open_ad_ms == null) {
                        if (other$open_ad_ms == null) {
                            break label57;
                        }
                    } else if (this$open_ad_ms.equals(other$open_ad_ms)) {
                        break label57;
                    }

                    return false;
                }

                Object this$open_ad_skip_ms = this.getOpen_ad_skip_ms();
                Object other$open_ad_skip_ms = other.getOpen_ad_skip_ms();
                if (this$open_ad_skip_ms == null) {
                    if (other$open_ad_skip_ms != null) {
                        return false;
                    }
                } else if (!this$open_ad_skip_ms.equals(other$open_ad_skip_ms)) {
                    return false;
                }

                Object this$loading_time = this.getLoading_time();
                Object other$loading_time = other.getLoading_time();
                if (this$loading_time == null) {
                    if (other$loading_time == null) {
                        return true;
                    }
                } else if (this$loading_time.equals(other$loading_time)) {
                    return true;
                }

                return false;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof AppStart;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $entry = this.getEntry();
        result = result * 59 + ($entry == null ? 43 : $entry.hashCode());
        Object $open_ad_id = this.getOpen_ad_id();
        result = result * 59 + ($open_ad_id == null ? 43 : $open_ad_id.hashCode());
        Object $open_ad_ms = this.getOpen_ad_ms();
        result = result * 59 + ($open_ad_ms == null ? 43 : $open_ad_ms.hashCode());
        Object $open_ad_skip_ms = this.getOpen_ad_skip_ms();
        result = result * 59 + ($open_ad_skip_ms == null ? 43 : $open_ad_skip_ms.hashCode());
        Object $loading_time = this.getLoading_time();
        result = result * 59 + ($loading_time == null ? 43 : $loading_time.hashCode());
        return result;
    }

    public String toString() {
        return "AppStart(entry=" + this.getEntry() + ", open_ad_id=" + this.getOpen_ad_id() + ", open_ad_ms=" + this.getOpen_ad_ms() + ", open_ad_skip_ms=" + this.getOpen_ad_skip_ms() + ", loading_time=" + this.getLoading_time() + ")";
    }

    public AppStart(final String entry, final Long open_ad_id, final Integer open_ad_ms, final Integer open_ad_skip_ms, final Integer loading_time) {
        this.entry = entry;
        this.open_ad_id = open_ad_id;
        this.open_ad_ms = open_ad_ms;
        this.open_ad_skip_ms = open_ad_skip_ms;
        this.loading_time = loading_time;
    }

    public static class Builder {
        private String entry = (new RandomOptionGroup(new RanOpt[]{new RanOpt("install", 5), new RanOpt("icon", 75), new RanOpt("notice", 20)})).getRandStringValue();
        private Long open_ad_id = (long) RandomNum.getRandInt(1, 20) + 0L;
        private Integer open_ad_ms = RandomNum.getRandInt(1000, 10000);
        private Integer open_ad_skip_ms;
        private Integer loading_time;
        private Integer first_open;

        public Builder() {
            this.open_ad_skip_ms = RandomOptionGroup.builder().add(0, 50).add(RandomNum.getRandInt(1000, this.open_ad_ms), 50).build().getRandIntValue();
            this.loading_time = RandomNum.getRandInt(1000, 20000);
            this.first_open = RandomNum.getRandInt(0, 1);
        }

        public AppStart build() {
            return new AppStart(this.entry, this.open_ad_id, this.open_ad_ms, this.open_ad_skip_ms, this.loading_time);
        }
    }
}
