package com.at.loggercreate.bean;

/**
 * @author zero
 * @create 2021-06-26 17:53
 */
public class AppNotice {
    Long notice_id;

    public Long getNotice_id() {
        return this.notice_id;
    }

    public void setNotice_id(final Long notice_id) {
        this.notice_id = notice_id;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AppNotice)) {
            return false;
        } else {
            AppNotice other = (AppNotice)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$notice_id = this.getNotice_id();
                Object other$notice_id = other.getNotice_id();
                if (this$notice_id == null) {
                    if (other$notice_id != null) {
                        return false;
                    }
                } else if (!this$notice_id.equals(other$notice_id)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof AppNotice;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $notice_id = this.getNotice_id();
         result = result * 59 + ($notice_id == null ? 43 : $notice_id.hashCode());
        return result;
    }

    public String toString() {
        return "AppNotice(notice_id=" + this.getNotice_id() + ")";
    }

    public AppNotice(final Long notice_id) {
        this.notice_id = notice_id;
    }
}
