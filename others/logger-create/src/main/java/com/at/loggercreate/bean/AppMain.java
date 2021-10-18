package com.at.loggercreate.bean;

import com.alibaba.fastjson.JSON;
import com.at.loggercreate.common.RandomOptionGroup;
import com.at.loggercreate.config.AppConfig;

import java.util.List;

/**
 * @author zero
 * @create 2021-06-26 17:53
 */
public class AppMain {
    private Long ts;
    private AppCommon common;
    private AppPage page;
    private AppError err;
    private AppNotice notice;
    private AppStart start;
    private List<AppDisplay> displays;
    private List<AppAction> actions;

    public String toString() {
        return JSON.toJSONString(this);
    }

    AppMain(final Long ts, final AppCommon common, final AppPage page, final AppError err, final AppNotice notice, final AppStart start, final List<AppDisplay> displays, final List<AppAction> actions) {
        this.ts = ts;
        this.common = common;
        this.page = page;
        this.err = err;
        this.notice = notice;
        this.start = start;
        this.displays = displays;
        this.actions = actions;
    }

    public static AppMain.AppMainBuilder builder() {
        return new AppMain.AppMainBuilder();
    }

    private AppMain() {
    }

    public Long getTs() {
        return this.ts;
    }

    public AppCommon getCommon() {
        return this.common;
    }

    public AppPage getPage() {
        return this.page;
    }

    public AppError getErr() {
        return this.err;
    }

    public AppNotice getNotice() {
        return this.notice;
    }

    public AppStart getStart() {
        return this.start;
    }

    public List<AppDisplay> getDisplays() {
        return this.displays;
    }

    public List<AppAction> getActions() {
        return this.actions;
    }

    public void setTs(final Long ts) {
        this.ts = ts;
    }

    public void setCommon(final AppCommon common) {
        this.common = common;
    }

    public void setPage(final AppPage page) {
        this.page = page;
    }

    public void setErr(final AppError err) {
        this.err = err;
    }

    public void setNotice(final AppNotice notice) {
        this.notice = notice;
    }

    public void setStart(final AppStart start) {
        this.start = start;
    }

    public void setDisplays(final List<AppDisplay> displays) {
        this.displays = displays;
    }

    public void setActions(final List<AppAction> actions) {
        this.actions = actions;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AppMain)) {
            return false;
        } else {
            AppMain other = (AppMain)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                label107: {
                    Object this$ts = this.getTs();
                    Object other$ts = other.getTs();
                    if (this$ts == null) {
                        if (other$ts == null) {
                            break label107;
                        }
                    } else if (this$ts.equals(other$ts)) {
                        break label107;
                    }

                    return false;
                }

                Object this$common = this.getCommon();
                Object other$common = other.getCommon();
                if (this$common == null) {
                    if (other$common != null) {
                        return false;
                    }
                } else if (!this$common.equals(other$common)) {
                    return false;
                }

                Object this$page = this.getPage();
                Object other$page = other.getPage();
                if (this$page == null) {
                    if (other$page != null) {
                        return false;
                    }
                } else if (!this$page.equals(other$page)) {
                    return false;
                }

                label86: {
                    Object this$err = this.getErr();
                    Object other$err = other.getErr();
                    if (this$err == null) {
                        if (other$err == null) {
                            break label86;
                        }
                    } else if (this$err.equals(other$err)) {
                        break label86;
                    }

                    return false;
                }

                label79: {
                    Object this$notice = this.getNotice();
                    Object other$notice = other.getNotice();
                    if (this$notice == null) {
                        if (other$notice == null) {
                            break label79;
                        }
                    } else if (this$notice.equals(other$notice)) {
                        break label79;
                    }

                    return false;
                }

                label72: {
                    Object this$start = this.getStart();
                    Object other$start = other.getStart();
                    if (this$start == null) {
                        if (other$start == null) {
                            break label72;
                        }
                    } else if (this$start.equals(other$start)) {
                        break label72;
                    }

                    return false;
                }

                Object this$displays = this.getDisplays();
                Object other$displays = other.getDisplays();
                if (this$displays == null) {
                    if (other$displays != null) {
                        return false;
                    }
                } else if (!this$displays.equals(other$displays)) {
                    return false;
                }

                Object this$actions = this.getActions();
                Object other$actions = other.getActions();
                if (this$actions == null) {
                    if (other$actions != null) {
                        return false;
                    }
                } else if (!this$actions.equals(other$actions)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof AppMain;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $ts = this.getTs();
        result = result * 59 + ($ts == null ? 43 : $ts.hashCode());
        Object $common = this.getCommon();
        result = result * 59 + ($common == null ? 43 : $common.hashCode());
        Object $page = this.getPage();
        result = result * 59 + ($page == null ? 43 : $page.hashCode());
        Object $err = this.getErr();
        result = result * 59 + ($err == null ? 43 : $err.hashCode());
        Object $notice = this.getNotice();
        result = result * 59 + ($notice == null ? 43 : $notice.hashCode());
        Object $start = this.getStart();
        result = result * 59 + ($start == null ? 43 : $start.hashCode());
        Object $displays = this.getDisplays();
        result = result * 59 + ($displays == null ? 43 : $displays.hashCode());
        Object $actions = this.getActions();
        result = result * 59 + ($actions == null ? 43 : $actions.hashCode());
        return result;
    }

    public static class AppMainBuilder {
        private Long ts;
        private AppCommon common;
        private AppPage page;
        private AppError err;
        private AppNotice notice;
        private AppStart start;
        private List<AppDisplay> displays;
        private List<AppAction> actions;

        public void checkError() {
            Integer errorRate = AppConfig.error_rate;
            Boolean ifError = RandomOptionGroup.builder().add(true, errorRate).add(false, 100 - errorRate).build().getRandBoolValue();
            if (ifError) {
                AppError appError = AppError.build();
                this.err = appError;
            }

        }

        AppMainBuilder() {
        }

        public AppMain.AppMainBuilder ts(final Long ts) {
            this.ts = ts;
            return this;
        }

        public AppMain.AppMainBuilder common(final AppCommon common) {
            this.common = common;
            return this;
        }

        public AppMain.AppMainBuilder page(final AppPage page) {
            this.page = page;
            return this;
        }

        public AppMain.AppMainBuilder err(final AppError err) {
            this.err = err;
            return this;
        }

        public AppMain.AppMainBuilder notice(final AppNotice notice) {
            this.notice = notice;
            return this;
        }

        public AppMain.AppMainBuilder start(final AppStart start) {
            this.start = start;
            return this;
        }

        public AppMain.AppMainBuilder displays(final List<AppDisplay> displays) {
            this.displays = displays;
            return this;
        }

        public AppMain.AppMainBuilder actions(final List<AppAction> actions) {
            this.actions = actions;
            return this;
        }

        public AppMain build() {
            return new AppMain(this.ts, this.common, this.page, this.err, this.notice, this.start, this.displays, this.actions);
        }

        public String toString() {
            return "AppMain.AppMainBuilder(ts=" + this.ts + ", common=" + this.common + ", page=" + this.page + ", err=" + this.err + ", notice=" + this.notice + ", start=" + this.start + ", displays=" + this.displays + ", actions=" + this.actions + ")";
        }
    }
}
