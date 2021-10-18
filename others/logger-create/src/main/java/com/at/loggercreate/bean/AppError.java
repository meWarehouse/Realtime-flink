package com.at.loggercreate.bean;

import com.at.loggercreate.common.RandomNum;
import java.net.ConnectException;
/**
 * @author zero
 * @create 2021-06-26 17:52
 */
public class AppError {
    Integer error_code;
    String msg;

    public static AppError build() {
        int errorCode = RandomNum.getRandInt(1001, 4001);
        String msg = " Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.log.bean.AppError.main(AppError.java:xxxxxx)";
        return new AppError(errorCode, msg);
    }

    public static void main(String[] args) throws Exception {
        throw new ConnectException();
    }

    public Integer getError_code() {
        return this.error_code;
    }

    public String getMsg() {
        return this.msg;
    }

    public void setError_code(final Integer error_code) {
        this.error_code = error_code;
    }

    public void setMsg(final String msg) {
        this.msg = msg;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AppError)) {
            return false;
        } else {
            AppError other = (AppError)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$error_code = this.getError_code();
                Object other$error_code = other.getError_code();
                if (this$error_code == null) {
                    if (other$error_code != null) {
                        return false;
                    }
                } else if (!this$error_code.equals(other$error_code)) {
                    return false;
                }

                Object this$msg = this.getMsg();
                Object other$msg = other.getMsg();
                if (this$msg == null) {
                    if (other$msg != null) {
                        return false;
                    }
                } else if (!this$msg.equals(other$msg)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof AppError;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $error_code = this.getError_code();
        result = result * 59 + ($error_code == null ? 43 : $error_code.hashCode());
        Object $msg = this.getMsg();
        result = result * 59 + ($msg == null ? 43 : $msg.hashCode());
        return result;
    }

    public String toString() {
        return "AppError(error_code=" + this.getError_code() + ", msg=" + this.getMsg() + ")";
    }

    public AppError(final Integer error_code, final String msg) {
        this.error_code = error_code;
        this.msg = msg;
    }
}
