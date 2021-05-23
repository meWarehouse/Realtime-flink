package com.at.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author zero
 * @create 2021-05-22 20:43
 *
 * 用该注解标记的属性，不需要插入到ClickHouse
 *
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
