package com.at.loggercreate.common;

/**
 * @author zero
 * @create 2021-06-26 17:46
 */
public class RanOpt<T> {
    T value;
    int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return this.value;
    }

    public int getWeight() {
        return this.weight;
    }
}
