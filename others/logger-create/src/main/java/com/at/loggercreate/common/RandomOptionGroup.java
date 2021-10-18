package com.at.loggercreate.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * @author zero
 * @create 2021-06-26 17:45
 */
public class RandomOptionGroup<T> {
    int totalWeight;
    List<RanOpt> optList;

    public static <T> RandomOptionGroup.Builder<T> builder() {
        return new RandomOptionGroup.Builder();
    }

    public RandomOptionGroup(String... values) {
        this.totalWeight = 0;
        this.optList = new ArrayList();
        String[] var2 = values;
        int var3 = values.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            String value = var2[var4];
            ++this.totalWeight;
            this.optList.add(new RanOpt(value, 1));
        }

    }

    public RandomOptionGroup(RanOpt<T>... opts) {
        this.totalWeight = 0;
        this.optList = new ArrayList();
        RanOpt[] var2 = opts;
        int var3 = opts.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            RanOpt opt = var2[var4];
            this.totalWeight += opt.getWeight();

            for(int i = 0; i < opt.getWeight(); ++i) {
                this.optList.add(opt);
            }
        }

    }

    public RandomOptionGroup(int trueWeight, int falseWeight) {
        this(new RanOpt(true, trueWeight), new RanOpt(false, falseWeight));
    }

    public RandomOptionGroup(String trueRate) {
        this(ParamUtil.checkRatioNum(trueRate), 100 - ParamUtil.checkRatioNum(trueRate));
    }

    public T getValue() {
        int i = (new Random()).nextInt(this.totalWeight);
        return (T) this.optList.get(i).getValue();
//        return ((RanOpt)this.optList.get(i)).getValue();
    }

    public RanOpt<T> getRandomOpt() {
        int i = (new Random()).nextInt(this.totalWeight);
        return (RanOpt)this.optList.get(i);
    }

    public String getRandStringValue() {
        int i = (new Random()).nextInt(this.totalWeight);
        return (String)((RanOpt)this.optList.get(i)).getValue();
    }

    public Integer getRandIntValue() {
        int i = (new Random()).nextInt(this.totalWeight);
        return (Integer)((RanOpt)this.optList.get(i)).getValue();
    }

    public Boolean getRandBoolValue() {
        int i = (new Random()).nextInt(this.totalWeight);
        return (Boolean)((RanOpt)this.optList.get(i)).getValue();
    }

    public static void main(String[] args) {
        RanOpt[] opts = new RanOpt[]{new RanOpt("zhang3", 20), new RanOpt("li4", 30), new RanOpt("wang5", 50)};
        RandomOptionGroup randomOptionGroup = new RandomOptionGroup(opts);

        for(int i = 0; i < 10; ++i) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }

    }

    public RandomOptionGroup(final int totalWeight, final List<RanOpt> optList) {
        this.totalWeight = 0;
        this.optList = new ArrayList();
        this.totalWeight = totalWeight;
        this.optList = optList;
    }

    public static class Builder<T> {
        List<RanOpt> optList = new ArrayList();
        int totalWeight = 0;

        public RandomOptionGroup.Builder add(T value, int weight) {
            RanOpt ranOpt = new RanOpt(value, weight);
            this.totalWeight += weight;

            for(int i = 0; i < weight; ++i) {
                this.optList.add(ranOpt);
            }

            return this;
        }

        public RandomOptionGroup<T> build() {
            return new RandomOptionGroup(this.totalWeight, this.optList);
        }

        Builder() {
        }

        public RandomOptionGroup.Builder<T> totalWeight(final int totalWeight) {
            this.totalWeight = totalWeight;
            return this;
        }

        public RandomOptionGroup.Builder<T> optList(final List<RanOpt> optList) {
            this.optList = optList;
            return this;
        }

        public String toString() {
            return "RandomOptionGroup.Builder(totalWeight=" + this.totalWeight + ", optList=" + this.optList + ")";
        }
    }
}
