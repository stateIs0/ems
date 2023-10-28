package cn.think.github.simple.stream.mybatis.plus.impl.clean.impl;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/18
 **/
public class Cost {

    private long start;
    private long end;
    private long cost;

    public static Cost start() {
        Cost cost = new Cost();
        cost.start = System.currentTimeMillis();
        return cost;
    }

    public Cost end() {
        this.end = System.currentTimeMillis();
        this.cost = this.end - this.start;
        return this;
    }

    public long cost() {
        if (end == 0) {
            return -1;
        }
        return this.cost;
    }
}
