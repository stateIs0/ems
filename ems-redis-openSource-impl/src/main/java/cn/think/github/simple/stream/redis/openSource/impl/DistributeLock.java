package cn.think.github.simple.stream.redis.openSource.impl;

import org.redisson.api.RLock;

import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/19
 **/
public class DistributeLock {

    private final RLock rLock;

    public DistributeLock(RLock rLock) {
        this.rLock = rLock;
    }

    public boolean tryLock(int waitTimeout, int leaseTime, TimeUnit timeUnit) throws InterruptedException {
        return rLock.tryLock(waitTimeout, leaseTime, timeUnit);
    }

    void unlock() {
        rLock.unlock();
    }

}