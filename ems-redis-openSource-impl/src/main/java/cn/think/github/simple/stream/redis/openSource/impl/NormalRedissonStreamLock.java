package cn.think.github.simple.stream.redis.openSource.impl;

import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.api.spi.StreamLock;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
@Service
public class NormalRedissonStreamLock implements StreamLock {

    @Resource
    private DistributeLockFactory distributeLockFactory;

    @Override
    public <T> T lockAndExecute(Callback<T> callback, String key, int timeoutInSec) throws Throwable {
        return lockAndExecute(callback, key, timeoutInSec, -1);
    }

    @Override
    public <T> T lockAndExecute(Callback<T> callback, String key, int timeoutInSec, long leaseTimeInSec) throws Throwable {
        DistributeLock distributeLock = distributeLockFactory.getDistributeLock(key);
        boolean b;
        try {
            b = distributeLock.tryLock(timeoutInSec, (int) leaseTimeInSec, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new LockFailException("获取锁失败... key=" + key + ", timeout=" + timeoutInSec + ", " + e.getMessage());
        }
        if (b) {
            try {
                return callback.execute();
            } finally {
                distributeLock.unlock();
            }
        } else {
            throw new LockFailException("获取锁失败... key=" + key + ", timeout=" + timeoutInSec);
        }
    }


}
