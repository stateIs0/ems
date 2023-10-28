package cn.think.github.simple.stream.redis.openSource.impl;

import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/19
 **/
@Component
public class DistributeLockFactory {

    @Resource
    RedissonClient redisClient;

    public DistributeLock getDistributeLock(String key) {
        return new DistributeLock(redisClient.getLock(key));
    }

}
