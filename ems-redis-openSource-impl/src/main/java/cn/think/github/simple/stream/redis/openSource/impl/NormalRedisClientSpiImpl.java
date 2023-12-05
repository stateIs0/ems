package cn.think.github.simple.stream.redis.openSource.impl;

import cn.think.github.simple.stream.api.spi.RedisClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/3
 **/
@Component
public class NormalRedisClientSpiImpl implements RedisClient {

    @Resource
    private Cache cache;

    @Override
    public AtomicLong getAtomicLong(String key) {
        return new MyAtomicLong(cache, key);
    }

    @Override
    public String get(String key) {
        return cache.get(key, String.class);
    }

    @Override
    public void set(String key, String value) {
        cache.set(key, value, 60, TimeUnit.SECONDS);
    }

    @Override
    public void set(String key, String value, int n, TimeUnit timeUnit) {
        cache.set(key, value, n, timeUnit);
    }

    public static class MyAtomicLong implements AtomicLong {

        Cache cache;

        String key;

        public MyAtomicLong(Cache cache, String key) {
            this.cache = cache;
            this.key = key;
        }

        @Override
        public long incrementAndGet() {
            return cache.increment(key, 1);
        }

        @Override
        public long incrementAndGet(long n) {
            return cache.increment(key, n);
        }

        @Override
        public long get() {
            Long l = cache.get(key, Long.class);
            if (l == null) {
                return 0;
            }
            return l;
        }
    }
}
