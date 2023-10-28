package cn.think.github.simple.stream.redis.openSource.impl;

import cn.think.github.simple.stream.api.spi.RedisClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/3
 **/
@Component
public class NormalRedisClientSpiImpl implements RedisClient {


    private final SimpleCache<String, String> simpleCache = new SimpleCache<>(TimeUnit.SECONDS.toMillis(60));

    @Resource
    private Cache cache;

    @Override
    public AtomicLong getAtomicLong(String key) {
        return new MyAtomicLong(cache, key);
    }

    @Override
    public String get(String key) {
        String value = simpleCache.get(key);
        if (value == null) {
            String v = cache.get(key, String.class);
            return v;
        } else {
            return value;
        }
    }

    @Override
    public void set(String key, String value) {
        simpleCache.put(key, value);
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
        public long get() {
            Long l = cache.get(key, Long.class);
            if (l == null) {
                return 0;
            }
            return l;
        }
    }

    static class SimpleCache<K, V> {
        private final ConcurrentHashMap<K, V> cache;
        private final Map<K, Long> entryTimeMap = new ConcurrentHashMap<>();
        private final long expirationTimeInMilliseconds;

        public SimpleCache(long expirationTimeInMilliseconds) {
            this.cache = new ConcurrentHashMap<>();
            this.expirationTimeInMilliseconds = expirationTimeInMilliseconds;
        }

        public void put(K key, V value) {
            if (key == null || value == null) {
                return;
            }
            cache.put(key, value);
            entryTimeMap.put(key, System.currentTimeMillis());
        }

        public V get(K key) {
            V value = cache.get(key);
            if (value != null && isExpired(key)) {
                cache.remove(key);
                entryTimeMap.remove(key);
                return null;
            }
            return value;
        }

        public void remove(K key) {
            cache.remove(key);
        }

        public void clear() {
            cache.clear();
        }

        private boolean isExpired(K key) {
            long currentTime = System.currentTimeMillis();
            long entryTime = entryTimeMap.get(key);
            return currentTime - entryTime > expirationTimeInMilliseconds;
        }
    }
}
