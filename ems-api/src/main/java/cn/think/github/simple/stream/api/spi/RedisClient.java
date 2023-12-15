package cn.think.github.simple.stream.api.spi;

import java.util.concurrent.TimeUnit;

public interface RedisClient {

    AtomicLong getAtomicLong(String key);

    String get(String key);

    void delete(String key);

    void set(String key, String value);

    void set(String key, String value, int n, TimeUnit timeUnit);

    interface AtomicLong {

        long incrementAndGet();

        long incrementAndGet(long n);

        long get();
    }
}
