package cn.think.github.simple.stream.redis.openSource.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.BoundValueOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/19
 **/
@Slf4j
@Component
public class Cache {

    @Resource
    private RedisTemplate<String, String> redisTemplate;

    @Resource
    private ObjectMapper objectMapper;

    public void set(String key, String value, int num, TimeUnit timeUnit) {
        redisTemplate.boundValueOps(key).set(value, num, timeUnit);
    }

    public <T> T get(String key, Class<T> c) {
        Object v = null;
        try {
            v = redisTemplate.boundValueOps(key).get();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        if (v == null) {
            return (T) v;
        }
        if ((v instanceof String)) {
            return (T) v;
        }
        if (v != null) {
            try {
                return objectMapper.readValue(v.toString(), c);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public long increment(String key, long num) {
        BoundValueOperations<String, String> v = redisTemplate.boundValueOps(key);
        Long increment = v.increment(num);
        if (increment != null) {
            return increment;
        }
        return 0;
    }

    public void set(String key, long n) {

        BoundValueOperations<String, String> v = redisTemplate.boundValueOps(key);
        v.set(key, n);
    }
}
