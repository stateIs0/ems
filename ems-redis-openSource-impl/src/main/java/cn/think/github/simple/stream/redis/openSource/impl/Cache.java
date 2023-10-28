package cn.think.github.simple.stream.redis.openSource.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
@Component
public class Cache {

    @Resource
    RedisTemplate<String, String> redisTemplate;

    @Resource
    ObjectMapper objectMapper;

    public void set(String key, String value, int num, TimeUnit timeUnit) {
        redisTemplate.boundValueOps(key).set(value, num, timeUnit);
    }

    public <T> T get(String key, Class<T> c) {
        Object v = redisTemplate.boundValueOps(key).get();
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

    public long increment(String key, int num) {
        BoundValueOperations<String, String> v = redisTemplate.boundValueOps(key);
        Long increment = v.increment(num);
        if (increment != null) {
            return increment;
        }
        return 0;
    }
}
