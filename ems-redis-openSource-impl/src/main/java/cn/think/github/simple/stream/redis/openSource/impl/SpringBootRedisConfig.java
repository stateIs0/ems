package cn.think.github.simple.stream.redis.openSource.impl;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/19
 **/
@Configuration
public class SpringBootRedisConfig {

    /**
     * redis://127.0.0.1:6379
     */
    @Value("${ems.redisson.url}")
    private String url;

    @Value("${ems.redisson.pwd:}")
    private String pwd;

    @Value("${spring.redis.host:localhost}")
    private String host;
    @Value("${spring.redis.port:6379}")
    private int port;
    @Value("${spring.redis.password:}")
    private String password;
    @Value("${spring.redis.timeout:6000}")
    private int timeout;
    @Value("${spring.redis.jedis.pool.max-idle:1}")
    private int maxIdle;
    @Value("${spring.redis.jedis.pool.max-active:20}")
    private int maxActive;
    @Value("${spring.redis.jedis.pool.min-idle:1}")
    private int minIdle;

    @Bean
    public RedissonClient redisson() {

        Config config = new Config();
        SingleServerConfig singleServerConfig = config.useSingleServer().setAddress(url);
        if (pwd != null && !pwd.isEmpty()) {
            singleServerConfig.setPassword(pwd);
        }
        return Redisson.create(config);
    }

    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMaxTotal(maxActive);
        jedisPoolConfig.setMinIdle(minIdle);

        JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout, password);

        return jedisPool;

    }

    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);

        RedisSerializer genericJackson2JsonRedisSerializer = new GenericJackson2JsonRedisSerializer();

        redisTemplate.setKeySerializer(new GenericToStringSerializer<>(Object.class));
        redisTemplate.setValueSerializer(genericJackson2JsonRedisSerializer);

        redisTemplate.setHashKeySerializer(new GenericToStringSerializer<>(Object.class));
        redisTemplate.setHashValueSerializer(genericJackson2JsonRedisSerializer);
        // 设置支持事物
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
}
