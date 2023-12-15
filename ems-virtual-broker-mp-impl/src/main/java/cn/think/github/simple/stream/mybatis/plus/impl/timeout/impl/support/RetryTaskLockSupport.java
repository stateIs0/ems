package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.support;

import cn.think.github.simple.stream.api.spi.RedisClient;
import cn.think.github.simple.stream.api.util.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString.EMS_RETRY_TASK_LOCK_MAIN_KEY;
import static cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString.EMS_RETRY_TASK_LOCK_SUPPORT_KEY;

/**
 * @Author cxs
 * @Description
 * @date 2023/12/14
 * @version 1.0
 **/
@Slf4j
@Service
public class RetryTaskLockSupport {

    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("RetryTaskLockSupport"));

    @Resource
    private RedisClient redisClient;
    @Value("${ems.RetryTaskLockSupport.InSec:600}")
    private String checkInSec;

    @PostConstruct
    public void init() {
        service.scheduleAtFixedRate(this::checkAndFix, 10, 10, TimeUnit.SECONDS);
    }

    public void recordRetryTask() {
        redisClient.set(EMS_RETRY_TASK_LOCK_SUPPORT_KEY, String.valueOf(System.currentTimeMillis()), 24, TimeUnit.HOURS);
    }

    public void checkAndFix() {
        try {
            String value = redisClient.get(EMS_RETRY_TASK_LOCK_SUPPORT_KEY);
            if (value == null) {
                bugFix();
                return;
            }

            long time = Long.parseLong(value);
            if (System.currentTimeMillis() - time > TimeUnit.SECONDS.toMillis(Integer.parseInt(checkInSec))) {
                bugFix();
            }
        } catch (Throwable throwable) {
            log.warn("RetryTaskLockSupport error, ignore", throwable);
        }
    }

    public void bugFix() {
        // 丢包场景下, 需要手动删除这个 key, 才可以恢复
        redisClient.delete(EMS_RETRY_TASK_LOCK_MAIN_KEY);

        log.warn("bug fix redisson lock bug.... delete key {}", EMS_RETRY_TASK_LOCK_MAIN_KEY);
    }

}
