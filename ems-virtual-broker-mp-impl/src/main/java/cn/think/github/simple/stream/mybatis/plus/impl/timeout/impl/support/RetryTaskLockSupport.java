//package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.support;
//
//import cn.think.github.simple.stream.api.spi.Broker;
//import cn.think.github.simple.stream.api.spi.RedisClient;
//import cn.think.github.simple.stream.api.util.NamedThreadFactory;
//import cn.think.github.simple.stream.api.util.SpiFactory;
//import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
//import cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.RetryMsgHandlerTask;
//import cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString;
//import cn.think.github.simple.stream.mybatis.plus.impl.util.StringUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//import javax.annotation.PostConstruct;
//import javax.annotation.Resource;
//import java.util.Date;
//import java.util.List;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//
//
///**
// * @Author cxs
// * @Description
// * @date 2023/12/14
// * @version 1.0
// **/
//@Slf4j
//@Service
//public class RetryTaskLockSupport {
//
//    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("RetryTaskLockSupport"));
//
//    @Resource
//    private RedisClient redisClient;
//    @Value("${ems.RetryTaskLockSupport.InSec:60}")
//    private String checkInSec;
//    @Value("${ems.RetryTaskLockSupport.fixStrategy:DELETE}")
//    private String fixStrategy;
//    @Resource
//    private RetryMsgMapper retryMsgMapper;
//
//    @PostConstruct
//    public void init() {
//        service.scheduleAtFixedRate(this::checkAndFix, 60, 10, TimeUnit.SECONDS);
//    }
//
//    public void recordRetryTaskExit(String group, long time) {
//        redisClient.set(String.format(RedisKeyFixString.EMS_RETRY_TASK_LOCK_SUPPORT_KEY, group), String.valueOf(time), 24, TimeUnit.HOURS);
//    }
//
//    public void checkAndFix() {
//        try {
//            if (Boolean.parseBoolean(System.getProperty("ems.RetryTaskLockSupport.enable", "false"))) {
//                return;
//            }
//            Broker broker = SpiFactory.getInstance().getObj(Broker.class).get();
//            List<String> subGroups = broker.getSubGroups().stream().filter(group -> StringUtil.notSame(group, RetryMsgHandlerTask.SYSTEM_RETRY)).collect(Collectors.toList());
//            subGroups.forEach(groupName -> {
//                String mainLey = String.format(RedisKeyFixString.EMS_RETRY_TASK_LOCK_MAIN_KEY, groupName);
//                String supportLey = String.format(RedisKeyFixString.EMS_RETRY_TASK_LOCK_SUPPORT_KEY, groupName);
//                String value = redisClient.get(supportLey);
//                if (value == null) {
//                    return;
//                }
//
//                long time = Long.parseLong(value);
//                // 很久不更新了, 怀疑是丢包导致死锁
//                if (System.currentTimeMillis() - time > TimeUnit.SECONDS.toMillis(Integer.parseInt(checkInSec))) {
//                    // check db, 性能问题?
//                    long count = retryMsgMapper.count(new Date(), groupName);
//                    if (count > 0) {
//                        bugFix(mainLey);
//                    }
//                }
//            });
//
//        } catch (Throwable throwable) {
//            log.warn("RetryTaskLockSupport error, ignore", throwable);
//        }
//    }
//
//    public void bugFix(String mainLey) {
//
//        log.error("before bug fix redisson lock bug.... delete key {}, fixStrategy = {}", mainLey, fixStrategy);
//        // 丢包场景下, 需要删除这个 key, 才可以恢复
//        redisClient.delete(mainLey);
//
//        log.error("after bug fix redisson lock bug.... delete key {}", mainLey);
//    }
//
//}
