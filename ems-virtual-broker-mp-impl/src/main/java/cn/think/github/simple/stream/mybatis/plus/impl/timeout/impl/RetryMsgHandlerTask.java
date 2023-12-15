package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.simple.util.IPv4AddressUtil;
import cn.think.github.simple.stream.api.simple.util.PIDUtil;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.api.util.NamedThreadFactory;
import cn.think.github.simple.stream.api.util.SpiFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.SimpleMsgWrapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.support.RetryTaskLockSupport;
import cn.think.github.simple.stream.mybatis.plus.impl.util.StringUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.RetryMsgQueue.buildKey;
import static cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString.EMS_RETRY_TASK_LOCK_MAIN_KEY;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/11
 **/
@Slf4j
@Service
public class RetryMsgHandlerTask implements LifeCycle {

    private static final int QUEUE_SIZE =
            Integer.parseInt(System.getProperty("ems.memory.retry.queue.size", "10240"));

    private static final String SYSTEM_RETRY = "system_retry";

    private static final int SERVICE_CORE = 1;
    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(SERVICE_CORE, new NamedThreadFactory("RetryMsgHandlerTask"));
    private final Map<String, ThreadPoolExecutor> poolManager = new ConcurrentHashMap<>();

    @Resource
    private RetryMsgMapper retryMsgMapper;
    @Resource
    private SimpleMsgMapper simpleMsgMapper;
    @Resource
    private Broker broker;
    @Resource
    private RetryMsgQueue retryMsgQueue;
    @Resource
    private RetryTaskLockSupport retryTaskLockSupport;

    private boolean running;

    private String clientId;

    @PostConstruct
    public void init() {
        broker.resisterTask(this);
    }


    private void pullSimpleMsg() throws InterruptedException {
        // 1. 定时捞取临近消费时间的消息
        // 2. 将这些消息推送到 queue 中.

        // 只处理当前 jvm 的 group.
        Set<String> subGroups = broker.getSubGroups();
        List<RetryMsg> retryMsgList = new ArrayList<>();

        // 从数据库捞取消息
        Date date = new Date();
        subGroups.stream()
                .filter(i -> StringUtil.notSame(i, SYSTEM_RETRY))
                .forEach(i -> retryMsgList.addAll(retryMsgMapper.selectRetryMsg(date, i)));

        List<Long> idList = retryMsgList.stream().map(BaseDO::getId).collect(Collectors.toList());
        if (idList.isEmpty()) {
            return;
        }
        // 更新状态
        updateSate(idList);

        Map<String, List<RetryMsg>> map = new HashMap<>();

        // 根据 key 聚合 msg;
        retryMsgList.forEach(item -> {
            String key = buildKey(item.getOldTopicName(), item.getGroupName());
            List<RetryMsg> list = map.computeIfAbsent(key, s -> new ArrayList<>());
            list.add(item);
        });

        // 循环处理, 每个 key 对应一个 list<Msg> + pool + queue;
        // 当其中一个 queue 满了的时候,这个 pool 也将繁忙, 调用 execute 会出现阻塞;
        map.forEach(this::process);
    }

    private void updateSate(List<Long> idList) {
        RetryMsg retryMsg = new RetryMsg();
        retryMsg.setState(RetryMsg.STATE_PROCESSING);
        retryMsg.setUpdateTime(new Date());
        retryMsg.setClientId(this.clientId);

        retryMsgMapper.update(retryMsg, new LambdaQueryWrapper<RetryMsg>()
                .in(RetryMsg::getId, idList));
    }

    private void process(String key, List<RetryMsg> list) {
        getPool(key).execute(() -> list.forEach(item -> {
            Long offset = item.getOffset();
            String oldTopicName = item.getOldTopicName();

            SimpleMsg simpleMsg = simpleMsgMapper.selectOne(new LambdaQueryWrapper<SimpleMsg>()
                    .eq(SimpleMsg::getTopicName, oldTopicName)
                    .eq(SimpleMsg::getPhysicsOffset, offset));
            if (simpleMsg == null) {
                log.info("原始消息被删除, 无法找到重试消息体, topicName={}, offset={} createTime={} consumerTimes={}, nextConsumerTime={}",
                        oldTopicName, offset, item.getCreateTime(), item.getConsumerTimes(), item.getNextConsumerTime());
                RetryMsg retryMsg = new RetryMsg();
                retryMsg.setId(item.getId());
                retryMsg.setState(RetryMsg.STATE_SUCCESS);
                retryMsg.setUpdateTime(new Date());
                retryMsgMapper.updateById(retryMsg);
                return;
            }
            SimpleMsgWrapper msgWrapper = SimpleMsgWrapper.builder()
                    .simpleMsg(simpleMsg)
                    .realTopic(item.getRetryTopicName())
                    .consumerTimes(item.getConsumerTimes())
                    .build();
            // 失败则回滚状态为 init.
            boolean offered = retryMsgQueue.getQueueMaps()
                    .computeIfAbsent(key, s -> new LinkedBlockingQueue<>(QUEUE_SIZE))// 一条消息 1kb, 1k 条, 1MB, 这里先放 10MB;
                    .offer(msgWrapper);
            if (!offered) {
                // 塞不进去, 只能回滚.
                item.setState(RetryMsg.STATE_INIT);
                item.setUpdateTime(new Date());
                retryMsgMapper.updateById(item);
                RetryMsgHandlerTask.specialRulesLogPrint(key, offset);
            }
        }));
    }


    @Override
    public void start() {
        service.scheduleWithFixedDelay(() -> {
            try {
                if (!running) {
                    return;
                }
                Set<String> subGroups = broker.getSubGroups();
                // 没有订阅者, 无需尝试重试.
                if (subGroups.isEmpty()) {
                    return;
                }
                if (this.clientId == null) {
                    this.clientId = IPv4AddressUtil.get() + "@" + PIDUtil.get() + "@RetryTaskThread@" + UUID.randomUUID();
                    log.warn("-->> retry task renew client {}", this.clientId);
                }
                SpiFactory.getInstance().getObj(Broker.class).get().renew(
                        this.clientId, SYSTEM_RETRY, SYSTEM_RETRY
                );
                LockFactory.get().lockAndExecute(() -> {
                    try {
                        retryTaskLockSupport.recordRetryTask();
                        pullSimpleMsg();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                    return null;
                }, EMS_RETRY_TASK_LOCK_MAIN_KEY, 10, 60);
            } catch (LockFailException lockFailException) {
                log.warn(lockFailException.getMessage());
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        }, 1000, 500, TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        Optional.ofNullable(clientId).ifPresent(i -> SpiFactory.getInstance().getObj(Broker.class).get().down(clientId));
    }

    @Override
    public String toString() {
        return "RetryMsgHandlerTask{" +
                "clientId='" + clientId + '\'' +
                '}';
    }

    private ThreadPoolExecutor getPool(String key) {
        return poolManager.computeIfAbsent(key, s -> new ThreadPoolExecutor(1,
                2, 10, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("retryTask-" + key),
                new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    private static long PRE_TIME = 0;

    public static void specialRulesLogPrint(String key, long offset) {
        if (System.currentTimeMillis() - PRE_TIME > TimeUnit.SECONDS.toMillis(3)) {
            log.warn("offer retry msg to queue fail, key = {}, offset = {}, QUEUE_SIZE = {}", key, offset, QUEUE_SIZE);

            PRE_TIME = System.currentTimeMillis();
        }
    }
}
