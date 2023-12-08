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
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.RetryMsgQueue.buildKey;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/11
 **/
@Slf4j
@Service
public class RetryMsgHandlerTask implements LifeCycle {

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
        subGroups.forEach(i -> retryMsgList.addAll(retryMsgMapper.selectRetryMsg(date, i)));

        List<Long> idList = retryMsgList.stream().map(BaseDO::getId).collect(Collectors.toList());
        if (idList.isEmpty()) {
            return;
        }
        // 更新状态
        updateSate(idList);

        // 根据线程繁忙程度进行排序, 不忙的靠前
        Map<String, List<RetryMsg>> map = new TreeMap<>((o1, o2) -> getPool(o1).getActiveCount() - getPool(o2).getActiveCount());

        // 根据 key 聚合 msg;
        retryMsgList.forEach(item -> {
            String key = buildKey(item.getOldTopicName(), item.getGroupName());
            List<RetryMsg> list = map.computeIfAbsent(key, s -> new ArrayList<>());
            list.add(item);
        });

        // 循环处理, 每个 key 对应一个 list<Msg> + pool + queue;
        // 当其中一个 queue 满了的时候,这个 pool 也将繁忙, 调用 execute 会出现阻塞;
        //    为了防止阻塞, 对 map 进行排序, 优先提交那些 pool 不繁忙的 key;
        //    但仍然存在某个 topic 有大量的重试消息, 可能阻塞整个主线程的可能行.
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
                log.warn("原始消息被删除, 无法找到重试消息体, topicName={}, offset={} createTime={} consumerTimes={}, nextConsumerTime={}",
                        oldTopicName, offset, item.getCreateTime(), item.getConsumerTimes(), item.getNextConsumerTime());
                RetryMsg retryMsg = new RetryMsg();
                retryMsg.setId(item.getId());
                retryMsg.setState(RetryMsg.STATE_SUCCESS);
                retryMsg.setUpdateTime(new Date());
                retryMsgMapper.updateById(retryMsg);
                return;
            }
            // 超过 {} 则会阻塞;
            try {
                SimpleMsgWrapper msgWrapper = SimpleMsgWrapper.builder()
                        .simpleMsg(simpleMsg)
                        .realTopic(item.getRetryTopicName())
                        .consumerTimes(item.getConsumerTimes())
                        .build();
                // 超时 2 s, 失败则回滚状态为 init.
                retryMsgQueue.getQueueMaps()
                        .computeIfAbsent(key, s -> new LinkedBlockingQueue<>(1024 * 10))// 一条消息 1kb, 1k 条, 1MB, 这里先放 10MB;
                        .offer(msgWrapper, 2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // 塞不进去, 只能回滚.
                item.setState(RetryMsg.STATE_INIT);
                item.setUpdateTime(new Date());
                retryMsgMapper.updateById(item);
                log.error("offer retry msg fail, key = {}", key);
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
                if (this.clientId == null) {
                    this.clientId = IPv4AddressUtil.get() + "@" + PIDUtil.get() + "@RetryTaskThread@" + UUID.randomUUID();
                    log.warn("-->> retry task renew client {}", this.clientId);
                }
                SpiFactory.getInstance().getObj(Broker.class).get().renew(
                        this.clientId, "system_retry", "system_retry"
                );
                LockFactory.get().lockAndExecute(() -> {
                    try {
                        pullSimpleMsg();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                    return null;
                }, getClass().getName(), 10);
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
        SpiFactory.getInstance().getObj(Broker.class).get().down(clientId);
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
}
