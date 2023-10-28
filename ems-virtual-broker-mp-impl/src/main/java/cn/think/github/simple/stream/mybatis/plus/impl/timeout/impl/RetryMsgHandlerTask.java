package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.simple.util.IPv4AddressUtil;
import cn.think.github.simple.stream.api.simple.util.PIDUtil;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.SimpleMsgWrapper;
import cn.think.github.spi.factory.NamedThreadFactory;
import cn.think.github.spi.factory.SpiFactory;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    ScheduledExecutorService service =
            new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("RetryMsgHandlerTask"));

    @Resource
    RetryMsgMapper retryMsgMapper;
    @Resource
    SimpleMsgMapper simpleMsgMapper;
    @Resource
    Broker broker;
    @Resource
    RetryMsgQueue retryMsgQueue;

    private String clientId;

    @PostConstruct
    public void init() {
        broker.resisterTask(this);
    }


    private void pullSimpleMsg() throws InterruptedException {
        // 1. 定时捞取临近消费时间的消息
        // 2. 将这些消息推送到 queue 中.
        Set<String> subGroups = broker.getSubGroups();
        List<RetryMsg> retryMsgList = new ArrayList<>();
        for (String subGroup : subGroups) {
            List<RetryMsg> l1 = retryMsgMapper.selectRetryMsg(new Date(), subGroup);
            retryMsgList.addAll(l1);
        }
        for (RetryMsg retryMsg : retryMsgList) {
            retryMsg.setState(RetryMsg.STATA_HANDLE);
            retryMsg.setUpdateTime(new Date());
            retryMsg.setClientId(this.clientId);
            retryMsgMapper.updateById(retryMsg);
        }

        for (RetryMsg item : retryMsgList) {
            String key = buildKey(item.getOldTopicName(), item.getGroupName());
            LinkedBlockingQueue<SimpleMsgWrapper> queue = retryMsgQueue.getQueueMaps().get(key);
            if (queue == null) {
                // {}, 防止内存溢出.
                queue = new LinkedBlockingQueue<>(2048);
                retryMsgQueue.getQueueMaps().put(key, queue);
            }
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
                retryMsg.setState(RetryMsg.STATA_SUCCESS);
                retryMsg.setUpdateTime(new Date());
                retryMsgMapper.updateById(retryMsg);
                continue;
            }
            // 超过 {} 则会阻塞;
            queue.put(new SimpleMsgWrapper(simpleMsg, item.getRetryTopicName(), item.getConsumerTimes()));
        }
    }


    @Override
    public void start() {
        service.scheduleAtFixedRate(() -> {
            try {
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
            } catch (Throwable e) {
                if (e instanceof LockFailException) {
                    // ignore
                } else {
                    log.warn(e.getMessage(), e);
                }
            }
        }, 1000, 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        SpiFactory.getInstance().getObj(Broker.class).get().down(clientId);
    }

    @Override
    public String toString() {
        return "RetryMsgHandlerTask{" +
                "clientId='" + clientId + '\'' +
                '}';
    }
}
