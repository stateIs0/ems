package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.simple.util.EmsEmptyList;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.api.spi.StreamLock;
import cn.think.github.simple.stream.api.util.JsonUtil;
import cn.think.github.simple.stream.api.util.SpiFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.GroupClientTableService;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.LogService;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.MsgService;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockKeyFixString;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.SimpleMsgWrapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.RetryMsgWriteProcess;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DefaultBroker implements Broker {

    @Resource
    private MsgService msgService;
    @Resource
    private LogService logService;
    @Resource
    private GroupClientTableService groupClientTableService;
    @Resource
    private RetryMsgWriteProcess retryMsgHandler;
    @Resource
    private StreamLock streamLock;
    @Resource
    private EmsSystemConfig emsSystemConfig;

    private final Set<String> subGroup = new ConcurrentSkipListSet<>();

    private boolean once;

    private final Set<LifeCycle> lifeCycles = new CopyOnWriteArraySet<>();

    @PostConstruct
    public void init() {
        log.info("--------->>>> mp-broker start.....");
    }

    @Override
    public Set<String> getSubGroups() {
        return subGroup;
    }

    @Override
    public synchronized void resisterTask(LifeCycle lifeCycle) {
        log.warn("add lifeCycle {}", lifeCycle.getClass().getName());
        lifeCycles.add(lifeCycle);
    }

    @Override
    public void renew(String clientId, String groupName, String topicName) {
        if (!once) {
            start();
        }
        groupClientTableService.renew(clientId, groupName);
    }

    @Override
    public void down(String clientId) {
        if (!once) {
            start();
        }
        groupClientTableService.down(clientId);
    }

    public List<Msg> pullMsg(String topicName, long maxOffset) {
        if (!once) {
            start();
        }
        if (maxOffset < 0) {
            maxOffset = msgService.getMsgMaxOffset(topicName);
        }
        if (maxOffset >= msgService.getMsgMaxOffset(topicName)) {
            return new EmsEmptyList<>(maxOffset);
        }
        List<SimpleMsg> simpleMsgList = msgService.getBatch(topicName, maxOffset + 1, maxOffset + 20);
        return simpleMsgList.stream().map(simpleMsg -> (Msg.builder()
                .body(simpleMsg.getJsonBody())
                .offset(simpleMsg.getPhysicsOffset())
                .msgId(String.valueOf(simpleMsg.getPhysicsOffset()))
                .topic(simpleMsg.getTopicName())
                .realTopic(simpleMsg.getTopicName())
                .properties(load(simpleMsg.getProperties()))
                .tags(simpleMsg.getTags())
                .build())).collect(Collectors.toList());

    }

    @Override
    public List<Msg> pullMsg(String topicName, String group, String clientId, int timeoutInSec) {
        if (!once) {
            start();
        }
        subGroup.add(group);

        String lockKey = LockKeyFixString.getGroupOpKey(topicName, group);

        OffsetPair offsetPair;
        try {
            offsetPair = (streamLock.lockAndExecute(() -> insertMsgLog(topicName, group, clientId), lockKey, timeoutInSec));
        } catch (LockFailException ignore) {
            return new ArrayList<>();
        } catch (Throwable e) {
            log.warn(e.getMessage(), e);
            return new ArrayList<>();
        }
        List<SimpleMsg> msgArrayList = new ArrayList<>();
        if (offsetPair != null) {
            msgArrayList = getSimpleMsgList(topicName, offsetPair, group);
        }
        List<SimpleMsgWrapper> retryMsgList = retryMsgHandler.getRetryMsgList(topicName, group);

        if (msgArrayList.isEmpty() && retryMsgList.isEmpty()) {
            return new ArrayList<>();
        }

        return filterMsgList(retryMsgList, msgArrayList);
    }

    @NotNull
    private List<SimpleMsg> getSimpleMsgList(String topicName, OffsetPair offsetPair, String group) {
        List<SimpleMsg> msgArrayList;
        msgArrayList = msgService.getBatch(topicName, offsetPair.min, offsetPair.max);
        // 可能 redis 提交了, 但事务没提交, 这里不上锁, 兼容处理. 处理 N 次...
        int count = 0;
        while (((offsetPair.max - offsetPair.min) + 1) != msgArrayList.size()) {
            log.warn("msg size fall short of expectations....topic = {}, group = {}, ResultMsgSize = {}, offsetPair = {}",
                    topicName, group, msgArrayList.size(), offsetPair);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            msgArrayList = msgService.getBatch(topicName, offsetPair.min, offsetPair.max);

            if (++count > 20 && ((offsetPair.max - offsetPair.min) + 1) != msgArrayList.size()) {
                log.warn("消息和预期数量不一致, 可能是发送时出了问题, 请检查...");
                return msgArrayList;
            }
        }
        return msgArrayList;
    }


    @Override
    public boolean ack(List<Msg> list, String group, String clientId, int timeoutInSec) {
        if (!once) {
            start();
        }
        if (list.isEmpty()) {
            return true;
        }

        List<Msg> badList = list.stream().filter(Msg::isReceiveLater).collect(Collectors.toList());
        List<Msg> goodList = list.stream().filter(a -> !a.isReceiveLater()).collect(Collectors.toList());

        String topic = list.get(0).getTopic();

        if (!badList.isEmpty()) {
            retryMsgHandler.saveRetry(badList, group);
            logService.updateOffsetBatch(topic, group, clientId, TopicGroupLog.STATE_RETRY, filterNormalMsgOffset(badList));
        }
        if (!goodList.isEmpty()) {
            logService.updateOffsetBatch(topic, group, clientId, TopicGroupLog.STATE_DONE, filterNormalMsgOffset(goodList));
            retryMsgHandler.ackSuccessRetryMsgList(goodList);
        }

        return true;
    }


    @Override
    public String send(Msg msg, int timeoutInSec) {
        if (!once) {
            start();
            once = true;
        }
        String insert = msgService.insert(msg);
        log.debug("send msg {} {}", msg.getTopic(), insert);
        return insert;
    }

    @Override
    public synchronized void start() {
        // 定时检查
        if (once) {
            return;
        }
        for (LifeCycle lifeCycle : lifeCycles) {
            log.info("lifeCycle start {}", lifeCycle.getClass().getName());
            lifeCycle.start();
        }
        once = true;

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @Override
    public void stop() {
        log.warn("stop {}", lifeCycles);
        lifeCycles.forEach(LifeCycle::stop);
    }

    public Properties load(String propertiesString) {
        if (propertiesString == null) {
            return new Properties();
        }
        return SpiFactory.getInstance().getObj(JsonUtil.class)
                .get().read(propertiesString, Properties.class);
    }

    private List<Msg> filterMsgList(List<SimpleMsgWrapper> retryMsgList, List<SimpleMsg> msgArrayList) {
        List<Msg> collect = retryMsgList.stream()
                .filter(Objects::nonNull)
                .filter(a -> a.getSimpleMsg() != null)
                .map(simpleMsg -> (Msg.builder()
                        .body(simpleMsg.getSimpleMsg().getJsonBody())
                        .offset(simpleMsg.getSimpleMsg().getPhysicsOffset())
                        .msgId(String.valueOf(simpleMsg.getSimpleMsg().getPhysicsOffset()))
                        .topic(simpleMsg.getSimpleMsg().getTopicName())
                        .realTopic(simpleMsg.getRealTopic())
                        .consumerTimes(simpleMsg.getConsumerTimes())
                        .properties(load(simpleMsg.getSimpleMsg().getProperties()))
                        .tags(simpleMsg.getSimpleMsg().getTags())
                        .build())).collect(Collectors.toList());

        collect.addAll(msgArrayList.stream().map(simpleMsg -> (Msg.builder()
                .body(simpleMsg.getJsonBody())
                .offset(simpleMsg.getPhysicsOffset())
                .msgId(String.valueOf(simpleMsg.getPhysicsOffset()))
                .topic(simpleMsg.getTopicName())
                .realTopic(simpleMsg.getTopicName())
                .properties(load(simpleMsg.getProperties()))
                .tags(simpleMsg.getTags())
                .build())).collect(Collectors.toList()));
        return collect;
    }

    private OffsetPair insertMsgLog(String topicName, String group, String clientId) {
        // topic max
        Long msgMaxOffset = msgService.getMsgMaxOffset(topicName);
        if (msgMaxOffset < 0) {
            // no msg;
            return null;
        }
        // group max
        Long logOffsetMax = logService.getLogMaxOffset(topicName, group);
        // first consumption
        if (logOffsetMax < 0) {
            // first produced
            if (msgService.justProduced(topicName)) {
                logOffsetMax = 0L;
            } else {
                logOffsetMax = msgMaxOffset - 1;
            }
        }
        if (logOffsetMax >= msgMaxOffset) {
            return null;
        }

        final long min = logOffsetMax + 1;
        long lastMaxOffset = min;
        List<TopicGroupLog> list = new ArrayList<>();
        int batchSize = emsSystemConfig.consumerBatchSize();
        long m = (msgMaxOffset - logOffsetMax > batchSize ? batchSize : msgMaxOffset - logOffsetMax);
        for (int i = 0; i < m; i++) {
            lastMaxOffset = logOffsetMax + 1;
            logOffsetMax = lastMaxOffset;
            TopicGroupLog db = new TopicGroupLog();
            db.setGroupName(group);
            db.setTopicName(topicName);
            db.setState(TopicGroupLog.STATE_START);
            db.setPhysicsOffset(lastMaxOffset);
            db.setClientId(clientId);
            db.setCreateTime(new Date());
            db.setUpdateTime(new Date());
            list.add(db);
        }

        if (list.isEmpty()) {
            return null;
        }

        logService.saveBatch(list);
        logService.setMaxLogOffset(topicName, group, lastMaxOffset);
        return new OffsetPair(min, lastMaxOffset);
    }


    /**
     * 过滤消息(非重试消息),并 map 为 offset.
     */
    private List<Long> filterNormalMsgOffset(List<Msg> list) {
        return list.stream().filter(i -> !retryMsgHandler.isRetryTopic(i.getRealTopic())).map(Msg::getOffset).collect(Collectors.toList());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class OffsetPair {
        long min;
        long max;
    }
}
