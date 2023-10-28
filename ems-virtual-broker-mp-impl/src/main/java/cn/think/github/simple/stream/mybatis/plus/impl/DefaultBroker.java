package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.StreamAdmin;
import cn.think.github.simple.stream.api.simple.util.EmsEmptyList;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.api.spi.StreamLock;
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
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
@Slf4j
@Service
public class DefaultBroker implements Broker {

    @Resource
    private MsgService msgService;
    @Resource
    private LogService logService;
    @Resource
    private StreamAdmin streamAdmin;
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
            offsetPair = (streamLock.lockAndExecute(() -> {
                // topic max
                Long max = msgService.getMsgMaxOffset(topicName);
                // group max
                Long offsetMax = logService.getMaxLogOffset(topicName, group);
                if (offsetMax >= max) {
                    return null;
                }

                long newOffset = 0L;
                long tmp;
                tmp = offsetMax + 1;
                List<TopicGroupLog> list = new ArrayList<>();
                int batchSize = emsSystemConfig.consumerBatchSize();
                for (int i = 0; i < (max - offsetMax > batchSize ? batchSize : max - offsetMax); i++) {
                    newOffset = offsetMax + 1;
                    offsetMax = newOffset;
                    TopicGroupLog db = new TopicGroupLog();
                    db.setGroupName(group);
                    db.setTopicName(topicName);
                    db.setState(TopicGroupLog.STATE_START);
                    db.setPhysicsOffset(newOffset);
                    db.setClientId(clientId);
                    db.setCreateTime(new Date());
                    db.setUpdateTime(new Date());
                    list.add(db);
                }

                logService.saveBatch(list);
                logService.setMaxLogOffset(topicName, group, newOffset);
                return new OffsetPair(tmp, newOffset);
            }, lockKey, timeoutInSec));
        } catch (LockFailException ignore) {
            // ignore
            return new ArrayList<>();
        } catch (Throwable e) {
            log.warn(e.getMessage(), e);
            return new ArrayList<>();
        }
        List<SimpleMsg> simpleMsgs = new ArrayList<>();
        if (offsetPair != null) {
            simpleMsgs = msgService.getBatch(topicName, offsetPair.min, offsetPair.max);
        }
        List<SimpleMsgWrapper> retryMsgList = retryMsgHandler.getRetryMsgList(topicName, group);

        if (simpleMsgs.isEmpty() && retryMsgList.isEmpty()) {
            if (!streamAdmin.existTopic(topicName)) {
                throw new RuntimeException("topicName 错误 " + topicName);
            } else {
                log.debug("0--->> {} {} {}", topicName, group, offsetPair);
                return new ArrayList<>();
            }
        }
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

        collect.addAll(simpleMsgs.stream().map(simpleMsg -> (Msg.builder()
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
            logService.updateOffsetBatch(topic, group, clientId, TopicGroupLog.STATE_RETRY,
                    badList.stream().map(Msg::getOffset).collect(Collectors.toList()));
        }
        if (!goodList.isEmpty()) {
            logService.updateOffsetBatch(topic, group, clientId, TopicGroupLog.STATE_DONE,
                    goodList.stream().map(Msg::getOffset).collect(Collectors.toList()));
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
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(propertiesString));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class OffsetPair {
        long min;
        long max;
    }
}
