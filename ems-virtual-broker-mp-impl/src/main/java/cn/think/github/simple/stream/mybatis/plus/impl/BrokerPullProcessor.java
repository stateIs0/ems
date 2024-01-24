package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.StreamAdmin;
import cn.think.github.simple.stream.api.simple.util.EmsEmptyList;
import cn.think.github.simple.stream.api.simple.util.TopicConstant;
import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.api.spi.StreamLock;
import cn.think.github.simple.stream.api.util.JsonUtil;
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

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * @Author cxs
 * @Description
 * @date 2023/12/7
 * @version 1.0
 **/
@Slf4j
@Service
public class BrokerPullProcessor {

    @Resource
    private MsgService msgService;
    @Resource
    private StreamLock streamLock;
    @Resource
    private LogService logService;
    @Resource
    private RetryMsgWriteProcess retryMsgWriteProcess;
    @Resource
    private EmsSystemConfig emsSystemConfig;
    @Resource
    private JsonUtil jsonUtil;
    @Resource
    private StreamAdmin streamAdmin;


    public List<Msg> pull(String topicName, String groupName, String clientId, int timeoutInSec) {

        int topicRule = streamAdmin.getTopicRule(topicName);
        int emsRule = streamAdmin.getEmsRule();
        if (topicRule > TopicConstant.RULE_READ || emsRule > TopicConstant.RULE_READ) {
            return new ArrayList<>();
        }

        List<SimpleMsgWrapper> retryMsgList = retryMsgWriteProcess.getRetryMsgList(topicName, groupName);

        String lockKey = LockKeyFixString.getGroupOpKey(topicName, groupName);
        OffsetPair offsetPair;

        try {
            offsetPair = (streamLock.lockAndExecute(() -> insertMsgLog(topicName, groupName, clientId,
                    retryMsgList.size() > 10), lockKey, timeoutInSec));
        } catch (LockFailException ignore) {
            return filterMsgList(retryMsgList, new ArrayList<>(), null);
        } catch (Throwable e) {
            log.warn(e.getMessage(), e);
            return filterMsgList(retryMsgList, new ArrayList<>(), null);
        }
        List<SimpleMsg> msgArrayList = new ArrayList<>();
        if (offsetPair != null) {
            msgArrayList = getSimpleMsgList(topicName, offsetPair, groupName, clientId);
        }

        if (msgArrayList.isEmpty() && retryMsgList.isEmpty()) {
            return new ArrayList<>();
        }

        Map<Long, Long> map = new HashMap<>();
        if (offsetPair != null) {
            map = offsetPair.getMap();
        }
        return filterMsgList(retryMsgList, msgArrayList, map);
    }

    public List<Msg> pullMsg(String topicName, long maxOffset) {
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

    public boolean ack(Msg msg, String group, String clientId) {

        String topic = msg.getTopic();
        if (msg.isReceiveLater()) {
            // fail
            retryMsgWriteProcess.saveRetry(msg, group);
            logService.updateOffsetBatch(topic, group, clientId, TopicGroupLog.STATE_RETRY, msg);
        } else {
            // success
            logService.updateOffsetBatch(topic, group, clientId, TopicGroupLog.STATE_DONE, msg);
            retryMsgWriteProcess.ackSuccessRetryMsgList(msg);
        }

        return true;
    }

    @NotNull
    private List<SimpleMsg> getSimpleMsgList(String topicName, OffsetPair offsetPair, String group, String clientId) {
        List<SimpleMsg> msgArrayList;
        msgArrayList = msgService.getBatch(topicName, offsetPair.min, offsetPair.max);
        // 由于是并发写入的, 则可能发生 10 写入成功 db&redis, 9 还没写入成功数据库.
        // 也可能是 9 在写入数据库的时候, 发生了异常. 如果是这样的话, 就没有这个消息;
        // 此时,我们将其放入到重试消息表里, 10s 后进行重试. 如果没有, 就忽略.
        int times = 0;
        int maxTimes = emsSystemConfig.emsBrokerPullMaxWaitTimes();
        while (((offsetPair.max - offsetPair.min) + 1) != msgArrayList.size() && times < maxTimes) {
            msgArrayList = msgService.getBatch(topicName, offsetPair.min, offsetPair.max);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            times++;
        }


        if (((offsetPair.max - offsetPair.min) + 1) == msgArrayList.size()) {
            return msgArrayList;
        }

        Set<Long> offsetCollect = msgArrayList.stream().map(SimpleMsg::getPhysicsOffset).collect(Collectors.toSet());

        for (long offset = offsetPair.min; offset <= offsetPair.max; offset++) {
            if (offsetCollect.contains(offset)) {
                continue;
            }
            // 没等到, 标记为 10s 重试;
            retryMsgWriteProcess.saveRetry(topicName, topicName, 1, group, offset);
            logService.updateOffsetBatch(topicName, group, clientId, TopicGroupLog.STATE_NOT_FOUND_WITH_RETRY,
                    Msg.builder().realTopic(topicName).dbId(offsetPair.map.get(offset)).offset(offset).build());
        }

        return msgArrayList;
    }

    private List<Msg> filterMsgList(List<SimpleMsgWrapper> retryMsgList, List<SimpleMsg> msgArrayList, Map<Long, Long> map) {
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
                .dbId(map != null ? map.get(simpleMsg.getPhysicsOffset()) : 0L)
                .build())).collect(Collectors.toList()));
        return collect;
    }

    /**
     * 1. 场景一: c 先启动, p 后启动, c 第一次 pull msg 时, 则将 offset 设置为 0, 返回空;
     *                             c 第二次 pull msg 时, 则从 offset 1 开始消费;
     * 2. 场景二: c 后启动, p 先启动, c pull msg 时, 设置当前的 offset 为 Max, 并忽略 c 启动前的消息, 只消费 c 启动后的消息(offset Max+);
     * 3.
     */
    private OffsetPair insertMsgLog(String topicName, String group, String clientId, boolean haveMoreRetry) {
        // topic max
        Long msgMaxOffset = msgService.getMsgMaxOffset(topicName);
        // group max
        Long logOffsetMax = logService.getLogMaxOffset(topicName, group);
        if (msgMaxOffset < 0 && logOffsetMax < 0) {
            // no msg & no log
            logService.generateEmptyConsumerLog(topicName, group, 0, clientId);
            return null;
        }
        if (msgMaxOffset < 0) {
            // no msg
            return null;
        }
        if (msgMaxOffset > 0 && logOffsetMax < 0) {
            // first consumer
            logService.generateEmptyConsumerLog(topicName, group, msgMaxOffset, clientId);
            return null;
        }
        if (logOffsetMax >= msgMaxOffset) {
            return null;
        }

        final long min = logOffsetMax + 1;
        long lastMaxOffset = min;
        List<TopicGroupLog> list = new ArrayList<>();
        int batchSize = emsSystemConfig.consumerBatchSize();
        if (haveMoreRetry && batchSize > 1) {
            batchSize = batchSize / 2;
        }
        // create msg log ↓↓↓↓↓↓↓
        long m = (msgMaxOffset - logOffsetMax > batchSize ? batchSize : msgMaxOffset - logOffsetMax);
        Map<Long, Long> map = new HashMap<>();
        for (int i = 0; i < m; i++) {
            lastMaxOffset = logOffsetMax + 1;
            logOffsetMax = lastMaxOffset;
            TopicGroupLog groupLog = createGroupLog(topicName, group, clientId, lastMaxOffset);
            list.add(groupLog);
            logService.save(groupLog);
            map.put(groupLog.getPhysicsOffset(), groupLog.getId());
        }

        if (list.isEmpty()) {
            return null;
        }

        logService.setMaxLogOffset(topicName, group, lastMaxOffset);
        return new OffsetPair(min, lastMaxOffset, map);
    }

    private Properties load(String propertiesString) {
        if (propertiesString == null) {
            return new Properties();
        }
        return jsonUtil.read(propertiesString, Properties.class);
    }

    public void cleanCache(String topic) {
        msgService.clean(topic);
    }

    @NotNull
    private static TopicGroupLog createGroupLog(String topicName, String group, String clientId, long lastMaxOffset) {
        TopicGroupLog db = new TopicGroupLog();
        db.setGroupName(group);
        db.setTopicName(topicName);
        db.setState(TopicGroupLog.STATE_START);
        db.setPhysicsOffset(lastMaxOffset);
        db.setClientId(clientId);
        db.setCreateTime(new Date());
        db.setUpdateTime(new Date());
        return db;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class OffsetPair {
        long min;
        long max;
        Map<Long, Long> map = new HashMap<>();
    }
}
