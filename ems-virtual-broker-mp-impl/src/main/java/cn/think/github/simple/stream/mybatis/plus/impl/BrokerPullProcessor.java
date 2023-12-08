package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.simple.util.EmsEmptyList;
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
    private RetryMsgWriteProcess retryMsgHandler;
    @Resource
    private EmsSystemConfig emsSystemConfig;
    @Resource
    private JsonUtil jsonUtil;

    public List<Msg> pull(String topicName, String groupName, String clientId, int timeoutInSec) {
        String lockKey = LockKeyFixString.getGroupOpKey(topicName, groupName);

        OffsetPair offsetPair;

        try {
            offsetPair = (streamLock.lockAndExecute(() -> insertMsgLog(topicName, groupName, clientId), lockKey, timeoutInSec));
        } catch (LockFailException ignore) {
            return new ArrayList<>();
        } catch (Throwable e) {
            log.warn(e.getMessage(), e);
            return new ArrayList<>();
        }
        List<SimpleMsg> msgArrayList = new ArrayList<>();
        if (offsetPair != null) {
            msgArrayList = getSimpleMsgList(topicName, offsetPair, groupName);
        }
        List<SimpleMsgWrapper> retryMsgList = retryMsgHandler.getRetryMsgList(topicName, groupName);

        if (msgArrayList.isEmpty() && retryMsgList.isEmpty()) {
            return new ArrayList<>();
        }

        return filterMsgList(retryMsgList, msgArrayList);
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

    public boolean ack(List<Msg> list, String group, String clientId, int timeoutInSec) {
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

    /**
     * 1. 场景一: c 先启动, p 后启动, c 第一次 pull msg 时, 则将 offset 设置为 0, 返回空;
     *                             c 第二次 pull msg 时, 则从 offset 1 开始消费;
     * 2. 场景二: c 后启动, p 先启动, c pull msg 时, 设置当前的 offset 为 Max, 并忽略 c 启动前的消息, 只消费 c 启动后的消息(offset Max+);
     * 3.
     */
    private OffsetPair insertMsgLog(String topicName, String group, String clientId) {
        // topic max
        Long msgMaxOffset = msgService.getMsgMaxOffset(topicName);
        // group max
        Long logOffsetMax = logService.getLogMaxOffset(topicName, group);
        if (msgMaxOffset < 0 && logOffsetMax < 0) {
            // no msg & no log
            logService.setMaxLogOffset(topicName, group, 0);
            return null;
        }
        if (msgMaxOffset < 0) {
            // no msg
            return null;
        }
        if (msgMaxOffset > 0 && logOffsetMax < 0) {
            // first consumer
            logService.setMaxLogOffset(topicName, group, msgMaxOffset);
            return null;
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

    private Properties load(String propertiesString) {
        if (propertiesString == null) {
            return new Properties();
        }
        return jsonUtil.read(propertiesString, Properties.class);
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
