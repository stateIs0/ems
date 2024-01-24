package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.DeadMsgService;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.DuplicateKeyExceptionClosure;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.RetryMsgService;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.SimpleMsgWrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg.STATE_DEAD;
import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg.STATE_SUCCESS;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Service
public class RetryMsgWriteProcess {

    public static final String RETRY_TOPIC_KEY_WORD = "%RETRY%";

    @Resource
    private DeadMsgService deadMsgService;
    @Resource
    private RetryMsgService retryMsgService;
    @Resource
    private RetryMsgMapper retryMsgMapper;
    @Resource
    private RetryMsgQueue retryMsgQueue;
    @Resource
    private EmsSystemConfig emsSystemConfig;

    public void saveRetry(Msg msg, String group) {
        // "%RETRY%" + group
        String realTopic = msg.getRealTopic();
        // TopicA
        String oldTopic = msg.getTopic();
        // 进入重试队列 or 死信队列
        int consumerTimes = msg.getConsumerTimes();

        saveRetry(realTopic, oldTopic, consumerTimes, group, msg.getOffset());
    }

    @SneakyThrows
    public void saveRetry(String topic, String oldTopic, int consumerTimes, String group, Long offset) {
        // "%RETRY%" + group
        // 进入重试队列 or 死信队列
        if (isGeneralTopic(topic)) {
            DuplicateKeyExceptionClosure.execute(() -> retryMsgService.insert(topic, buildRetryTopic(group), group, offset, 1));
            return;
        }
        int consumerRetryMaxTimes = emsSystemConfig.consumerRetryMaxTimes();
        if (consumerTimes >= consumerRetryMaxTimes) {
            // 死信
            deadMsgService.save(oldTopic, group, String.valueOf(offset), consumerTimes + 1);
            // fix
            retryMsgService.update0(topic, group, offset, consumerTimes + 1, STATE_DEAD);
        } else {
            retryMsgService.update0(topic, group, offset, consumerTimes + 1);
        }
    }

    public List<SimpleMsgWrapper> getRetryMsgList(String topicName, String groupName) {
        return retryMsgQueue.getRetryMsgList(topicName, groupName);
    }

    public void ackSuccessRetryMsgList(Msg msg) {
        long offset = msg.getOffset();
        String topic = msg.getRealTopic();
        if (isGeneralTopic(topic)) {
            return;
        }
        List<RetryMsg> retryMsgList = retryMsgMapper.selectList(new LambdaQueryWrapper<RetryMsg>()
                .eq(RetryMsg::getOffset, offset)
                .eq(RetryMsg::getRetryTopicName, topic));
        if (retryMsgList == null) {
            log.warn("retryMsg is null {} {}", topic, offset);
            return;
        }
        for (RetryMsg retryMsg : retryMsgList) {
            retryMsg.setState(STATE_SUCCESS);
            boolean success = retryMsgService.updateForId(retryMsg);
            log.debug("updateForId success = {}", success);
        }
    }

    /**
     * 普通 topic;
     */
    public static boolean isGeneralTopic(String topic) {
        return !topic.contains(RETRY_TOPIC_KEY_WORD);
    }

    public String buildRetryTopic(String group) {
        return RETRY_TOPIC_KEY_WORD + group;
    }

}
