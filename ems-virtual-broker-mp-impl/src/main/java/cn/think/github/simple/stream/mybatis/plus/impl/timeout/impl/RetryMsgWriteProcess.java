package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.DeadMsgService;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.RetryMsgService;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.SimpleMsgWrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg.STATA_SUCCESS;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Service
public class RetryMsgWriteProcess {

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

    public boolean saveRetry(List<Msg> msg, String group) {
        msg.forEach(i -> saveRetry(i, group));
        return true;
    }

    public boolean saveRetry(Msg msg, String group) {
        // "%RETRY%" + group
        String realTopic = msg.getRealTopic();
        // TopicA
        String oldTopic = msg.getTopic();
        // 进入重试队列 or 死信队列
        int consumerTimes = msg.getConsumerTimes();

        return saveRetry(realTopic, oldTopic, consumerTimes, group, msg.getOffset());
    }

    public boolean saveRetry(String topic, String oldTopic, int consumerTimes, String group, Long offset) {
        // "%RETRY%" + group
        // 进入重试队列 or 死信队列
        if (!isRetryTopic(topic)) {
            try {
                return retryMsgService.insert(topic,
                        buildRetryTopic(group), group, offset, 0) > 0;
            } catch (Exception e) {
                throw e;
            }
        }
        int times = emsSystemConfig.consumerRetryMaxTimes();
        if (consumerTimes >= times) {
            // 死信
            deadMsgService.save(oldTopic, group, String.valueOf(offset), consumerTimes);
        }
        retryMsgService.update0(topic, group, offset, consumerTimes + 1);
        return true;
    }

    public List<SimpleMsgWrapper> getRetryMsgList(String topicName, String g) {
        return retryMsgQueue.getRetryMsgList(topicName, g);
    }

    public boolean ackSuccessRetryMsgList(List<Msg> list) {
        for (Msg msg : list) {
            long offset = msg.getOffset();
            String topic = msg.getRealTopic();
            if (!isRetryTopic(topic)) {
                continue;
            }
            List<RetryMsg> retryMsgList = retryMsgMapper.selectList(new LambdaQueryWrapper<RetryMsg>()
                    .eq(RetryMsg::getOffset, offset)
                    .eq(RetryMsg::getRetryTopicName, topic));
            if (retryMsgList == null) {
                log.warn("retryMsg is null {} {}", topic, offset);
                continue;
            }
            for (RetryMsg retryMsg : retryMsgList) {
                retryMsg.setState(STATA_SUCCESS);
                retryMsgMapper.updateById(retryMsg);
            }
        }
        return true;
    }

    public boolean isRetryTopic(String topic) {
        return topic.contains("%RETRY%");
    }

    public String buildRetryTopic(String group) {
        return "%RETRY%" + group;
    }

}
