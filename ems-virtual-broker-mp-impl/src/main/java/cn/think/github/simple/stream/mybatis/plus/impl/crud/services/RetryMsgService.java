package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Service
public class RetryMsgService extends ServiceImpl<RetryMsgMapper, RetryMsg> {

    static M[] ARR = new M[]{
            null,// 从 1 开始
            new M(1, 10),
            new M(2, 20),
            new M(3, 30),
            new M(4, TimeUnit.MINUTES.toSeconds(1)),
            new M(5, TimeUnit.MINUTES.toSeconds(2)),
            new M(6, TimeUnit.MINUTES.toSeconds(3)),
            new M(7, TimeUnit.MINUTES.toSeconds(4)),
            new M(8, TimeUnit.MINUTES.toSeconds(5)),
            new M(9, TimeUnit.MINUTES.toSeconds(6)),
            new M(10, TimeUnit.MINUTES.toSeconds(7)),
            new M(11, TimeUnit.MINUTES.toSeconds(8)),
            new M(12, TimeUnit.MINUTES.toSeconds(9)),
            new M(13, TimeUnit.MINUTES.toSeconds(10)),
            new M(14, TimeUnit.MINUTES.toSeconds(20)),
            new M(15, TimeUnit.MINUTES.toSeconds(30)),
            new M(16, TimeUnit.HOURS.toSeconds(1)),
            new M(17, TimeUnit.HOURS.toSeconds(1))
    };

    static M[] TEST_ARR = new M[]{
            null,// 从 1 开始
            new M(1, 3),
            new M(2, 5),
            new M(3, 2),
            new M(4, 2),
            new M(5, 2),
            new M(6, 2),
            new M(7, 2),
            new M(8, 2),
            new M(9, 2),
            new M(10, 2),
            new M(11, 2),
            new M(12, 2),
            new M(13, 1),
            new M(14, 1),
            new M(15, 1),
            new M(16, 1),
            new M(17, 3)
    };

    private final boolean test = Boolean.parseBoolean(System.getProperty("ems.test"));

    @SneakyThrows
    public int insert(String oldTopic, String newTopic/*"%RETRY%" + group*/, String group, Long offset, int consumerTimes) {

        List<RetryMsg> retryMsgs = baseMapper.selectList(new LambdaQueryWrapper<RetryMsg>()
                .eq(RetryMsg::getOldTopicName, oldTopic)
                .eq(RetryMsg::getRetryTopicName, newTopic)
                .eq(RetryMsg::getGroupName, group)
                .eq(RetryMsg::getOffset, offset));
        if (!CollectionUtils.isEmpty(retryMsgs)) {
            return 0;
        }

        RetryMsg retryMsg = new RetryMsg();
        retryMsg.setOldTopicName(oldTopic);
        retryMsg.setRetryTopicName(newTopic);
        retryMsg.setGroupName(group);
        retryMsg.setOffset(offset);
        retryMsg.setClientId("empty");
        retryMsg.setConsumerTimes(consumerTimes);
        retryMsg.setNextConsumerTime(getNext(consumerTimes));
        retryMsg.setCreateTime(new Date());
        retryMsg.setUpdateTime(new Date());
        int result = 0;
        while (result <= 0) {
            // 唯一索引 // offset + retryTopicName
            result = DuplicateKeyExceptionClosure.submit(() -> baseMapper.insert(retryMsg), 0);
        }
        return result;
    }

    @SneakyThrows
    public boolean updateForId(RetryMsg retryMsg) {
        return baseMapper.updateById(retryMsg) > 0;
    }

    public void update0(String topic, String group, Long offset, int consumerTimes, int state) {
        RetryMsg retryMsg = new RetryMsg();
        // "%RETRY%" + group
        retryMsg.setRetryTopicName(topic);
        retryMsg.setOffset(offset);
        retryMsg.setGroupName(group);
        retryMsg.setState(state);
        retryMsg.setConsumerTimes(consumerTimes);
        retryMsg.setNextConsumerTime(getNext(consumerTimes));
        retryMsg.setUpdateTime(new Date());
        baseMapper.update(retryMsg, new UpdateWrapper<RetryMsg>().lambda()
                .eq(RetryMsg::getRetryTopicName, topic)
                .eq(RetryMsg::getGroupName, group)
                .eq(RetryMsg::getOffset, offset));
    }

    @SneakyThrows
    public void update0(String topic, String group, Long offset, int consumerTimes) {
        update0(topic, group, offset, consumerTimes, RetryMsg.STATE_INIT);
    }

    private Date getNext(int consumerTimes) {
        long timeInSec = ARR[consumerTimes].getTimeInSec();
        if (test) {
            timeInSec = TEST_ARR[consumerTimes].getTimeInSec();
        }
        return new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeInSec));
    }

    @AllArgsConstructor
    @Data
    static class M {
        int timeNum;
        long timeInSec;
    }
}
