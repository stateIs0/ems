package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.AllArgsConstructor;
import lombok.Data;
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

    static M[] arr = new M[]{
            new M(0, 10),
            new M(1, 20),
            new M(2, 30),
            new M(3, TimeUnit.MINUTES.toSeconds(1)),
            new M(4, TimeUnit.MINUTES.toSeconds(2)),
            new M(5, TimeUnit.MINUTES.toSeconds(3)),
            new M(6, TimeUnit.MINUTES.toSeconds(4)),
            new M(7, TimeUnit.MINUTES.toSeconds(5)),
            new M(8, TimeUnit.MINUTES.toSeconds(6)),
            new M(9, TimeUnit.MINUTES.toSeconds(7)),
            new M(10, TimeUnit.MINUTES.toSeconds(8)),
            new M(11, TimeUnit.MINUTES.toSeconds(9)),
            new M(12, TimeUnit.MINUTES.toSeconds(10)),
            new M(13, TimeUnit.MINUTES.toSeconds(20)),
            new M(14, TimeUnit.MINUTES.toSeconds(30)),
            new M(15, TimeUnit.HOURS.toSeconds(1)),
            new M(16, TimeUnit.HOURS.toSeconds(2))
    };

    static {
        Boolean test = Boolean.parseBoolean(System.getProperty("ems.test.env", "false"));
        if (test) {
            arr = new M[]{
                    new M(0, 1),
                    new M(1, 2),
                    new M(2, 3),
                    new M(3, 4),
                    new M(4, 5),
                    new M(5, 6),
                    new M(6, 7),
                    new M(7, 8),
                    new M(8, 9),
                    new M(9, 10),
                    new M(10, 11),
                    new M(11, 12),
                    new M(12, 13),
                    new M(13, 14),
                    new M(14, 15),
                    new M(15, 16),
                    new M(16, 17)
            };
        }
    }

    // 锁住.
    public synchronized int insert(String oldTopic, String newTopic/*"%RETRY%" + group*/, String group, Long offset, int consumerTimes) {

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
        return baseMapper.insert(retryMsg);
    }

    public void update0(String topic, String group, Long offset, int consumerTimes) {
        RetryMsg retryMsg = new RetryMsg();
        // "%RETRY%" + group
        retryMsg.setRetryTopicName(topic);
        retryMsg.setOffset(offset);
        retryMsg.setGroupName(group);
        retryMsg.setState(RetryMsg.STATA_INIT);
        retryMsg.setConsumerTimes(consumerTimes);
        retryMsg.setNextConsumerTime(getNext(consumerTimes));
        retryMsg.setUpdateTime(new Date());
        baseMapper.update(retryMsg, new UpdateWrapper<RetryMsg>().lambda()
                .eq(RetryMsg::getRetryTopicName, topic)
                .eq(RetryMsg::getGroupName, group)
                .eq(RetryMsg::getOffset, offset));
    }

    private Date getNext(int consumerTimes) {
        long timeInSec = arr[consumerTimes].getTimeInSec();
        return new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeInSec));
    }

    @AllArgsConstructor
    @Data
    static class M {
        int timeNum;
        long timeInSec;
    }
}
