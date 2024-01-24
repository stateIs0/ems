package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.spi.RedisClient;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleGroupMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.TopicGroupLogMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl.RetryMsgWriteProcess;
import cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString;
import cn.think.github.simple.stream.mybatis.plus.impl.util.StringUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Service
public class LogService extends ServiceImpl<TopicGroupLogMapper, TopicGroupLog> {

    @Resource
    private RedisClient redisClient;
    @Resource
    private TopicGroupLogMapper topicGroupLogMapper;
    @Resource
    private SimpleGroupMapper simpleGroupMapper;
    @Resource
    private MsgService msgService;

    static String buildMaxLogOffsetKey(String topic, String group) {
        return String.format(RedisKeyFixString.GROUP_LOG_MAX_OFFSET_CACHE, topic, group);
    }

    public void setMaxLogOffset(String topic, String group, long offset) {
        redisClient.set(buildMaxLogOffsetKey(topic, group), String.valueOf(offset), 30, TimeUnit.SECONDS);
    }

    public synchronized void generateEmptyConsumerLog(String topic, String group, long offset, String clientId) {
        // redis
        this.setMaxLogOffset(topic, group, offset);
        // db
        List<TopicGroupLog> list = new ArrayList<>();
        TopicGroupLog topicGroupLog = new TopicGroupLog();
        topicGroupLog.setGroupName(group);
        topicGroupLog.setTopicName(topic);
        topicGroupLog.setState(TopicGroupLog.STATE_EMPTY);
        topicGroupLog.setPhysicsOffset(offset);
        topicGroupLog.setClientId(clientId);
        topicGroupLog.setErrorMsg("empty_log");
        topicGroupLog.setCreateTime(new Date());
        topicGroupLog.setUpdateTime(new Date());
        list.add(topicGroupLog);
        this.saveBatch(list);
    }

    public Long getLogMaxOffset(String t, String g) {

        String offsetString = redisClient.get(buildMaxLogOffsetKey(t, g));
        if (offsetString != null) {
            return Long.valueOf(offsetString);
        } else {
            TopicGroupLog topicGroupLog = topicGroupLogMapper.selectOne(
                    new QueryWrapper<TopicGroupLog>().lambda().
                            eq(TopicGroupLog::getTopicName, t).
                            eq(TopicGroupLog::getGroupName, g)
                            .orderByDesc(TopicGroupLog::getPhysicsOffset)
                            .last("limit 1"));
            if (topicGroupLog == null) {
                // 这里从 group 表里取出兜底记录;
                String lastConsumerOffset = simpleGroupMapper.selectLastOffset(g);
                if (StringUtil.isNotEmpty(lastConsumerOffset)) {
                    return Long.parseLong(lastConsumerOffset);
                }
                return -1L;
            } else {
                return topicGroupLog.getPhysicsOffset();
            }
        }
    }

    public TopicGroupLog getMinLog(String t, String g) {
        return topicGroupLogMapper.selectOne(new LambdaQueryWrapper<TopicGroupLog>()
                .eq(TopicGroupLog::getTopicName, t)
                .eq(TopicGroupLog::getGroupName, g)
                .orderBy(true, true, TopicGroupLog::getPhysicsOffset)
                .last("limit 1"));
    }


    public int updateOffset(String group, long offset) {
        TopicGroupLog topicGroupLog = new TopicGroupLog();
        topicGroupLog.setPhysicsOffset(offset);
        topicGroupLog.setUpdateTime(new Date());

        return topicGroupLogMapper.update(topicGroupLog, new LambdaQueryWrapper<TopicGroupLog>().eq(TopicGroupLog::getGroupName, group));
    }


    public void updateOffsetBatch(String t, String group, String clientId, int state, Msg msg) {
        String realTopic = msg.getRealTopic();
        if (RetryMsgWriteProcess.isGeneralTopic(realTopic)) {
            TopicGroupLog l = new TopicGroupLog();
            l.setUpdateTime(new Date());
            l.setState(state);
            Long dbId = msg.getDbId();
            if (dbId != null && dbId != 0) {
                l.setId(dbId);
                topicGroupLogMapper.updateById(l);
                return;
            }
            topicGroupLogMapper.update(l, new LambdaQueryWrapper<TopicGroupLog>()
                    .eq(TopicGroupLog::getState, TopicGroupLog.STATE_START)
                    .eq(TopicGroupLog::getTopicName, t)
                    .eq(TopicGroupLog::getGroupName, group)
                    .eq(TopicGroupLog::getClientId, clientId)
                    .eq(TopicGroupLog::getPhysicsOffset, msg.getOffset()));
        }

    }

}