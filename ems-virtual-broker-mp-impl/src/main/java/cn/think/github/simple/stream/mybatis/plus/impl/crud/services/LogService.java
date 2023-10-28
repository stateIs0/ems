package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.api.spi.RedisClient;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.TopicGroupLogMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
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
public class LogService extends ServiceImpl<TopicGroupLogMapper, TopicGroupLog> {

    @Resource
    private RedisClient redisClient;
    @Resource
    TopicGroupLogMapper topicGroupLogMapper;

    static String buildMaxLogOffsetKey(String topic, String group) {
        return topic + "$$__##__%%" + group;
    }

    public void setMaxLogOffset(String topic, String group, long offset) {
        redisClient.set(buildMaxLogOffsetKey(topic, group), String.valueOf(offset), 30, TimeUnit.SECONDS);
    }

    public Long getMaxLogOffset(String t, String g) {

        String s = redisClient.get(buildMaxLogOffsetKey(t, g));
        if (s != null) {
            return Long.valueOf(s);
        } else {
            // from {@code setMaxOffset(String topic, String group, long offset)}
            TopicGroupLog topicGroupLog = topicGroupLogMapper.selectOne(
                    new QueryWrapper<TopicGroupLog>().lambda().
                            eq(TopicGroupLog::getTopicName, t).
                            eq(TopicGroupLog::getGroupName, g)
                            .orderByDesc(TopicGroupLog::getPhysicsOffset)
                            .last("limit 1"));
            if (topicGroupLog == null) {
                return 0L;
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

        return baseMapper.update(topicGroupLog, new LambdaQueryWrapper<TopicGroupLog>().eq(TopicGroupLog::getGroupName, group));
    }


    public void updateOffsetBatch(String t, String group, String clientId, int state, List<Long> list) {
        if (list.isEmpty()) {
            return;
        }
        TopicGroupLog l = new TopicGroupLog();
        l.setUpdateTime(new Date());
        l.setState(state);
        baseMapper.update(l, new LambdaQueryWrapper<TopicGroupLog>()
                .eq(TopicGroupLog::getTopicName, t)
                .eq(TopicGroupLog::getGroupName, group)
                .eq(TopicGroupLog::getClientId, clientId)
                .in(TopicGroupLog::getPhysicsOffset, list));

    }

}