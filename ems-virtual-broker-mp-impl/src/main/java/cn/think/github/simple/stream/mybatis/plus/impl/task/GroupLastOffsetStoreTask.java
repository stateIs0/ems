package cn.think.github.simple.stream.mybatis.plus.impl.task;

import cn.think.github.simple.stream.mybatis.plus.impl.QuartzStdScheduler;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleGroup;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleGroupMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.TopicGroupLogMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleGroup.GROUP_TYPE_CLUSTER;

/**
 * @Author cxs
 * @Description
 * @date 2023/12/28
 * @version 1.0
 **/
@Slf4j
@Component
public class GroupLastOffsetStoreTask implements Job {

    @Value("${ems.GroupLastOffsetStoreTask.cronExpression:0/1 * * * * ?}")
    private String cronExpression;

    @Resource
    private SimpleGroupMapper simpleGroupMapper;
    @Resource
    private TopicGroupLogMapper topicGroupLogMapper;
    @Resource
    private QuartzStdScheduler quartzStdScheduler;

    @PostConstruct
    public void init() {
        log.info("start GroupLastOffsetStoreTask... corn = {}", cronExpression);
        quartzStdScheduler.tryStart(this.getClass(), getClass().getName(), cronExpression, null);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        List<SimpleGroup> simpleGroups = simpleGroupMapper.selectList(new LambdaQueryWrapper<SimpleGroup>()
                .eq(SimpleGroup::getGroupType, GROUP_TYPE_CLUSTER));
        if (simpleGroups.isEmpty()) {
            return;
        }
        simpleGroups.forEach(i -> {
            String groupName = i.getGroupName();
            String topicName = i.getTopicName();
            TopicGroupLog topicGroupLog = topicGroupLogMapper.selectOne(
                    new QueryWrapper<TopicGroupLog>().lambda()
                            .eq(TopicGroupLog::getTopicName, topicName)
                            .eq(TopicGroupLog::getGroupName, groupName)
                            .orderByDesc(TopicGroupLog::getPhysicsOffset)
                            .last("limit 1"));
            if (topicGroupLog == null) {
                return;
            }
            Long physicsOffset = topicGroupLog.getPhysicsOffset();
            if (!String.valueOf(physicsOffset).equals(i.getLastOffset())) {
                i.setLastOffset(physicsOffset.toString());
                i.setUpdateTime(new Date());
                simpleGroupMapper.updateById(i);
            }
        });
    }
}
