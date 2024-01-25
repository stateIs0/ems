package cn.think.github.simple.stream.mybatis.plus.impl.task;

import cn.think.github.simple.stream.mybatis.plus.impl.QuartzStdScheduler;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleTopic;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleTopicMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
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

/**
 * @Author cxs
 * @Description
 * @date 2023/12/28
 * @version 1.0
 **/
@Slf4j
@Component
public class TopicLastOffsetStoreTask implements Job {

    @Value("${ems.TopicLastOffsetStoreTask.cronExpression:0/1 * * * * ?}")
    private String cronExpression;

    @Resource
    private SimpleTopicMapper simpleTopicMapper;
    @Resource
    private SimpleMsgMapper simpleMsgMapper;
    @Resource
    private QuartzStdScheduler quartzStdScheduler;

    @PostConstruct
    public void init() {
        log.info("start TopicLastOffsetStoreTask... corn = {}", cronExpression);
        quartzStdScheduler.tryStart(this.getClass(), getClass().getName(), cronExpression, null);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        List<SimpleTopic> simpleGroups = simpleTopicMapper.selectList(new LambdaQueryWrapper<>());
        if (simpleGroups.isEmpty()) {
            return;
        }
        simpleGroups.forEach(i -> {
            String topicName = i.getTopicName();
            SimpleMsg simpleMsg = simpleMsgMapper.selectOne(new LambdaQueryWrapper<SimpleMsg>()
                    .eq(SimpleMsg::getTopicName, topicName)
                    .orderByDesc(SimpleMsg::getPhysicsOffset)
                    .last("limit 1"));
            if (simpleMsg == null) {
                return;
            }
            Long physicsOffset = simpleMsg.getPhysicsOffset();
            if (!String.valueOf(physicsOffset).equals(i.getLastOffset())) {
                i.setLastOffset(physicsOffset.toString());
                i.setUpdateTime(new Date());
                simpleTopicMapper.updateById(i);
            }
        });
    }
}
