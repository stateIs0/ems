package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.mybatis.plus.impl.QuartzStdScheduler;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.GroupClientTable;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.GroupClientTableMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.TopicGroupLogMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.ClientTimeoutChecker;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog.STATE_RETRY;
import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog.STATE_START;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Service
public class ClientTimeoutCheckerImpl implements ClientTimeoutChecker, LifeCycle, Job {

    @Resource
    GroupClientTableMapper mapper;
    @Resource
    Broker broker;
    @Resource
    RetryMsgWriteProcess retryMsgHandler;
    @Resource
    RetryMsgMapper retryMsgMapper;
    @Resource
    TopicGroupLogMapper logMapper;
    @Resource
    QuartzStdScheduler quartzStdScheduler;


    @PostConstruct
    public void init() {
        broker.resisterTask(this);
    }

    @Override
    public void checkAndClean() {
        try {
            List<GroupClientTable> groupClientTables = mapper.selectList(new LambdaQueryWrapper<>());

            for (GroupClientTable table : groupClientTables) {
                long time = table.getRenewTime().getTime();
                long now = System.currentTimeMillis();
                // 30s 断开续约
                if (now - time > TimeUnit.SECONDS.toMillis(30)
                        || table.getState() == GroupClientTable.state_down) {
                    log.warn("超时 or 下线 client ..... {}, state = {}", table.getClientId(), table.getState());
                    normalLogProcess(table);
                    retryLogProcess(table);
                    // bad
                    delete(table);
                }
            }
        } catch (Throwable throwable) {
            log.warn(throwable.getMessage(), throwable);
        }
    }

    private void retryLogProcess(GroupClientTable table) {
        // 将这个 client 的重试消息更新状态.
        List<RetryMsg> retryMsgs = retryMsgMapper.selectList(new LambdaQueryWrapper<RetryMsg>()
                .eq(RetryMsg::getState, STATE_RETRY)
                .eq(RetryMsg::getClientId, table.getClientId()));

        if (!retryMsgs.isEmpty()) {
            log.info("client {} 过期, 捞取 retry 超时消费消息 {} 条", table.getClientId(), retryMsgs.size());
        }

        for (RetryMsg retryMsg : retryMsgs) {
            retryMsg.setState(STATE_START);
            retryMsgMapper.updateById(retryMsg);
        }
    }

    private void normalLogProcess(GroupClientTable table) {
        // 将这个 client 的未成功消息投递到重试队列
        List<TopicGroupLog> topicGroupLogs = logMapper.selectList(new LambdaQueryWrapper<TopicGroupLog>()
                .eq(TopicGroupLog::getState, STATE_START)
                .eq(TopicGroupLog::getClientId, table.getClientId()));

        if (!topicGroupLogs.isEmpty()) {
            log.info("client {} 过期, 捞取 log 超时消费消息 {} 条", table.getClientId(), topicGroupLogs.size());
        }

        for (TopicGroupLog topicGroupLog : topicGroupLogs) {
            retryMsgHandler.saveRetry(topicGroupLog.getTopicName(), topicGroupLog.getTopicName(),
                    0, topicGroupLog.getGroupName(), topicGroupLog.getPhysicsOffset());
        }
    }

    public void delete(GroupClientTable clientTable) {
        mapper.delete(new LambdaQueryWrapper<GroupClientTable>()
                .eq(GroupClientTable::getId, clientTable.getId()));
    }

    @Override
    public boolean isHealth(String clientId) {
        List<GroupClientTable> groupClientTables =
                mapper.selectList(new LambdaQueryWrapper<GroupClientTable>().eq(GroupClientTable::getClientId, clientId));
        if (groupClientTables == null) {
            return false;
        }
        if (groupClientTables.isEmpty()) {
            return false;
        }
        for (GroupClientTable groupClientTable : groupClientTables) {
            long time = groupClientTable.getRenewTime().getTime();
            long now = System.currentTimeMillis();
            if (now - time > TimeUnit.SECONDS.toMillis(20)) {
                // bad
                return false;
            }
        }
        return true;
    }

    @Override
    public void start() {
        log.info("start");
        quartzStdScheduler.tryStart(this.getClass(), this.getClass().getName()
                , "0/5 * * * * ? ", null);
    }

    @Override
    public void stop() {

    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        checkAndClean();
    }

    @Override
    public String toString() {
        return "ClientTimeoutCheckerImpl{}";
    }
}
