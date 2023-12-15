package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.mybatis.plus.impl.QuartzStdScheduler;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.GroupClientTable;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.GroupClientTableMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.TopicGroupLogMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.timeout.ClientTimeoutChecker;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Date;
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
    @Value("${ems.consumer.timeout.inMin:30}")
    String emsConsumerTimeoutInMin;


    @PostConstruct
    public void init() {
        broker.resisterTask(this);
    }

    @Override
    public void checkAndClean() {
        try {
            List<GroupClientTable> groupClientTables = mapper.selectList(new LambdaQueryWrapper<>());

            groupClientTables.forEach(table -> {
                // 处理断开的 client 的消息;
                long time = table.getRenewTime().getTime();
                long now = System.currentTimeMillis();
                // X秒 断开续约
                boolean fail = false;
                String tip = "";
                if (now - time > TimeUnit.SECONDS.toMillis(60)) {
                    fail = true;
                    tip = "timeout";
                }

                if (table.getState() == GroupClientTable.state_down) {
                    fail = true;
                    tip = "down";
                }
                if (fail) {
                    if ("timeout".equals(tip)) {
                        log.warn("--->> [{}] client ..... {}, state = {}", tip, table.getClientId(), table.getState());
                    }
                    normalLogProcess(table, tip);
                    retryLogProcess(table, tip);
                    delete(table);
                }
            });
            // 有可能 client 没断开, 但是超时了, 这里要处理 timeout 的消息;
            normalTimeoutLogProcess();
            normalTimeoutRetryLogProcess();
        } catch (Throwable throwable) {
            log.warn(throwable.getMessage(), throwable);
        }
    }

    private void retryLogProcess(GroupClientTable table, String tip) {
        // 将这个 client 的重试消息更新状态.
        List<RetryMsg> retryMsgs = retryMsgMapper.selectList(new LambdaQueryWrapper<RetryMsg>()
                .eq(RetryMsg::getState, RetryMsg.STATE_PROCESSING)
                .eq(RetryMsg::getClientId, table.getClientId()));

        if (!retryMsgs.isEmpty()) {
            log.info("client {} {}, 捞取 retry log 消费消息 {} 条", table.getClientId(), tip, retryMsgs.size());
        }

        retryMsgs.forEach(retryMsg -> {
            retryMsg.setState(STATE_START);
            retryMsg.setUpdateTime(new Date());
            retryMsgMapper.updateById(retryMsg);
        });
    }

    private void normalTimeoutRetryLogProcess() {
        // 将超过 {X} 分钟没有消费的消息, 更新状态;
        List<RetryMsg> retryMsgList = retryMsgMapper.selectList(new LambdaQueryWrapper<RetryMsg>()
                .eq(RetryMsg::getState, RetryMsg.STATE_PROCESSING)
                .lt(RetryMsg::getUpdateTime, new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(Integer.parseInt(emsConsumerTimeoutInMin)))));


        if (!retryMsgList.isEmpty()) {
            log.info("捞取 retry log 超时消费消息 {} 条", retryMsgList.size());
        }

        retryMsgList.forEach(retryMsg -> {
            retryMsg.setState(STATE_START);
            retryMsg.setUpdateTime(new Date());
            retryMsgMapper.updateById(retryMsg);
        });
    }

    private void normalTimeoutLogProcess() {
        // 将超过 {X} 分钟没有消费的消息, 投递到重试队列
        List<TopicGroupLog> topicGroupLogs = logMapper.selectList(new LambdaQueryWrapper<TopicGroupLog>()
                .eq(TopicGroupLog::getState, STATE_START)
                .lt(TopicGroupLog::getUpdateTime, new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(Integer.parseInt(emsConsumerTimeoutInMin)))));


        if (!topicGroupLogs.isEmpty()) {
            log.info("捞取 log 超时消费消息 {} 条", topicGroupLogs.size());
        }

        saveAndUpdate(topicGroupLogs);
    }

    private void normalLogProcess(GroupClientTable table, String tip) {
        // 将这个 client 的未成功消息投递到重试队列
        List<TopicGroupLog> topicGroupLogs = logMapper.selectList(new LambdaQueryWrapper<TopicGroupLog>()
                .eq(TopicGroupLog::getState, STATE_START)
                .eq(TopicGroupLog::getClientId, table.getClientId()));

        if (!topicGroupLogs.isEmpty()) {
            log.info("client {} {}, 捞取 log 超时消费消息 {} 条", table.getClientId(), tip, topicGroupLogs.size());
        }

        saveAndUpdate(topicGroupLogs);
    }

    private void saveAndUpdate(List<TopicGroupLog> topicGroupLogs) {
        for (TopicGroupLog topicGroupLog : topicGroupLogs) {
            retryMsgHandler.saveRetry(topicGroupLog.getTopicName(), topicGroupLog.getTopicName(),
                    0, topicGroupLog.getGroupName(), topicGroupLog.getPhysicsOffset());
            topicGroupLog.setState(STATE_RETRY);
            topicGroupLog.setUpdateTime(new Date());
            logMapper.updateById(topicGroupLog);
        }
    }

    public void delete(GroupClientTable clientTable) {
        mapper.delete(new LambdaQueryWrapper<GroupClientTable>()
                .eq(GroupClientTable::getId, clientTable.getId()));
        log.debug("delete client {}", clientTable);
    }

    @Override
    public void start() {
        log.info("start ClientTimeoutChecker");
        quartzStdScheduler.tryStart(this.getClass(), this.getClass().getName()
                , "0/5 * * * * ? ", null);
    }

    @Override
    public void execute(JobExecutionContext context) {
        checkAndClean();
    }

    @Override
    public String toString() {
        return "ClientTimeoutCheckerImpl";
    }
}
