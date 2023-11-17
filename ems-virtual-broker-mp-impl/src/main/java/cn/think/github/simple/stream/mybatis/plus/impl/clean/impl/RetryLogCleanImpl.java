package cn.think.github.simple.stream.mybatis.plus.impl.clean.impl;

import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.RetryMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.TopicGroupLogMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/10
 **/
@Slf4j
@Service
public class RetryLogCleanImpl extends BaseMsgClean {

    // 0 0/3 * * * ?
    // 3分钟一次
    // 0 0 3 * * ? 凌晨 3 点
    public static String corn = "0 0 5 * * ?";

    @Resource
    Environment environment;
    @Resource
    RetryMsgMapper retryMsgMapper;

    @Override
    public String corn() {
        return environment.getProperty("auto.clean.retryMsgLog.corn", corn);
    }

    @Override
    public void execute(JobExecutionContext ignore) {
        try {
            Cost cost = Cost.start();
            String min = environment.getProperty("auto.clean.retryMsgLog.time.in.min",
                    String.valueOf(TimeUnit.DAYS.toMinutes(3)));

            long millis = TimeUnit.MINUTES.toMillis(Integer.parseInt(min));
            Date date = new Date(System.currentTimeMillis() - millis);
            long count = 0;
            int delete = 1;
            while (delete > 0) {
                delete = retryMsgMapper.delete(new LambdaQueryWrapper<RetryMsg>()
                        .le(RetryMsg::getCreateTime, date)
                        .last("limit 100"));
                count += delete;
                if (delete == 0) {
                    break;
                }
            }
            if (count > 0) {
                log.warn("delete RetryLog count {}", count);
            }
            log.info("execute inner end cost = {}ms", cost.end().cost());
        } catch (LockFailException l) {
            // ignore
        } catch (Throwable e) {
            log.warn(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "RetryLogCleanImpl{}";
    }
}
