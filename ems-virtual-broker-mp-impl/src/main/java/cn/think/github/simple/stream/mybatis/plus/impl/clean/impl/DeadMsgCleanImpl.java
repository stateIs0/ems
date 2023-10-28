package cn.think.github.simple.stream.mybatis.plus.impl.clean.impl;

import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.DeadMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.DeadMsgMapper;
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
 * @date 2023/9/11
 **/
@Service
@Slf4j
public class DeadMsgCleanImpl extends BaseMsgClean {

    public static String corn3 = "0 0 4 * * ?";

    @Resource
    Environment environment;
    @Resource
    DeadMsgMapper deadMsgMapper;

    @Override
    public String corn() {
        return environment.getProperty("auto.clean.deadMsg.corn", corn3);
    }

    @Override
    public void execute(JobExecutionContext context) {
        try {
            Cost cost = Cost.start();
            String min = environment.getProperty("auto.clean.deadMsg.time.in.min",
                    // 3 天前的删除
                    String.valueOf(TimeUnit.DAYS.toMinutes(3)));

            long millis = TimeUnit.MINUTES.toMillis(Integer.parseInt(min));

            Date date = new Date(System.currentTimeMillis() - millis);
            long count = 0;
            int delete = 1;
            while (delete > 0) {
                delete = deadMsgMapper.delete(new LambdaQueryWrapper<DeadMsg>()
                        .le(DeadMsg::getCreateTime, date)
                        .last("limit 100"));
                count += delete;
                if (delete == 0) {
                    break;
                }
            }
            if (count > 0) {
                log.warn("delete deadMsg count {}", count);
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
        return "DeadMsgCleanImpl{}";
    }
}
