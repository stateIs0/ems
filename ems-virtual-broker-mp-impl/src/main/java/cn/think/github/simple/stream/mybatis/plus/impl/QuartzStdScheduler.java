package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.spi.StreamLock;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockFactory;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/18
 **/
@Slf4j
@Component
public class QuartzStdScheduler {

    public static String group = "ems";

    @Resource
    private Scheduler scheduler;
    @Resource
    private StreamLock streamLock;

    public QuartzStdScheduler() {
    }

    public void tryStart(Class<? extends Job> c, String name, String cronExpression, Map<String, String> jobDataMap) {
        try {
            JobDetail job = JobBuilder.newJob(c)
                    .withIdentity(name, group)
                    .build();
            streamLock.lockAndExecute(() -> {
                try {
                    if (!scheduler.checkExists(job.getKey())) {
                        create(cronExpression, job, jobDataMap);
                        return null;
                    }
                    List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(job.getKey());
                    Trigger trigger = triggersOfJob.get(0);
                    if (trigger instanceof CronTriggerImpl) {
                        CronTriggerImpl t = (CronTriggerImpl) trigger;
                        // 表达式变化了
                        if (!t.getCronExpression().equals(cronExpression)) {
                            scheduler.deleteJob(job.getKey());
                            log.info("job {} exists {} {}, delete and add", job.getKey(), name, cronExpression);
                            create(cronExpression, job, jobDataMap);
                        }
                    }
                } catch (SchedulerException | ParseException e) {
                    // ignore
                    log.warn(e.getMessage(), e);
                }
                return null;
            }, getClass().getName(), 100);


        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private void create(String cronExpression, JobDetail job, Map<String, String> jobDataMap) throws ParseException, SchedulerException {
        CronTriggerImpl trigger = new CronTriggerImpl();
        trigger.setCronExpression(cronExpression);
        trigger.setName(job.getKey().getName());
        if (jobDataMap != null) {
            for (Map.Entry<String, String> item : jobDataMap.entrySet()) {
                job.getJobDataMap().put(item.getKey(), item.getValue());
            }
        }
        scheduler.scheduleJob(job, trigger);
    }
}
