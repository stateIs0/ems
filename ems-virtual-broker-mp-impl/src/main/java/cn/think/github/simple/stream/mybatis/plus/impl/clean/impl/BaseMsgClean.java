package cn.think.github.simple.stream.mybatis.plus.impl.clean.impl;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.mybatis.plus.impl.QuartzStdScheduler;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/14
 * https://www.bejson.com/othertools/cronvalidate/
 **/
@Slf4j
public abstract class BaseMsgClean implements LifeCycle, Job {

    protected static final String CORN = "0 0/1 * * * ?";

    @Resource
    protected Broker broker;
    @Resource
    protected QuartzStdScheduler quartzStdScheduler;

    @PostConstruct
    public void init_() {
        broker.resisterTask(this);
    }

    @Override
    public void start() {
        log.warn("start delete {} {} {} ", this.getClass().getName(), getClass().getName(), this.corn());
        quartzStdScheduler.tryStart(this.getClass(), getClass().getName(), corn(), null);
    }

    /**
     * 0/20 * * * * ?   表示每20秒 调整任务
     * 0 0 4 * * ? 每天凌晨 4 点
     */
    public abstract String corn();


    @Override
    public void stop() {
    }
}
