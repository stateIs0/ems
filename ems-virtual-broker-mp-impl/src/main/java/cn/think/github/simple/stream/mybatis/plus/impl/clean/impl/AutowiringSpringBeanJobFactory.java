package cn.think.github.simple.stream.mybatis.plus.impl.clean.impl;

import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

/**
 * Adds autowiring support to quartz jobs.
 * Created by david on 2015-01-20.
 * @see https://gist.github.com/jelies/5085593
 */
public final class AutowiringSpringBeanJobFactory extends SpringBeanJobFactory implements
        ApplicationContextAware {

    private transient AutowireCapableBeanFactory beanFactory;

    @Override
    public void setApplicationContext(final ApplicationContext context) {
        beanFactory = context.getAutowireCapableBeanFactory();
    }

    @Override
    protected Object createJobInstance(final TriggerFiredBundle triggerFiredBundle) throws Exception {
        final Object job = super.createJobInstance(triggerFiredBundle);
        beanFactory.autowireBean(job);
        return job;
    }
}