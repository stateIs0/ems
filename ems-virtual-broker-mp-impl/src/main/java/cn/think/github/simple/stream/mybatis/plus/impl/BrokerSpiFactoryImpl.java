package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.spi.factory.SpiFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Component
public class BrokerSpiFactoryImpl implements SpiFactory {

    static BrokerSpiFactoryImpl brokerSpiFactory;

    ApplicationContext applicationContext;

    public BrokerSpiFactoryImpl() {
    }


    @Autowired
    public void setApplicationContext(ApplicationContext applicationContext) {
        brokerSpiFactory = this;
        brokerSpiFactory.applicationContext = applicationContext;
    }

    @Override
    public <T> Supplier<T> getObj(Class<T> c) {
        T bean = null;
        try {
            while (brokerSpiFactory == null) {
                log.warn("brokerSpiFactory is null, wait springboot ready....");
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
            if (brokerSpiFactory.applicationContext == null) {
                log.warn("spring applicationContext 容器没有初始化....");
            } else {
                bean = brokerSpiFactory.applicationContext.getBean(c);
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
        if (bean == null) {
            return SpiFactory.I.getObj(c);
        } else {
            T finalBean = bean;
            return () -> finalBean;
        }
    }
}
