package cn.think.github.simple.stream.adapter;

import cn.think.github.simple.stream.mybatis.plus.impl.BrokerSpiFactoryImpl;
import cn.think.github.simple.stream.mybatis.plus.impl.clean.impl.AutowiringSpringBeanJobFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.clean.impl.DruidConnectionProvider;
import cn.think.github.simple.stream.api.util.JsonUtil;
import cn.think.github.simple.stream.api.util.SpiFactory;
import com.alibaba.druid.pool.DruidDataSource;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.annotation.MapperScan;
import org.quartz.Scheduler;
import org.quartz.spi.JobFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.IOException;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/17
 **/
@Configuration
// 开关, 默认关闭
@ConditionalOnProperty(name = "ems.enabled", havingValue = "true", matchIfMissing = false)
@MapperScan(basePackages = {"cn.think.github.simple.stream.mybatis.plus"}, sqlSessionFactoryRef = "emsSqlSessionFactory")
@ComponentScan(basePackages = {"cn.think.github.simple.stream.mybatis.plus", "cn.think.github.simple.stream.redis.openSource.impl"})
public class EmsBrokerBoot {

    @Resource
    ObjectMapper objectMapper;

    @Bean(name = "emsDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.ems")
    public DataSource dataSource() {
        // 数据库连接池打印日志:discard long time none received connection
        System.setProperty("druid.mysql.usePingMethod","false");
        return new DruidDataSource();
    }


    @Bean(name = "emsTransactionManager")
    public PlatformTransactionManager dataSourceTransactionManager(@Qualifier("emsDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "emsSqlSessionFactory")
    public SqlSessionFactory sqlSessionFactory(@Qualifier("emsDataSource") DataSource dataSource) throws Exception {
        final MybatisSqlSessionFactoryBean sessionFactoryBean = new MybatisSqlSessionFactoryBean();
        sessionFactoryBean.setDataSource(dataSource);
        return sessionFactoryBean.getObject();
    }

    @Bean
    public JobFactory jobFactory(ApplicationContext applicationContext) {
        AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(JobFactory jobFactory) throws IOException {
        PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
        propertiesFactoryBean.setLocation(new ClassPathResource("/ems-quartz.properties"));
        propertiesFactoryBean.afterPropertiesSet();
        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setQuartzProperties(propertiesFactoryBean.getObject());
        factory.setJobFactory(jobFactory);
        factory.setApplicationContextSchedulerContextKey("applicationContextKey");
        factory.setWaitForJobsToCompleteOnShutdown(true);
        factory.setOverwriteExistingJobs(false);
        factory.setStartupDelay(3);

        return factory;
    }


    @Bean
    public Scheduler scheduler(@Qualifier("emsDataSource") DataSource dataSource, JobFactory jobFactory) throws Exception {
        DruidConnectionProvider.datasource = dataSource;
        return schedulerFactoryBean(jobFactory).getScheduler();
    }

    @Bean
    public SpiFactory spiFactory() {
        return new BrokerSpiFactoryImpl();
    }

    @Bean
    public JsonUtil jsonUtil() {
        return new JsonUtil() {
            @Override
            public String write(Object o) {
                try {
                    return objectMapper.writeValueAsString(o);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public <T> T read(String json, Class<T> c) {
                try {
                    return objectMapper.readValue(json, c);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

}
