package cn.think.github.spi.factory;

import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.api.util.SpiFactory;
import cn.think.github.simple.stream.client.api.impl.SimpleConsumerImpl;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;


/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
@Slf4j
@Service
public class MyApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

    public static String Consumer = "Consumer1";

    static String url = "jdbc:mysql://localhost:3306/ems";
    static String username = "root";
    static String password = "Cc123456_";

    @Resource
    SpiFactory spiFactory;
    @Resource
    Broker broker;

    String topic = "TopicA";
    String group = "Group1";

    public void main() {

        cleanData();

        broker.start();

        Supplier<StreamAdmin> adminSupplier = spiFactory.getObj(StreamAdmin.class);
        adminSupplier.get().createTopic(topic);
        adminSupplier.get().createGroup(group, topic, GroupType.CLUSTER);

        SimpleProducer producer = new SimpleProducerImpl();
        producer.start();

        for (int i = 0; i < 1000; i++) {
            SendResult send = producer.send(Msg.builder().topic(topic).body("hello-world ")
                    .properties(new Properties())
                    .tags("aaa tag")
                    .build());
            log.warn("发送消息 {}", send.getMsgId());
        }


        SimpleConsumer consumer = new SimpleConsumerImpl(group, topic, 5, GroupType.CLUSTER);

        final Long[] preTime = {0L};
        AtomicLong atomicLong = new AtomicLong(0);
        consumer.register(msgs -> {
            Msg m = msgs.get(0);
            if (preTime[0] == 0) {
                preTime[0] = System.currentTimeMillis();
            }
            long l = System.currentTimeMillis() - preTime[0];

            try {
                log.info("retry --------->>> {} {} {}", atomicLong.incrementAndGet(), TimeUnit.MILLISECONDS.toSeconds(l), m.getConsumerTimes());
                Thread.sleep(100);
            } catch (Throwable ignore) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    //
                }
                throw new RuntimeException(ignore);
            }
            preTime[0] = System.currentTimeMillis();
            return ConsumerResult.fail();
        });

        consumer.start();

        log.warn("start .....");


    }

    private static void initFlowRules() {
        List<FlowRule> rules = new ArrayList<>();
        FlowRule rule = new FlowRule();
        rule.setResource("Group");
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        // Set limit QPS to 20.
        rule.setCount(200000);
        rules.add(rule);
        FlowRuleManager.loadRules(rules);
    }

    static void cleanData() {
        try {
            // 1. 注册JDBC驱动程序（通常只需在应用程序启动时执行一次）
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 2. 建立数据库连接
            Connection connection = DriverManager.getConnection(url, username, password);

            // 3. 创建Statement对象
            Statement statement = connection.createStatement();

            // 4. 执行多个SQL语句
            String sqlQuery;
            sqlQuery = "truncate table ems_simple_msg;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table ems_topic_group_log;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table ems_retry_msg;";
            statement.execute(sqlQuery);

            statement.close();
            connection.close();

            Jedis jedis = new Jedis("localhost");
            jedis.flushDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        main();
    }
}
