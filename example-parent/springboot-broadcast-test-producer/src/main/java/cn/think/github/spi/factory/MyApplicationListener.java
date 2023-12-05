package cn.think.github.spi.factory;

import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.api.util.SpiFactory;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
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

    static String url = "jdbc:mysql://localhost:3306/simple_stream";
    static String username = "root";
    static String password = "Cc123456_";

    @Resource
    SpiFactory spiFactory;
    @Resource
    Broker broker;

    String topic = "TopicBroadcast";
    String group = "GroupBroadcast";


    public void main() {

        //cleanData();

        broker.start();

        SimpleProducerFactory simpleProducerFactory = SimpleProducerImpl::new;

        SimpleProducer producer = simpleProducerFactory.create();
        producer.start();


        Supplier<StreamAdmin> adminSupplier = spiFactory.getObj(StreamAdmin.class);
        adminSupplier.get().createTopic(topic, 1);
        adminSupplier.get().createGroup(group, topic, GroupType.BROADCASTING);

        for (int i = 0; i < 100; i++) {
            SendResult send = producer.send(Msg.builder().topic(topic).body("hello-world " + System.currentTimeMillis()).build());
            log.info("msgId={}", send.getMsgId());
        }
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
            sqlQuery = "truncate table simple_msg;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table running_log;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table topic_group_log;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table retry_msg;";
            statement.execute(sqlQuery);

            statement.close();
            connection.close();

            Jedis jedis = new Jedis("localhost");
            jedis.del("TopicA");
            jedis.del("TopicB");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    boolean f = false;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        synchronized (this) {
            if (!f) {
                main();
                f = true;
            }
        }
    }
}
