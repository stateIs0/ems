package cn.think.github.spi.factory;

import cn.think.github.simple.stream.api.ConsumerResult;
import cn.think.github.simple.stream.api.GroupType;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.SimpleConsumer;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.client.api.impl.SimpleConsumerImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;


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

    static String url = "jdbc:mysql://localhost:3306/simple_stream";
    static String username = "root";
    static String password = "Cc123456_";

    @Resource
    Broker broker;


    String topic = "TopicBroadcast";
    String group = "GroupBroadcast1";

    public void main() {

        broker.start();

        SimpleConsumer consumer = getSimpleConsumer(topic);

        consumer.register(msgs -> {
            Msg m = msgs.get(0);
            log.info("---> GroupBroadcast1 msg = {}", m.toString());
            return ConsumerResult.success();
        });

        consumer.start();


    }

    private SimpleConsumer getSimpleConsumer(String topicName) {
        return new SimpleConsumerImpl(group, topicName, 5, GroupType.CLUSTER);
    }


    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        main();
    }
}
