package cn.think.github.example.consumer;

import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.client.api.impl.SimpleConsumerImpl;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
@Slf4j
@Service
public class MyApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

    @Resource
    Broker broker;
    @Resource
    StreamAdmin streamAdmin;

    @SneakyThrows
    public void main() {
        String topic = "Topic_" + System.currentTimeMillis();
//        topic = "Topic_1702018431070";
        String group = "Group_" + System.currentTimeMillis();
//        group = "Group_1702018504899";

        log.info("topic = {}", topic);
        log.info("group = {}", group);



        broker.start();

        streamAdmin.createTopic(topic);
        streamAdmin.createGroup(group, topic, GroupType.CLUSTER);

        consumer(group, topic);


        //LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(4));


//        producer(topic);



        producer(topic);



    }

    private static void consumer(String group, String topic) {
        SimpleConsumer consumer = new SimpleConsumerImpl(group, topic, 4, GroupType.CLUSTER);
        consumer.register(msgs -> {
            Msg m = msgs.get(0);
            log.info("msg id =[{}]", m.getMsgId());
            return ConsumerResult.success();
        });

        consumer.start();
    }

    private static void producer(String topic) throws InterruptedException {
        SimpleProducer simpleProducer = new SimpleProducerImpl();
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            String finalTopic = topic;
            simpleProducer.send(Msg.builder()
                    .body("hello")
                    .topic(finalTopic)
                    .build());
        }

        Thread.sleep(1000);

        for (int i = 0; i < 10; i++) {
            latch.countDown();
            log.info("countDown " + i);
        }
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        main();
    }
}
