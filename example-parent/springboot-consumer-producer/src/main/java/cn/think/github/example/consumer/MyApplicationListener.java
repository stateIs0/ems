package cn.think.github.example.consumer;

import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.client.api.impl.SimpleConsumerImpl;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

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

    @SneakyThrows
    public void main() {
        System.setProperty("ems.test", "true");
        String topic = "Topic_" + System.currentTimeMillis();
        topic = "Topic_Retry_1214";
        String group = "Group_" + System.currentTimeMillis();
        group = "Group_Retry_1214";

        log.info("topic = {}", topic);
        log.info("group = {}", group);


        consumer(group + "_1", topic);
        consumer(group + "_2", topic);
        consumer(group + "_3", topic);
        consumer(group + "_4", topic);

        producer(topic);


    }

    @SneakyThrows
    private static void consumer(String group, String topic) {
        SimpleConsumer consumer = new SimpleConsumerImpl(group, topic, 20, GroupType.CLUSTER);
        consumer.register(msgs -> {
            Msg m = msgs.get(0);
            //log.info("msg id =[{}] ---> getConsumerTimes={}", m.getMsgId(), m.getConsumerTimes());
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(400));
            } catch (InterruptedException e) {
                //
            }
            if (m.getMsgId().endsWith("0") && m.getConsumerTimes() < 3) {
                return ConsumerResult.fail();
            }
            return ConsumerResult.success();
        });

        consumer.start();
    }

    private static void producer(String topic) throws InterruptedException {
        SimpleProducer simpleProducer = new SimpleProducerImpl();

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < Integer.MAX_VALUE; i++) {
                    // 10 tps
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    String finalTopic = topic;
                    simpleProducer.send(Msg.builder()
                            .body("hello")
                            .topic(finalTopic)
                            .build());
                }
            }
        }).start();
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        main();
    }
}
