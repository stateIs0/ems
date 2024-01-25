package cn.think.github.example.consumer;

import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
import cn.think.github.simple.stream.client.impl.ConsumerClientImpl;
import cn.think.github.simple.stream.client.support.ConsumerClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @Author cxs
 * @Description
 * @date 2024/1/17
 * @version 1.0
 **/
@Slf4j
@RequestMapping("/")
@RestController
public class DemoController {

    Map<String, ConsumerClient> consumerClientMap = new java.util.HashMap<>();

    @PostMapping("/send")
    public String send(@RequestBody final SendParams sendParams) {
        SimpleProducer simpleProducer = new SimpleProducerImpl();

        SendResult send = simpleProducer.send(Msg.builder().topic(sendParams.topicName)
                .body(sendParams.msg).build());

        return send.getMsgId();
    }

    @PostMapping("/startConsumer")
    public String startConsumer(@RequestBody final ConsumerParams consumerParams) {
        ConsumerClient client = new ConsumerClientImpl();

        client.setListener(msgList -> {
            log.info(msgList.stream().findFirst().toString());
            return ConsumerResult.success();
        }, consumerParams.group, consumerParams.topicName, GroupType.CLUSTER);

        client.setThreadNum(10);

        client.start();

        consumerClientMap.put(consumerParams.group, client);
        return "success";
    }

    @PostMapping("/stopConsumer")
    public String stopConsumer(String group) {
        ConsumerClient client = consumerClientMap.get(group);
        if (client != null) {
            client.stop();
            return "success";
        } else {
            return "fail";
        }
    }

    @Data
    public static class ConsumerParams {
        String topicName;
        String group;
    }

    @Data
    public static class SendParams {
        String topicName;
        String msg;
    }
}
