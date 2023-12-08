package org.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class RocketMQConsumer {
    public static void main(String[] args) throws Exception {
        // 设置消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("AAA_G");
        // 设置 NameServer 地址，多个地址用分号分隔
        consumer.setNamesrvAddr("172.20.62.133:9876");
        // 订阅消息，参数分别是 topic 和 tag，* 表示订阅所有消息
        consumer.subscribe("AAA_T", "*");

        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("Received message: " + new String(msg.getBody()));
                }
                // 消息处理成功
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        // 启动消费者
        consumer.start();

        System.out.println("Consumer started.");
    }
}
