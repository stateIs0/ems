package org.example;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

public class RocketMQProducer {
    public static void main(String[] args) throws Exception {
        // 设置生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("your_producer_group");
        // 设置 NameServer 地址，多个地址用分号分隔
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            // 创建消息实例，参数分别是 topic、tag 和消息体
            Message message = new Message("AAA_T", "Hello RocketMQ".getBytes());
            // 发送消息到指定队列
            producer.send(message);

            System.out.println("Message sent successfully.");
        } finally {
            // 关闭生产者
            producer.shutdown();
        }
    }
}
