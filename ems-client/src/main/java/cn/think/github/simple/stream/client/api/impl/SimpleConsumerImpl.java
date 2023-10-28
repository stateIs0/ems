package cn.think.github.simple.stream.client.api.impl;

import cn.think.github.simple.stream.api.GroupType;
import cn.think.github.simple.stream.api.SimpleConsumer;
import cn.think.github.simple.stream.api.SimpleListener;
import cn.think.github.simple.stream.client.impl.ConsumerClientImpl;
import cn.think.github.simple.stream.client.support.ConsumerClient;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public class SimpleConsumerImpl implements SimpleConsumer {

    ConsumerClient consumerClient = new ConsumerClientImpl();

    String groupName;

    String topicName;

    int threads;

    GroupType groupType;

    public SimpleConsumerImpl(String groupName, String topicName, int threads, GroupType groupType) {
        this.groupName = groupName;
        this.topicName = topicName;
        this.threads = threads;
        this.groupType = groupType;
    }

    @Override
    public void register(SimpleListener simpleListener) {
        consumerClient.setListener(simpleListener, groupName, topicName, groupType);
        consumerClient.setThreadNum(threads);
    }

    @Override
    public void start() {
        consumerClient.start();
    }

    @Override
    public void stop() {
        consumerClient.stop();
    }
}
