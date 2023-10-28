package cn.think.github.simple.stream.client.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.SendResult;
import cn.think.github.simple.stream.api.StreamAdmin;
import cn.think.github.simple.stream.api.simple.util.TopicConstant;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.client.VirtualBrokerFactory;
import cn.think.github.simple.stream.client.support.ProducerClient;
import cn.think.github.spi.factory.SpiFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Slf4j
public class ProducerClientImpl implements ProducerClient {

    private volatile Broker broker;
    private volatile StreamAdmin admin;
    private volatile boolean first = true;
    private volatile EmsSystemConfig systemConfig;
    private final Set<String> exitsTopicSet = new ConcurrentSkipListSet<>();

    @Override
    public SendResult saveMsg(Msg msg) {
        lazyLoad();
        SendResult x = check(msg);
        if (x != null) {
            return x;
        }

        String send = broker.send(msg, 30);
        SendResult sendResult = new SendResult(true);
        sendResult.setMsgId(send);
        return sendResult;
    }

    private SendResult check(Msg msg) {
        // 检查 topic 是否存在.
        if (!exitsTopicSet.contains(msg.getTopic())) {
            if (!admin.existTopic(msg.getTopic())) {
                if (admin.isAutoCreateTopicOrGroup()) {
                    admin.createTopic(msg.getTopic());
                } else {
                    throw new RuntimeException("未开通自动创建 topic");
                }
            }
            exitsTopicSet.add(msg.getTopic());
        }

        // 判断 topic 写入权限, 如果禁止写入, 就抛弃消息
        if (first || new Random().nextInt(10000) % 5000 == 0) {
            first = false;
            if (TopicConstant.RULE_WRITE < admin.getTopicRule(msg.getTopic())) {
                log.warn("topic {} 写权限被关闭, 停止写入", msg.getTopic());
                return new SendResult(false);
            }
        }

        String body = msg.getBody();
        if (body != null) {
            int length = body.getBytes(StandardCharsets.UTF_8).length;
            int max = systemConfig.msgMaxSizeInBytes();
            if (length > max) {
                throw new RuntimeException("消息超过指定大小 " + max);
            }
        }
        return null;
    }

    private void lazyLoad() {
        if (broker == null) {
            synchronized (this) {
                if (broker == null) {
                    broker = VirtualBrokerFactory.get().get();
                }
            }
        }
        if (systemConfig == null) {
            synchronized (this) {
                if (systemConfig == null) {
                    systemConfig = SpiFactory.getInstance().getObj(EmsSystemConfig.class).get();
                }
            }
        }
        if (admin == null) {
            synchronized (this) {
                if (admin == null) {
                    admin = SpiFactory.getInstance().getObj(StreamAdmin.class).get();
                }
            }
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
    }
}
