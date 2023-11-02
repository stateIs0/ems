package cn.think.github.simple.stream.client.impl;

import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.api.simple.util.IPv4AddressUtil;
import cn.think.github.simple.stream.api.simple.util.PIDUtil;
import cn.think.github.simple.stream.api.simple.util.TopicConstant;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.client.VirtualBrokerFactory;
import cn.think.github.simple.stream.client.support.ConsumerClient;
import cn.think.github.spi.factory.NamedThreadFactory;
import cn.think.github.spi.factory.SpiFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * @author cxs
 * @version 1.0
 **/
@Slf4j
public class ConsumerClientImpl implements ConsumerClient {

    public final static ScheduledThreadPoolExecutor RENEW =
            new ScheduledThreadPoolExecutor(1,
                    new NamedThreadFactory("EMS-RENEW"));

    volatile boolean running = false;

    private static final int keepAliveTime = 60;

    String clientId;

    String groupName;

    String topicName;

    SimpleListener bizListener;

    int threadNum;

    ThreadPoolExecutor mainWorker;

    Supplier<Broker> broker;

    private Runnable renew_runnable;

    private StreamAdmin admin;

    private GroupType groupType;

    private final Supplier<EmsSystemConfig> systemConfigSupplier = SpiFactory.getInstance().getObj(EmsSystemConfig.class);

    @Override
    public void setListener(SimpleListener simpleListener, String groupName, String topicName, GroupType groupType) {
        this.bizListener = simpleListener;
        this.groupName = groupName;
        this.topicName = topicName;
        this.groupType = groupType;
    }

    @Override
    public void setThreadNum(int num) {
        if (num <= 0) {
            throw new RuntimeException("非法的线程参数 num .... -> " + num);
        }
        this.threadNum = num;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (bizListener == null) {
            throw new RuntimeException("请先设置 simpleListener 监听对象再启动.");
        }
        if (admin == null) {
            Supplier<StreamAdmin> obj = SpiFactory.getInstance().getObj(StreamAdmin.class);
            if ((admin = obj.get()) == null) {
                throw new RuntimeException("未配置 ems broker");
            }
        }
        if (!admin.existTopic(topicName)) {
            if (admin.isAutoCreateTopicOrGroup()) {
                admin.createTopic(topicName);
            } else {
                throw new RuntimeException("topic 不存在, 且没有配置自动创建 topic");
            }
        }
        if (!admin.existGroup(groupName)) {
            if (admin.isAutoCreateTopicOrGroup()) {
                admin.createGroup(groupName, topicName, GroupType.CLUSTER);
            } else {
                throw new RuntimeException("group 不存在, 且没有配置自动创建 group");
            }
        }
        if (admin.getTopicRule(topicName) >= TopicConstant.RULE_DENY) {
            log.warn("当前 topic 禁止读写, {}", topicName);
            return;
        }

        running = true;
        broker = VirtualBrokerFactory.get();
        renew_runnable = () -> {
            try {
                broker.get().renew(clientId, groupName, topicName);
            } catch (Throwable e) {
                //ignore
                log.warn(e.getMessage(), e);
            }
        };
        if (threadNum == 0) {
            threadNum = systemConfigSupplier.get().consumerThreads();
        }

        mainWorker = new ThreadPoolExecutor(1, 1, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new MainWorkerThreadFactory(topicName, groupName));

        this.clientId = IPv4AddressUtil.get() + "@" + PIDUtil.get() + "@" + UUID.randomUUID().toString().replace("-", "");

        if (groupType == GroupType.BROADCASTING) {
            log.info("广播 group {}, this.clientId = {} ", groupName, clientId);
            mainWorker.execute(new BroadcastConsumerRunner(this, threadNum));
        } else {
            mainWorker.execute(new ClusterConsumerRunner(this, clientId, threadNum));
        }

        log.info("{} start consumer {} {}, thread {}", clientId, groupName, topicName, threadNum);
        RENEW.scheduleAtFixedRate(renew_runnable, 10, 10, TimeUnit.SECONDS);
        broker.get().resisterTask(this);
    }

    @Override
    public synchronized void stop() {
        if (running) {
            running = false;
            mainWorker.shutdownNow();
            RENEW.remove(renew_runnable);
            broker.get().down(clientId);
            log.warn("stop consumer {} {}", groupName, topicName);
        }
    }

    @Override
    public String toString() {
        return "ConsumerClientImpl{" +
                "clientId='" + clientId + '\'' +
                ", groupName='" + groupName + '\'' +
                ", topicName='" + topicName + '\'' +
                '}';
    }

    static class MainWorkerThreadFactory implements ThreadFactory {

        int num = 0;
        String topicName;
        String groupName;

        public MainWorkerThreadFactory(String topicName, String groupName) {
            this.topicName = topicName;
            this.groupName = groupName;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread t = new Thread(runnable, topicName + "-" + groupName + "-" + ++num);
            t.setDaemon(true);
            return t;
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    static class TopicGroupPair {
        String topic;
        String group;
        final Object notify;
        Broker broker;

    }


}
