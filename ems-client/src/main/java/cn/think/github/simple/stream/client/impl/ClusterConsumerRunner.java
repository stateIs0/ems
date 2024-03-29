package cn.think.github.simple.stream.client.impl;

import cn.think.github.simple.stream.api.ConsumerResult;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.spi.LockFailException;
import cn.think.github.simple.stream.api.util.NamedThreadFactory;
import cn.think.github.simple.stream.client.support.CollectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/31
 **/
@Slf4j
public class ClusterConsumerRunner implements Runnable {

    private final ConsumerClientImpl consumerClient;
    String clientIdId;

    int threadNum;

    ThreadPoolExecutor workerPool;

    public ClusterConsumerRunner(ConsumerClientImpl consumerClient, String clientIdId, int threadNum) {
        this.consumerClient = consumerClient;
        this.clientIdId = clientIdId;
        this.threadNum = threadNum;
        this.workerPool = new ThreadPoolExecutor(
                threadNum,
                threadNum,
                60,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory(consumerClient.topicName + "$" + consumerClient.groupName),
                new ThreadPoolExecutor.AbortPolicy());
        // 不用的时候, 关闭核心线程, 节约资源
        this.workerPool.allowCoreThreadTimeOut(true);
    }

    @Override
    public void run() {
        while (consumerClient.running) {
            processMsg();
        }
        workerPool.shutdownNow();
        log.info("{} 消费者 [{}] 线程优雅停止..... {}", consumerClient.groupName, threadNum, consumerClient.topicName);
    }

    private void processMsg() {
        try {
            // 循环读取消息
            List<Msg> msgList = new ArrayList<>();
            try {
                msgList = consumerClient.broker.get().pullMsg(consumerClient.topicName, consumerClient.groupName, clientIdId, 30);
            } catch (LockFailException e) {
                // ignore
                log.warn("fail get lock {} {} {}", consumerClient.topicName, consumerClient.groupName, consumerClient.clientId);
            }
            if (CollectionUtils.isEmpty(msgList)) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(new Random().nextInt(200)));
                return;
            }

            msgList.forEach(msg -> {
                while (true) {
                    try {
                        List<Msg> array = new ArrayList<>();
                        // only one
                        array.add(msg);
                        workerPool.execute(() -> consumer(array));
                        break;
                    } catch (RejectedExecutionException r) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    }
                }
            });

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

    private void consumer(List<Msg> array) {
        Msg m = array.stream().findFirst().get();
        try {
            m.setReceiveLater(true);
            ConsumerResult result = consumerClient.bizListener.consumer(array);
            if (result == null) {
                throw new RuntimeException("消费者不能返回 null");
            }
            m.setReceiveLater(result.isReceiveLater());
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            m.setReceiveLater(true);
        }
        if (consumerClient.broker.get().ack(array, consumerClient.groupName, clientIdId, 30)) {
            // success, ignore
            return;
        }
        log.error("ack fail, msg = {}", array);
    }
}
