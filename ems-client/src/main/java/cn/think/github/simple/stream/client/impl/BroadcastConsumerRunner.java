package cn.think.github.simple.stream.client.impl;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.simple.util.EmsEmptyList;
import cn.think.github.simple.stream.api.util.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/11
 **/
@Slf4j
public class BroadcastConsumerRunner implements Runnable {

    private static final String BROAD_CAST_DIR = "ems_bc/";

    private static boolean init = false;

    // 广播消息;
    // 如果磁盘没有记录, 则从当前时间 最新的消息开始消费
    // offset 记录在内存中和磁盘中
    // k8s 重启可能丢失 offset 记录
    // 没有 ack 和重试
    private final ConsumerClientImpl consumerClient;

    private final ScheduledThreadPoolExecutor persistencePool = new ScheduledThreadPoolExecutor(1);

    private final ThreadPoolExecutor workerPool;

    // 单线程更新, 无并发问题.
    private long maxOffset;

    public BroadcastConsumerRunner(ConsumerClientImpl consumerClient, int threadNum) {
        this.consumerClient = consumerClient;

        String offset = PersistenceOffset.getOffset(consumerClient.topicName, consumerClient.groupName);
        if (offset == null || offset.trim().isEmpty()) {
            maxOffset = -1L;
        } else {
            maxOffset = (Long.parseLong(offset));
            log.info("read {} maxOffset from file " + consumerClient.groupName + "$" + consumerClient.topicName, maxOffset);
        }

        this.workerPool = new ThreadPoolExecutor(threadNum, threadNum, 60, TimeUnit.SECONDS,
                //16, 设置的小一点
                new SynchronousQueue<>(), new NamedThreadFactory(consumerClient.topicName + "$" + consumerClient.groupName),
                // 在当前线程执行
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.persistencePool.scheduleWithFixedDelay(() -> {
            try {
                if (maxOffset >= 0) {
                    PersistenceOffset.persistence(maxOffset, consumerClient.topicName, consumerClient.groupName);
                }
            } catch (Throwable e) {
                // ignore
                log.warn(e.getMessage(), e);
            }
        }, 5, 2, TimeUnit.SECONDS);
    }


    @Override
    public void run() {
        while (consumerClient.running) {
            try {
                loopLogic();
            } catch (Throwable e) {
                log.warn(e.getMessage(), e);
            }
        }
        log.info("EMS Broadcast Graceful exit {}", consumerClient.groupName);
        persistencePool.shutdown();
        workerPool.shutdown();
    }

    private void loopLogic() {
        List<Msg> msgList = consumerClient.broker.get().pullMsg(consumerClient.topicName, maxOffset);
        if (msgList instanceof EmsEmptyList) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            EmsEmptyList<Msg> emsEmptyList = (EmsEmptyList<Msg>) msgList;
            maxOffset = emsEmptyList.getMaxOffset();
            return;
        }
        if (msgList.isEmpty()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            return;
        }
        try {
            for (Msg msg : msgList) {
                List<Msg> w = new ArrayList<>();
                w.add(msg);
                workerPool.execute(() -> {
                    try {
                        consumerClient.bizListener.consumer(w);
                    } catch (Throwable throwable) {
                        log.error(throwable.getMessage(), throwable);
                    }
                });
            }
        } catch (Throwable t) {
            log.warn(t.getMessage(), t);
        } finally {
            msgList.sort((o1, o2) -> (int) (o2.getOffset() - o1.getOffset()));
            Msg msg = msgList.get(0);
            // 提交成功了, 就更新.
            maxOffset = (msg.getOffset());
        }
    }

    public static class PersistenceOffset {

        public static void persistence(Long offset, String topic, String group) throws IOException {
            // 注意: k8s 场景下, 持久化也没用.
            initDir();
            File file = new File(BROAD_CAST_DIR + group + "$" + topic);
            if (file.exists()) {
                Files.write(file.toPath(), String.valueOf(offset).getBytes());
                return;
            }
            try {
                Files.createFile(file.toPath());
                Files.write(file.toPath(), String.valueOf(offset).getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static String getOffset(String topic, String group) {
            initDir();
            try {
                String filePath = BROAD_CAST_DIR + group + "$" + topic;
                if (!new File(filePath).exists()) {
                    log.info("offset {} 文件不存在", filePath);
                    return null;
                }
                FileReader reader = new FileReader(filePath);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String line;
                StringBuilder r = new StringBuilder();
                while ((line = bufferedReader.readLine()) != null) {
                    r.append(line);
                }
                bufferedReader.close();
                return r.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void initDir() {
        if (init) {
            return;
        }
        File dir = new File(BROAD_CAST_DIR);
        if (!dir.exists()) {
            boolean result = dir.mkdir();
            if (!result) {
                log.warn("create [ems_bc/] dir fail");
                return;
            }
        }
        init = true;
    }
}
