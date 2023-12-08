package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.api.util.NamedThreadFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RetryMsgHandlerTaskTest {
    private final Map<String, ThreadPoolExecutor> poolManager = new ConcurrentHashMap<>();

    @Test
    public void start() throws InterruptedException {

        Map<String, List<RetryMsg>> map = new TreeMap<>((o1, o2) -> getPool(o1).getActiveCount() - getPool(o2).getActiveCount());

        for (int i = 0; i < 10; i++) {
            getPool("1").execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println("1");
                        Thread.sleep(10000000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        for (int i = 0; i < 20; i++) {
            getPool("2").execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println("2");
                        Thread.sleep(10000000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        map.put("1", new ArrayList<>());
        map.put("2", new ArrayList<>());
        map.put("2", new ArrayList<>());
        map.put("1", new ArrayList<>());
        map.put("1", new ArrayList<>());

        Thread.sleep(1500);

        System.out.println("Thread.sleep(1000);");

        for (Map.Entry<String, List<RetryMsg>> stringListEntry : map.entrySet()) {
            System.out.println(stringListEntry.getKey());
        }
    }

    private ThreadPoolExecutor getPool(String key) {
        return poolManager.computeIfAbsent(key, s -> new ThreadPoolExecutor(1,
                20, 10, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("retryTask-" + key),
                new ThreadPoolExecutor.CallerRunsPolicy()));
    }
}