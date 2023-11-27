package cn.think.github.simple.stream.client.impl;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class BroadcastConsumerRunnerTest {

    @Test
    public void run() throws IOException {

        BroadcastConsumerRunner.PersistenceOffset.persistence(1L, "aaaT", "aaaG");
        BroadcastConsumerRunner.PersistenceOffset.persistence(2L, "aaaT", "aaaG");
        String offset = BroadcastConsumerRunner.PersistenceOffset.getOffset("aaaT", "aaaG");
        BroadcastConsumerRunner.PersistenceOffset.persistence(3L, "aaaT", "aaaG");
        System.out.println(offset);
        offset = BroadcastConsumerRunner.PersistenceOffset.getOffset("aaaT", "aaaC");
        System.out.println(offset);

    }

    ThreadPoolExecutor workerPool = new ThreadPoolExecutor(1, 1 * 2, 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> {
        Thread t = new Thread(r, "Broadcast-workerPool");
        t.setDaemon(true);
        return t;
    });

    @Test
    public void testRun() {
        workerPool = new ThreadPoolExecutor(1, 1 * 2, 60, TimeUnit.SECONDS,
                new SynchronousQueue<>(), r -> {
            Thread t = new Thread(r, "Broadcast-workerPool");
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < 4; i++) {
            System.out.println("---------->>>> " + i);
            workerPool.execute(new Runnable() {
                @Override
                public void run() {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(100));
                }
            });
        }

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
    }
}