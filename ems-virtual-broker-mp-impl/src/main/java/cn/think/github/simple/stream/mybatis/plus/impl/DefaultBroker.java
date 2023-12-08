package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.GroupClientTableProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
@Service
public class DefaultBroker implements Broker {

    @Resource
    private GroupClientTableProcessor groupClientTableProcessor;
    @Resource
    private BrokerPullProcessor brokerPullProcessor;
    @Resource
    private SendMsgProcessor sendMsgProcessor;

    private final Map<String, String> memoryTable = new ConcurrentHashMap<>();

    private final Set<LifeCycle> lifeCycles = new CopyOnWriteArraySet<>();

    private volatile boolean start;

    @PostConstruct
    public void init() {
        log.info("--------->>>> mp-broker start.....");
    }

    @Override
    public Set<String> getSubGroups() {
        return new HashSet<>(memoryTable.values());
    }

    @Override
    public synchronized void resisterTask(LifeCycle lifeCycle) {
        log.warn("add lifeCycle {}", lifeCycle.getClass().getName());
        lifeCycles.add(lifeCycle);
    }

    @Override
    public void renew(String clientId, String groupName, String topicName) {
        if (notStart()) {
            start();
        }
        groupClientTableProcessor.renew(clientId, groupName);
        memoryTable.put(clientId, groupName);
    }

    @Override
    public void down(String clientId) {
        if (notStart()) {
            start();
        }
        groupClientTableProcessor.down(clientId);
        memoryTable.remove(clientId);
    }

    @Override
    public List<Msg> pullMsg(String topicName, long maxOffset) {
        if (notStart()) {
            start();
        }
        return brokerPullProcessor.pullMsg(topicName, maxOffset);
    }

    @Override
    public List<Msg> pullMsg(String topicName, String groupName, String clientId, int timeoutInSec) {
        if (notStart()) {
            start();
        }

        return brokerPullProcessor.pull(topicName, groupName, clientId, timeoutInSec);
    }

    @Override
    public boolean ack(List<Msg> list, String group, String clientId, int timeoutInSec) {
        if (notStart()) {
            start();
        }
        return brokerPullProcessor.ack(list, group, clientId, timeoutInSec);
    }


    @Override
    public String send(Msg msg, int timeoutInSec) {
        if (notStart()) {
            start();
        }
        return sendMsgProcessor.send(msg, timeoutInSec);
    }

    @Override
    public synchronized void start() {
        if (alreadyStart()) {
            return;
        }
        for (LifeCycle lifeCycle : lifeCycles) {
            log.info("lifeCycle start {}", lifeCycle.getClass().getName());
            lifeCycle.start();
        }

        markStart();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @Override
    public void stop() {
        if (alreadyStart()) {
            log.warn("stop {}", lifeCycles);
            lifeCycles.forEach(LifeCycle::stop);
            markStop();
        }
    }


    private boolean notStart() {
        return !start;
    }

    private boolean alreadyStart() {
        return start;
    }

    private void markStart() {
        start = true;
    }

    private void markStop() {
        start = false;
    }


}
