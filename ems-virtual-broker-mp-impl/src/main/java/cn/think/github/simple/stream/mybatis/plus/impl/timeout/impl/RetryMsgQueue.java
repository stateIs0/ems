package cn.think.github.simple.stream.mybatis.plus.impl.timeout.impl;

import cn.think.github.simple.stream.mybatis.plus.impl.timeout.SimpleMsgWrapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/18
 **/
@Getter
@Slf4j
@Component
public class RetryMsgQueue {

    Map<String, LinkedBlockingQueue<SimpleMsgWrapper>> queueMaps = new ConcurrentHashMap<>();

    public static String buildKey(String topic, String group) {
        return topic + "#" + group;
    }

    public List<SimpleMsgWrapper> getRetryMsgList(String topicName, String groupName) {
        final LinkedBlockingQueue<SimpleMsgWrapper> simpleMsgQueue = queueMaps.get(buildKey(topicName, groupName));
        if (simpleMsgQueue == null) {
            return new ArrayList<>();
        }
        if (simpleMsgQueue.isEmpty()) {
            return new ArrayList<>();
        }
        // for jdk 21, modify lock
        synchronized (simpleMsgQueue) {
            List<SimpleMsgWrapper> result = new ArrayList<>();
            while (!simpleMsgQueue.isEmpty()) {
                SimpleMsgWrapper poll = simpleMsgQueue.poll();
                if (poll != null) {
                    result.add(poll);
                }
                if (result.size() >= 150) {
                    log.debug("--->>> topicName = {}, groupName = {}, simpleMsgQueue.size = {}", topicName, groupName, simpleMsgQueue.size());
                    break;
                }
            }
            if (!result.isEmpty()) {
                log.debug("-->> ems retry msg count {} {}", topicName, result.size());
            }
            return result;
        }

    }

}
