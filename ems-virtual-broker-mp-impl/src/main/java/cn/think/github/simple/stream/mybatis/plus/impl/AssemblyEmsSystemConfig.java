package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.spi.RedisClient;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/24
 **/
@Service
public class AssemblyEmsSystemConfig implements EmsSystemConfig {

    @Resource
    MySQLEmsSystemConfig mySQLEmsSystemConfig;
    @Resource
    RedisClient redisClient;

    @Override
    public boolean autoCreateTopic() {
        String autoCreteTopic = redisClient.get("autoCreateTopic");
        if (autoCreteTopic == null) {
            boolean autoCreteTopicBoo = mySQLEmsSystemConfig.autoCreteTopic();
            redisClient.set("autoCreateTopic", String.valueOf(autoCreteTopicBoo));
            return autoCreteTopicBoo;
        }
        return Boolean.parseBoolean(autoCreteTopic);
    }

    @Override
    public int msgMaxSizeInBytes() {
        String msgMaxSizeInBytes = redisClient.get("msgMaxSizeInBytes");
        if (msgMaxSizeInBytes == null) {
            int msgMaxSizeInBytesInt = mySQLEmsSystemConfig.msgMaxSizeInBytes();
            redisClient.set("msgMaxSizeInBytes", String.valueOf(msgMaxSizeInBytesInt));
            return msgMaxSizeInBytesInt;
        }
        return Integer.parseInt(msgMaxSizeInBytes);
    }

    @Override
    public int consumerBatchSize() {
        String consumerBatchSize = redisClient.get("consumerBatchSize");
        if (consumerBatchSize == null) {
            int consumerBatchSizeInt = mySQLEmsSystemConfig.consumerBatchSize();
            redisClient.set("consumerBatchSize", String.valueOf(consumerBatchSizeInt));
            return consumerBatchSizeInt;
        }
        return Integer.parseInt(consumerBatchSize);
    }

    @Override
    public int consumerThreads() {
        String consumerThreads = redisClient.get("consumerThreads");
        if (consumerThreads == null) {
            int consumerThreadsInt = mySQLEmsSystemConfig.consumerInitThreads();
            redisClient.set("consumerThreads", String.valueOf(consumerThreadsInt));
            return consumerThreadsInt;
        }
        return Integer.parseInt(consumerThreads);
    }

    @Override
    public int consumerRetryMaxTimes() {
        String consumerRetryMaxTimes = redisClient.get("consumerRetryMaxTimes");
        if (consumerRetryMaxTimes == null) {
            int consumerRetryMaxTimesInt = mySQLEmsSystemConfig.consumerRetryMaxTimes();
            redisClient.set("consumerRetryMaxTimes", String.valueOf(consumerRetryMaxTimesInt));
            return consumerRetryMaxTimesInt;
        }
        return Integer.parseInt(consumerRetryMaxTimes);
    }
}
