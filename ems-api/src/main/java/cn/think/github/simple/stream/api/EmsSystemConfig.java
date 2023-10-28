package cn.think.github.simple.stream.api;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/24
 **/
public interface EmsSystemConfig {

    boolean autoCreateTopic();

    int msgMaxSizeInBytes();

    int consumerBatchSize();

    int consumerThreads();

    int consumerRetryMaxTimes();
}
