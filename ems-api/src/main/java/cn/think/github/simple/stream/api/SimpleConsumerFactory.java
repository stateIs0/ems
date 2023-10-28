package cn.think.github.simple.stream.api;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public interface SimpleConsumerFactory {

    SimpleConsumer create(String group, String topicName, int threads, GroupType groupType);
}
