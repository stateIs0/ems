package cn.think.github.simple.stream.api.spi;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.Msg;

import java.util.List;
import java.util.Set;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public interface Broker extends LifeCycle {

    /**
     * 获取当前实例订阅的 group 组
     */
    Set<String> getSubGroups();

    /**
     * 注册 task, 所有 task 统一 Broker 管理生命周期.
     */
    void resisterTask(LifeCycle lifeCycle);

    /**
     * 心跳续约;
     */
    void renew(String clientId, String groupName, String topicName);

    /**
     * client 下线
     */
    void down(String clientId);

    /**
     * 普通的广播消息
     */
    List<Msg> pullMsg(String topicName, long maxOffset);

    /**
     * producer 发送消息接口
     */
    String send(Msg msg, int timeoutInSec);

    /**
     * consumer 获取消息接口
     */
    List<Msg> pullMsg(String topicName, String group, String clientId, int timeoutInSec);

    /**
     * consumer ack 接口
     */
    boolean ack(List<Msg> list, String group, String clientId, int timeoutInSec);
}
