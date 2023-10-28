package cn.think.github.simple.stream.client.support;

import cn.think.github.simple.stream.api.GroupType;
import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.SimpleListener;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public interface ConsumerClient extends LifeCycle {

    void setListener(SimpleListener simpleListener, String groupName, String topicName, GroupType groupType);

    void setThreadNum(int num);
}
