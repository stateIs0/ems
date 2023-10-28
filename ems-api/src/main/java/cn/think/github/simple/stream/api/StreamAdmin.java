package cn.think.github.simple.stream.api;

import cn.think.github.simple.stream.api.monitor.MonitorGroup;
import cn.think.github.simple.stream.api.monitor.MonitorTopic;

import java.util.List;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public interface StreamAdmin {

    boolean isAutoCreateTopicOrGroup();

    boolean existTopic(String topic);

    boolean createTopic(String topic, int type);

    boolean createTopic(String topic);

    boolean deleteTopic(String topic);

    int getTopicRule(String topic);

    boolean setTopicRule(String t, int rule);

    boolean resetTopicOffset(String topic, long offset);

    /***************************************************************************************/
    /***************************************************************************************/
    /***************************************************************************************/
    /***************************************************************************************/
    /***************************************************************************************/

    boolean existGroup(String group);

    boolean createGroup(String groupName, String topicName, GroupType groupType);

    boolean deleteGroup(String group);

    boolean isBroadcast(String groupName);

    List<MonitorGroup> getGroupForTopic(String topic);

    List<MonitorTopic> monitorAll();
}
