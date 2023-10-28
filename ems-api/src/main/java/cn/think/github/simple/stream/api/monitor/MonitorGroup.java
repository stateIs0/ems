package cn.think.github.simple.stream.api.monitor;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Data
@Builder
public class MonitorGroup {

    String topicName;

    String groupName;

    Long maxOffset;

    Long minOffset;

    /**
     * 积压
     */
    Long stock;

    List<MonitorClient> clients;
}
