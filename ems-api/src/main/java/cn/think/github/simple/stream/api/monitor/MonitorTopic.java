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
public class MonitorTopic {

    String topicName;

    Long maxOffset;

    Long minOffset;

    List<MonitorGroup> groups;
}
