package cn.think.github.simple.stream.api.monitor;

import lombok.Builder;
import lombok.Data;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Data
@Builder
public class MonitorClient {

    String groupName;

    String clientId;

    String renewTime;
}
