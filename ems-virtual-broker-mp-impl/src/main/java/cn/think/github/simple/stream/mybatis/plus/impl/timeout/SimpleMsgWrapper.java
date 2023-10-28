package cn.think.github.simple.stream.mybatis.plus.impl.timeout;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleMsg;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/11
 **/
@AllArgsConstructor
@Data
@Builder
public class SimpleMsgWrapper {

    SimpleMsg simpleMsg;

    // "%RETRY%" + group
    String realTopic;

    int consumerTimes;
}
