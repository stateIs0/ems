package cn.think.github.simple.stream.mybatis.plus.impl.repository.dao;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@ToString
@TableName("ems_topic_group_log")
@AllArgsConstructor
@NoArgsConstructor
public class TopicGroupLog extends BaseDO {

    public static final int STATE_START = 1;
    public static final int STATE_DONE = 2;
    public static final int STATE_RETRY = 3;
    public static final int STATE_EMPTY = 4;
    // pull 时没找到消息, 需要重试, 但又不是真正的重试, 通常是并发导致的空隙消息
    public static final int STATE_NOT_FOUND_WITH_RETRY = 5;

    String topicName;

    String groupName;

    String clientId;

    Long physicsOffset;

    Integer state;

    String errorMsg;

}
