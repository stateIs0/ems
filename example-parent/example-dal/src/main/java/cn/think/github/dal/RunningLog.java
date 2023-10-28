package cn.think.github.dal;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/24
 **/
@Getter
@Setter
@ToString
@TableName("running_log")
public class RunningLog {

    @TableId(value = "id", type = IdType.AUTO)
    Long id;

    String topicName;

    String groupName;

    String offset;

    String clientId;

    Integer consumerTimes;

    Date createTime;

    public static RunningLog create(String offset, String topicName, String groupName, String clientId, int consumerTimes) {
        RunningLog log = new RunningLog();
        log.setOffset(offset);
        log.setTopicName(topicName);
        log.setGroupName(groupName);
        log.setClientId(clientId);
        log.setConsumerTimes(consumerTimes);
        log.setCreateTime(new Date());
        return log;
    }
}
