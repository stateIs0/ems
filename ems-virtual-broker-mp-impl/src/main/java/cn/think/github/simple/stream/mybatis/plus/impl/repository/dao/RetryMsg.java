package cn.think.github.simple.stream.mybatis.plus.impl.repository.dao;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@EqualsAndHashCode(callSuper = true)
@Data
@TableName("ems_retry_msg")
public class RetryMsg extends BaseDO {

    public static int STATE_INIT = 1;
    public static int STATE_SUCCESS = 2;
    public static int STATE_PROCESSING = 3;// 处理中.

    String oldTopicName;

    // "%RETRY%" + group
    String retryTopicName;

    String groupName;

    Long offset;

    Integer consumerTimes;

    /**
     * 下次消费时间.
     */
    Date nextConsumerTime;

    /**
     * 状态
     * 1. 重试阶段
     * 2. 重试成功;
     */
    Integer state;

    String clientId;


}
