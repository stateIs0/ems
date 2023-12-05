package cn.think.github.simple.stream.mybatis.plus.impl.util;

/**
 * @Author cxs
 * @Description
 * @date 2023/12/5
 * @version 1.0
 **/
public interface RedisKeyFixString {

    /**
     * 这个 group 消费的最大 offset
     */
    String GROUP_LOG_MAX_OFFSET_CACHE = "EMS_GROUP_LOG_MAX_OFFSET_%s_X<->X_%s";

    /**
     * 这个 topic 的最大
     */
    String TOPIC_MAX_OFFSET_CACHE = "EMS_MAX_OFFSET_%s";

    /**
     * 刚刚产生消息的 flag;
     */
    String EMS_JUST_PRODUCED_CACHE = "EMS_JUST_PRODUCED_%s";

    /**
     * 操作这个 topic 和 group 的 lock key
     */
    String GROUP_OP_LOCK_KEY = "EMS_GROUP_OP_KEY_%s_X<->X_%s";

    /**
     * 保佑这个 topic 的 lock key;
     */
    String SAVE_MSG_LOCK_KEY = "EMS_SAVE_MSG_LOCK_KEY_%s";

}
