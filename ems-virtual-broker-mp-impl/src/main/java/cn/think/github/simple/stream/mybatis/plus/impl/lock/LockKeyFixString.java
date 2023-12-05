package cn.think.github.simple.stream.mybatis.plus.impl.lock;

import cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
public class LockKeyFixString {

    public static String saveMsgKey(String topic) {
        return String.format(RedisKeyFixString.SAVE_MSG_LOCK_KEY, topic);
    }

    public static String getGroupOpKey(String topic, String group) {
        return String.format(RedisKeyFixString.GROUP_OP_LOCK_KEY, topic, group);
    }
}
