package cn.think.github.simple.stream.mybatis.plus.impl.lock;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
public class LockKeyFixString {

    private static String SP = "#";

    public static String saveMsgKey(String topic) {
        return topic + SP + "save";
    }

    public static String getGroupOpKey(String topic, String group) {
        return topic + SP + group + SP + "group_op";
    }
}
