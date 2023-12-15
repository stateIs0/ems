package cn.think.github.simple.stream.mybatis.plus.impl.util;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/28
 **/
public class StringUtil {

    public static boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static boolean notSame(String a, String b) {
        return !a.equals(b);
    }

    public static boolean isNotEmpty(String s) {
        return !isEmpty(s);
    }
}
