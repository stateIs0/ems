package cn.think.github.simple.stream.client.support;

import java.util.Collection;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/27
 **/
public class CollectionUtils {

    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNotEmpty(Collection<?> collection) {
        return !isEmpty(collection);
    }
}
