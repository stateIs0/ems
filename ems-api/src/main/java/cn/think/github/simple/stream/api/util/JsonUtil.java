package cn.think.github.simple.stream.api.util;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/19
 **/
public interface JsonUtil {

    String write(Object o);

    <T> T read(String json, Class<T> c);
}
