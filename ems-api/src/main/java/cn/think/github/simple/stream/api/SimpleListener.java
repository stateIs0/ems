package cn.think.github.simple.stream.api;

import java.util.List;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public interface SimpleListener {

    ConsumerResult consumer(List<Msg> msgList);
}
