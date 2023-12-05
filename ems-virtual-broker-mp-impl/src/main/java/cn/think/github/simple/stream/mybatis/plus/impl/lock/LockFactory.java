package cn.think.github.simple.stream.mybatis.plus.impl.lock;

import cn.think.github.simple.stream.api.spi.StreamLock;
import cn.think.github.simple.stream.api.util.SpiFactory;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
public class LockFactory {


    public static StreamLock get() {
        return SpiFactory.getInstance().getObj(StreamLock.class).get();
    }
}
