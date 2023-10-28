package cn.think.github.simple.stream.client;


import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.spi.factory.SpiFactory;

import java.util.function.Supplier;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
public class VirtualBrokerFactory {

    public static Supplier<Broker> get() {
        return SpiFactory.getInstance().getObj(Broker.class);
    }
}
