package cn.think.github.spi.factory;

import java.util.concurrent.ThreadFactory;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/18
 **/
public class NamedThreadFactory implements ThreadFactory {

    String name;
    int num;

    public NamedThreadFactory(String namePrefix) {
        this.name = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + "-" + ++num);
        t.setDaemon(true);
        return t;
    }
}
