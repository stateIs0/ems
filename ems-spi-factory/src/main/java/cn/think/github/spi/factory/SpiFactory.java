package cn.think.github.spi.factory;

import java.util.ServiceLoader;
import java.util.function.Supplier;

public interface SpiFactory {

    SpiFactory I = new SpiFactory() {
        @Override
        public <T> Supplier<T> getObj(Class<T> c) {
            for (T t : ServiceLoader.load(c)) {
                return () -> t;
            }
            return () -> null;
        }
    };

    /**
     * 可以用 Springboot 容器作为 factory.
     */
    static SpiFactory getInstance() {
        for (SpiFactory spiFactory : ServiceLoader.load(SpiFactory.class)) {
            return spiFactory;
        }

        return I;
    }

    /**
     * 懒加载返回.
     */
    <T> Supplier<T> getObj(Class<T> c);
}
