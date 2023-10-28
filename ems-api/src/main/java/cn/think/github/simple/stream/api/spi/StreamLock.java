package cn.think.github.simple.stream.api.spi;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
public interface StreamLock {


    /**
     * @see this#lockAndExecute(Callback, String, int, long)
     */
    <T> T lockAndExecute(Callback<T> callback, String key, int timeoutInSec) throws Throwable;

    /**
     * 锁住并执行模板方法.
     *
     * @param callback       回调
     * @param key            key
     * @param timeoutInSec   秒
     * @param leaseTimeInSec 续约
     * @param <T>            泛型
     * @return 结果
     * @throws Throwable 异常
     */
    <T> T lockAndExecute(Callback<T> callback, String key, int timeoutInSec, long leaseTimeInSec) throws Throwable;

    interface Callback<T> {
        T execute();
    }
}
