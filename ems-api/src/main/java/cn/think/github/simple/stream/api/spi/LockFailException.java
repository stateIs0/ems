package cn.think.github.simple.stream.api.spi;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
public class LockFailException extends RuntimeException {

    public LockFailException(String message) {
        super(message);
    }
}
