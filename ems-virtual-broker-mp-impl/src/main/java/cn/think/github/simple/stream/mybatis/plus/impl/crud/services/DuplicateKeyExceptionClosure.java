package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

/**
 * @Author cxs
 * @Description
 * @date 2023/12/20
 * @version 1.0
 **/
@Slf4j
public class DuplicateKeyExceptionClosure {

    private static final String CLASS_NAME = "org.springframework.dao.DuplicateKeyException";

    public static <T> T submit(Callable<T> r, T def) {
        try {
            return r.call();
        } catch (Exception e) {
            if (CLASS_NAME.equals(e.getClass().getName())) {
                // Duplicate entry
                log.debug(e.getMessage(), e);
            } else {
                throw new RuntimeException(e);
            }
        }
        return def;
    }

    public static void execute(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            if (CLASS_NAME.equals(e.getClass().getName())) {
                // Duplicate entry
                log.debug(e.getMessage(), e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
