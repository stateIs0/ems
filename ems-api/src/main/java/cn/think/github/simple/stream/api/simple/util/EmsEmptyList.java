package cn.think.github.simple.stream.api.simple.util;

import lombok.Getter;

import java.util.ArrayList;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/10/19
 **/
@Getter
public class EmsEmptyList<E> extends ArrayList<E> {

    long maxOffset;

    public EmsEmptyList(long maxOffset) {
        super();
        this.maxOffset = maxOffset;
    }

}
