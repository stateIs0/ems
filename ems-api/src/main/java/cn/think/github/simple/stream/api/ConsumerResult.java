package cn.think.github.simple.stream.api;

import lombok.Builder;
import lombok.Data;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Data
@Builder
public class ConsumerResult {

    private boolean receiveLater;

    public static ConsumerResult fail() {
        return ConsumerResult.builder().receiveLater(true).build();
    }

    public static ConsumerResult success() {
        return ConsumerResult.builder().receiveLater(false).build();
    }
}
