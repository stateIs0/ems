package cn.think.github.simple.stream.api;

import lombok.Data;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Data
public class SendResult {

    private boolean success;

    private String msgId;

    public SendResult(boolean success) {
        this.success = success;
    }
}
