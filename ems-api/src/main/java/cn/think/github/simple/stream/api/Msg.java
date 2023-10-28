package cn.think.github.simple.stream.api;

import lombok.Builder;
import lombok.Data;

import java.util.Properties;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Data
@Builder
public class Msg {

    private String realTopic;

    private String topic;

    private String msgId;

    private String body;

    private int consumerTimes;

    private String tags;

    private Properties properties;

    private long offset;

    private boolean receiveLater;

}
