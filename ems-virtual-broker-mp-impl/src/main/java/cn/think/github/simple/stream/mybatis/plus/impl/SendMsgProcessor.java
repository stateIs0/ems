package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.MsgService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @Author cxs
 * @Description
 * @date 2023/12/7
 * @version 1.0
 **/
@Slf4j
@Service
public class SendMsgProcessor {

    @Resource
    private MsgService msgService;

    public String send(Msg msg, int timeoutInSec) {
        String insert = msgService.insert(msg, timeoutInSec);
        log.debug("send msg {} {}", msg.getTopic(), insert);
        return insert;
    }
}
