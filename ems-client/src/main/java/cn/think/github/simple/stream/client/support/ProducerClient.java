package cn.think.github.simple.stream.client.support;

import cn.think.github.simple.stream.api.LifeCycle;
import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.SendResult;

public interface ProducerClient extends LifeCycle {

    SendResult saveMsg(Msg msg);
}
