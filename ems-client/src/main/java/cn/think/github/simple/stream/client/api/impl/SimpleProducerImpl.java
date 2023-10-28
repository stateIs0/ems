package cn.think.github.simple.stream.client.api.impl;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.SendResult;
import cn.think.github.simple.stream.api.SimpleProducer;
import cn.think.github.simple.stream.client.impl.ProducerClientImpl;
import cn.think.github.simple.stream.client.support.ProducerClient;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
public class SimpleProducerImpl implements SimpleProducer {

    ProducerClient producerClient = new ProducerClientImpl();

    @Override
    public SendResult send(Msg msg) {
        return producerClient.saveMsg(msg);
    }

    @Override
    public void start() {
        producerClient.start();
    }

    @Override
    public void stop() {
        producerClient.stop();
    }
}
