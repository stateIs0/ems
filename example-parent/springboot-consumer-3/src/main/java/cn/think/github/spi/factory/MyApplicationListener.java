package cn.think.github.spi.factory;

import cn.think.github.dal.RunningLog;
import cn.think.github.dal.RunningLogService;
import cn.think.github.simple.stream.api.*;
import cn.think.github.simple.stream.api.spi.Broker;
import cn.think.github.simple.stream.client.api.impl.SimpleConsumerImpl;
import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;


/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
@Slf4j
@Service
public class MyApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

    static String Consumer = "Consumer3";

    @Resource
    SpiFactory spiFactory;
    @Resource
    Broker broker;
    @Resource
    RunningLogService runningLogService;

    public void main() {

        String topic = "TopicA";
        String group = "Group1";

        broker.start();

        initFlowRules();

        Supplier<StreamAdmin> adminSupplier = spiFactory.getObj(StreamAdmin.class);
        adminSupplier.get().createTopic(topic);
        adminSupplier.get().createGroup(group, topic, GroupType.CLUSTER);

        SimpleConsumer consumer = new SimpleConsumerImpl(group, topic, 4, GroupType.CLUSTER);
        consumer.register(msgList -> {
            for (Msg msg : msgList) {
                System.out.println(msg.getMsgId());
            }
            return ConsumerResult.success();
        });
        consumer.start();

        consumer.register(msgs -> {
            try (Entry ignored = SphU.entry("Group")) {
                Msg m = msgs.get(0);
                save(topic, m, group);
                log.info("msg id =[{}]", m.getMsgId());
                return ConsumerResult.success();
            } catch (BlockException e) {
                e.printStackTrace();
                return ConsumerResult.fail();
            }

        });

        consumer.start();


    }

    private void save(String topic, Msg m, String group) {
        RunningLog runningLog = new RunningLog();
        runningLog.setOffset(topic);
        runningLog.setTopicName(topic);
        runningLog.setConsumerTimes(m.getConsumerTimes());
        runningLog.setGroupName(group);
        runningLog.setOffset(m.getMsgId());
        runningLogService.save(runningLog);
    }

    public void main2() {

        String topic = "TopicB";
        String group = "Group3";

        broker.start();

        initFlowRules();

        Supplier<StreamAdmin> adminSupplier = spiFactory.getObj(StreamAdmin.class);
        adminSupplier.get().createTopic(topic);
        adminSupplier.get().createGroup(group, topic, GroupType.CLUSTER);

        SimpleConsumer consumer = new SimpleConsumerImpl(group, topic, 4, GroupType.CLUSTER);
        consumer.register(msgs -> {
            try (Entry entry = SphU.entry("Group")) {
                Msg m = msgs.get(0);
                save(topic, m, group);
                log.info("msg id =[{}]", m.getMsgId());
                return ConsumerResult.success();
            } catch (BlockException ex) {
                System.out.println("blocked!");
                return ConsumerResult.fail();
            }

        });

        consumer.start();


    }

    private static void initFlowRules() {
        List<FlowRule> rules = new ArrayList<>();
        FlowRule rule = new FlowRule();
        rule.setResource("Group");
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        // Set limit QPS to 20.
        rule.setCount(200000);
        rules.add(rule);
        FlowRuleManager.loadRules(rules);
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        main();
        main2();
    }
}
