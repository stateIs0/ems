package cn.think.github.spi.factory;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.SendResult;
import cn.think.github.simple.stream.api.SimpleProducer;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/1
 **/
@Slf4j
@Service
public class MyApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

    public void main() {

        initFlowRules();

        SimpleProducer producer = new SimpleProducerImpl();
        producer.start();

        StringBuilder b = new StringBuilder("a");
        for (int i = 0; i < 1000; i++) {
            b.append("a");
        }

        for (int i = 0; i < 25; i++) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 200; i++) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(980));
                        try (Entry entry = SphU.entry("send")) {
                            producer.send(Msg.builder().topic("TopicA").body(b.toString()).build());
                        } catch (BlockException ex) {
                            //System.out.println("blocked!");
                        }
                    }

                }
            }).start();
        }

        for (int i = 0; i < 25; i++) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 200; i++) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(980));
                        try (Entry entry = SphU.entry("send")) {
                            // 被保护的逻辑
                            SendResult send = producer.send(Msg.builder().topic("TopicB").body(b.toString()).build());
                        } catch (BlockException ex) {
                            // 处理被流控的逻辑
                            // System.out.println("blocked!");
                        }
                    }

                }
            }).start();
        }

    }

    private static void initFlowRules() {
        List<FlowRule> rules = new ArrayList<>();
        FlowRule rule = new FlowRule();
        rule.setResource("send");
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        // Set limit QPS to 20.
        rule.setCount(500);
        rules.add(rule);
        FlowRuleManager.loadRules(rules);
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        main();
    }
}
