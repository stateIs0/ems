package cn.think.github.example.consumer;

import cn.think.github.simple.stream.api.StreamAdmin;
import cn.think.github.simple.stream.api.monitor.MonitorTopic;
import cn.think.github.simple.stream.api.util.SpiFactory;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@SpringBootApplication(scanBasePackages = {"cn.think.github.example.consumer", "cn.think.github.dal"})
@MapperScan(value = {"cn.think.github.dal"})
@RestController
@RequestMapping("/base")
public class MainConsumerProducer {
    public static void main(String[] args) {
        SpringApplication.run(MainConsumerProducer.class);
    }


    @RequestMapping("/hello")
    public List<MonitorTopic> hi() {
        return SpiFactory.getInstance().getObj(StreamAdmin.class).get().monitorAll();
    }

    @RequestMapping("/reset")
    public Boolean reset() {
        return SpiFactory.getInstance().getObj(StreamAdmin.class).get().resetTopicOffset("aaa", Integer.MAX_VALUE);
    }

}