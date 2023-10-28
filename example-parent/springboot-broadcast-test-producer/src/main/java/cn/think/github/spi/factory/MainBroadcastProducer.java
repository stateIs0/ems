package cn.think.github.spi.factory;

import cn.think.github.simple.stream.api.StreamAdmin;
import cn.think.github.simple.stream.api.monitor.MonitorTopic;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@SpringBootApplication(scanBasePackages = {"cn.think.github.spi.factory", "cn.think.github.dal"})
@MapperScan(value = {"cn.think.github.dal"})
@RestController
@RequestMapping("/base")
public class MainBroadcastProducer {
    public static void main(String[] args) {
        SpringApplication.run(MainBroadcastProducer.class);
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