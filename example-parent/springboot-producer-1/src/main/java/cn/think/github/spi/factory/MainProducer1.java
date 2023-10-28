package cn.think.github.spi.factory;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.SendResult;
import cn.think.github.simple.stream.api.SimpleProducer;
import cn.think.github.simple.stream.api.SimpleProducerFactory;
import cn.think.github.simple.stream.client.api.impl.SimpleProducerImpl;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@SpringBootApplication(scanBasePackages = {"cn.think.github.spi.factory", "cn.think.github.dal"})
@MapperScan(value = {"cn.think.github.dal"})
@RequestMapping("/")
@RestController
public class MainProducer1 {
    public static void main(String[] args) {
        SpringApplication.run(MainProducer1.class);
    }

    @GetMapping("/hi")
    public String hi() {
        SimpleProducer producer = new SimpleProducerImpl();
        producer.start();
        Properties properties = new Properties();
        properties.put("1", "2");
        SendResult sendResult = producer.send(Msg.builder().topic("TopicA")
                .properties(properties)
                .body("hello-world").build());
        System.out.println(sendResult.getMsgId());
        return "hi";
    }
}