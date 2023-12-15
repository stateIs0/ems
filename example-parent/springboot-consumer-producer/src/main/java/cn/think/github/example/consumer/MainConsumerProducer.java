package cn.think.github.example.consumer;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"cn.think.github.example.consumer", "cn.think.github"})
@MapperScan(value = {"cn.think.github.dal"})
public class MainConsumerProducer {
    public static void main(String[] args) {
        SpringApplication.run(MainConsumerProducer.class);
    }

}