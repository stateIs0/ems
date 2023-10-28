package cn.think.github.spi.factory;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"cn.think.github.spi.factory", "cn.think.github.dal"})
@MapperScan(value = {"cn.think.github.dal"})
public class MainConsumer3 {
    public static void main(String[] args) {
        SpringApplication.run(MainConsumer3.class);
    }

}