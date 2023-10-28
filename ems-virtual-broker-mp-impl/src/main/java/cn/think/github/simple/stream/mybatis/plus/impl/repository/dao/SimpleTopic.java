package cn.think.github.simple.stream.mybatis.plus.impl.repository.dao;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@ToString
@TableName("ems_simple_topic")
public class SimpleTopic extends BaseDO {


    private String topicName;

    /**
     * 普通/重试/死信
     * 1. 普通
     * 2. 重试
     * 3. 私信
     */
    private Integer type;

    /**
     * topic 权限, 默认 1
     * int RULE_FULL = 1;
     * int RULE_WRITE = 2;
     * int RULE_READ = 3;
     * int RULE_DENY = 4;
     */
    private Integer rule;
}
