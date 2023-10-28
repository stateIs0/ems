package cn.think.github.simple.stream.mybatis.plus.impl.repository.dao;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

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
@TableName("ems_simple_msg")
@AllArgsConstructor
@NoArgsConstructor
public class SimpleMsg extends BaseDO {

    private String topicName;

    private String jsonBody;

    private String fromIp;

    private String properties;

    private String tags;

    private Long physicsOffset;

}
