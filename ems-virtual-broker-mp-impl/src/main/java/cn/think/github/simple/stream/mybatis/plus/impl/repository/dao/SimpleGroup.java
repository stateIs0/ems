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
@TableName("ems_simple_group")
public class SimpleGroup extends BaseDO {

    public static String GROUP_TYPE_CLUSTER = "CLUSTER";
    public static String GROUP_TYPE_BROADCASTING = "BROADCASTING";

    private String topicName;

    private String groupName;

    /**
     * CLUSTER
     * BROADCASTING
     */
    private String groupType;
}
