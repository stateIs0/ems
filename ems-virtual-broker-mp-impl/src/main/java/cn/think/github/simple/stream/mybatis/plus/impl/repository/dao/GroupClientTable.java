package cn.think.github.simple.stream.mybatis.plus.impl.repository.dao;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

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
@TableName("ems_group_client_table")
public class GroupClientTable extends BaseDO {

    public static int state_up = 1;
    public static int state_down = 2;

    String groupName;

    String clientId;

    Date renewTime;

    Integer state;

    Long lastOffset;

    Date lastConsumerTime;
}
