package cn.think.github.simple.stream.mybatis.plus.impl.repository.dao;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Data
@TableName("ems_simple_stream_system_config")
public class SimpleStreamSystemConfig extends BaseDO {

    String simpleKey;

    String simpleValue;
}
