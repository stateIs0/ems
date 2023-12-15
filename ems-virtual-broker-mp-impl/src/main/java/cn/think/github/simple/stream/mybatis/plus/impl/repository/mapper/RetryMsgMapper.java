package cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.RetryMsg;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.Date;
import java.util.List;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Mapper
public interface RetryMsgMapper extends BaseMapper<RetryMsg> {

    /**
     * 正常的重试消息;
     */
    @Select("select id, old_topic_name, retry_topic_name, group_name, offset, " +
            "consumer_times, next_consumer_time, client_id, state"+
            " from ems_retry_msg where state = 1 and consumer_times <= 16 and next_consumer_time <= #{date} " +
            "and group_name = #{group} order by next_consumer_time limit 100")
    List<RetryMsg> selectRetryMsg(@Param("date") Date date, @Param("group") String group);
}
