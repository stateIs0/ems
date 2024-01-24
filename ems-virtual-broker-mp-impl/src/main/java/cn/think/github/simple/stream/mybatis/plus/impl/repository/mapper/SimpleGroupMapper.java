package cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleGroup;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/8/31
 **/
@Mapper
public interface SimpleGroupMapper extends BaseMapper<SimpleGroup> {

    @Select(value = "select last_offset from ems_simple_group where group_name = #{group}")
    String selectLastOffset(@Param("group") String group);

}
