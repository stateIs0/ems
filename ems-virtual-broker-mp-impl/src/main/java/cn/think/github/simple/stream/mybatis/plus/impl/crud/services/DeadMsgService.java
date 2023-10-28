package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.DeadMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.DeadMsgMapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Service
public class DeadMsgService extends ServiceImpl<DeadMsgMapper, DeadMsg> {

    public void save(String topic, String group, String offset, int consumerTimes) {
        DeadMsg deadMsg = new DeadMsg();
        deadMsg.setOffset(offset);
        deadMsg.setGroupName(group);
        deadMsg.setConsumerTimes(consumerTimes);
        deadMsg.setTopicName(topic);
        deadMsg.setCreateTime(new Date());
        deadMsg.setUpdateTime(new Date());
        baseMapper.insert(deadMsg);
    }
}
