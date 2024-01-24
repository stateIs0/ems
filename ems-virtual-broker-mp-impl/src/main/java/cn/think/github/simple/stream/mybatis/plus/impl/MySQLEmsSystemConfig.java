package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleStreamSystemConfig;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleStreamSystemConfigMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.springframework.stereotype.Service;

import static cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.base.BaseDO.DELETED_EXIST;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/25
 **/
@Service
public class MySQLEmsSystemConfig {

    SimpleStreamSystemConfigMapper mapper;

    public MySQLEmsSystemConfig(SimpleStreamSystemConfigMapper mapper) {
        this.mapper = mapper;
    }

    public boolean autoCreteTopic() {
        SimpleStreamSystemConfig autoCreteTopic = mapper.selectOne(new LambdaQueryWrapper<SimpleStreamSystemConfig>()
                .eq(SimpleStreamSystemConfig::getSimpleKey, "autoCreteTopic")
                .eq(SimpleStreamSystemConfig::getDeleted, DELETED_EXIST));
        // 默认自动创建
        return autoCreteTopic == null || Boolean.parseBoolean(autoCreteTopic.getSimpleValue());
    }

    public int msgMaxSizeInBytes() {
        SimpleStreamSystemConfig systemConfig = mapper.selectOne(new LambdaQueryWrapper<SimpleStreamSystemConfig>()
                .eq(SimpleStreamSystemConfig::getSimpleKey, "msgMaxSizeInBytes")
                .eq(SimpleStreamSystemConfig::getDeleted, DELETED_EXIST));
        return systemConfig == null ? 1024 * 1024 * 4 : Integer.parseInt(systemConfig.getSimpleValue());
    }

    public int consumerBatchSize() {
        SimpleStreamSystemConfig systemConfig = mapper.selectOne(new LambdaQueryWrapper<SimpleStreamSystemConfig>()
                .eq(SimpleStreamSystemConfig::getSimpleKey, "consumerBatchSize")
                .eq(SimpleStreamSystemConfig::getDeleted, DELETED_EXIST));
        return systemConfig == null ? 1 : Integer.parseInt(systemConfig.getSimpleValue());
    }

    public int consumerInitThreads() {
        SimpleStreamSystemConfig systemConfig = mapper.selectOne(new LambdaQueryWrapper<SimpleStreamSystemConfig>()
                .eq(SimpleStreamSystemConfig::getSimpleKey, "consumerInitThreads")
                .eq(SimpleStreamSystemConfig::getDeleted, DELETED_EXIST));
        return systemConfig == null ? 5 : Integer.parseInt(systemConfig.getSimpleValue());
    }

    public int consumerRetryMaxTimes() {
        SimpleStreamSystemConfig systemConfig = mapper.selectOne(new LambdaQueryWrapper<SimpleStreamSystemConfig>()
                .eq(SimpleStreamSystemConfig::getSimpleKey, "consumerRetryMaxTimes")
                .eq(SimpleStreamSystemConfig::getDeleted, DELETED_EXIST));
        return systemConfig == null ? 16 : Integer.parseInt(systemConfig.getSimpleValue());
    }

    public int emsBrokerPullMaxWaitTimes() {
        SimpleStreamSystemConfig systemConfig = mapper.selectOne(new LambdaQueryWrapper<SimpleStreamSystemConfig>()
                .eq(SimpleStreamSystemConfig::getSimpleKey, "emsBrokerPullMaxWaitTimes")
                .eq(SimpleStreamSystemConfig::getDeleted, DELETED_EXIST));
        return systemConfig == null ? 10 : Integer.parseInt(systemConfig.getSimpleValue());
    }
}
