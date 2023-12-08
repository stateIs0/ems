package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.EmsSystemConfig;
import cn.think.github.simple.stream.api.GroupType;
import cn.think.github.simple.stream.api.StreamAdmin;
import cn.think.github.simple.stream.api.monitor.MonitorClient;
import cn.think.github.simple.stream.api.monitor.MonitorGroup;
import cn.think.github.simple.stream.api.monitor.MonitorTopic;
import cn.think.github.simple.stream.api.simple.util.TopicConstant;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.GroupClientTableProcessor;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.LogService;
import cn.think.github.simple.stream.mybatis.plus.impl.crud.services.MsgService;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockKeyFixString;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleGroup;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleTopic;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.TopicGroupLog;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleGroupMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleTopicMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StreamAdminImpl implements StreamAdmin {

    @Resource
    SimpleTopicMapper topicMapper;
    @Resource
    SimpleGroupMapper groupMapper;
    @Resource
    LogService logService;
    @Resource
    GroupClientTableProcessor clientTableService;
    @Resource
    MsgService msgService;
    @Resource
    EmsSystemConfig emsSystemConfig;

    @Override
    public boolean isBroadcast(String groupName) {
        SimpleGroup simpleGroup = groupMapper.selectOne(new LambdaQueryWrapper<SimpleGroup>().eq(SimpleGroup::getGroupName, groupName));
        if (simpleGroup == null) {
            throw new RuntimeException("ems tip ---> simpleGroup 错误, 不存在. " + groupName);
        }
        String groupType = simpleGroup.getGroupType();
        return groupType.equals(SimpleGroup.GROUP_TYPE_BROADCASTING);
    }

    @Override
    public boolean isAutoCreateTopicOrGroup() {
        return emsSystemConfig.autoCreateTopic();
    }

    @Override
    public synchronized boolean existTopic(String topic) {
        SimpleTopic simpleTopic = topicMapper.selectOne(new QueryWrapper<SimpleTopic>().lambda()
                .eq(SimpleTopic::getTopicName, topic)
                .eq(SimpleTopic::getDeleted, 0));

        return simpleTopic != null;
    }

    @Override
    public boolean existGroup(String group) {
        List<SimpleGroup> groups = groupMapper.selectList(new QueryWrapper<SimpleGroup>().lambda()
                .eq(SimpleGroup::getGroupName, group));

        return groups != null && !groups.isEmpty();
    }

    @Override
    public boolean reviewSubRelation(String topic, String group) {
        List<SimpleGroup> groups = groupMapper.selectList(new QueryWrapper<SimpleGroup>().lambda()
                .eq(SimpleGroup::getGroupName, group));
        // 空的, 不合法
        if (groups.isEmpty()) {
            log.warn("ems tip ---> groups is isEmpty {}", group);
            return false;
        }
        Optional<SimpleGroup> first = groups.stream().findFirst();
        // 数据库的 topicName 和实际订阅的 topic name 不一致, 不合法.
        Optional<String> topicDb = first.map(SimpleGroup::getTopicName);
        Optional<Boolean> optional = first.map(a -> a.getTopicName().equals(topic));
        if (!optional.get()) {
            log.warn("ems tip ---> group {}, db topic {}, sub topic {}", group, topicDb.orElse(null), topic);
        }
        return optional.get();
    }

    @Override
    public boolean createTopic(String topic, int type) {
        if (existTopic(topic)) {
            log.info("ems tip ---> topic exist {}", topic);
            return false;
        }
        SimpleTopic simpleTopic = new SimpleTopic();
        simpleTopic.setTopicName(topic);
        simpleTopic.setRule(TopicConstant.RULE_FULL);
        simpleTopic.setType(type);
        simpleTopic.setCreateTime(new Date());
        simpleTopic.setUpdateTime(new Date());
        try {
            topicMapper.insert(simpleTopic);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return false;
        }
        log.warn("ems tip ---> create topic {} success", topic);
        return true;
    }

    @Override
    public boolean createTopic(String topic) {
        return createTopic(topic, TopicConstant.TYPE_NORMAL);
    }

    @Override
    public int getTopicRule(String topic) {
        SimpleTopic simpleTopic = topicMapper.selectOne(new LambdaQueryWrapper<SimpleTopic>()
                .eq(SimpleTopic::getTopicName, topic));
        if (simpleTopic == null) {
            return -1;
        }
        return simpleTopic.getRule();
    }

    @Override
    public boolean createGroup(String groupName, String topicName, GroupType groupType) {
        if (existGroup(groupName)) {
            log.info("ems tip ---> group exist {}", groupName);
            return false;
        }
        SimpleGroup s = new SimpleGroup();
        s.setGroupName(groupName);
        s.setTopicName(topicName);
        s.setGroupType(groupType.name());
        s.setCreateTime(new Date());
        s.setUpdateTime(new Date());
        groupMapper.insert(s);
        return true;
    }

    @Override
    public boolean deleteTopic(String topic) {
        if (!existTopic(topic)) {
            log.info("ems tip ---> topic exist {}", topic);
            return false;
        }
        int delete = topicMapper.delete(new QueryWrapper<SimpleTopic>().lambda().eq(SimpleTopic::getTopicName, topic));
        return delete > 0;
    }

    @Override
    public boolean deleteGroup(String group) {
        if (!existTopic(group)) {
            log.info("ems tip ---> group exist {}", group);
            return false;
        }
        int delete = groupMapper.delete(new QueryWrapper<SimpleGroup>().lambda().eq(SimpleGroup::getGroupName, group));
        return delete > 0;
    }

    @Override
    public boolean resetTopicOffset(String topic, long offset) {
        List<SimpleGroup> simpleGroups = groupMapper.selectList(
                new LambdaQueryWrapper<>(SimpleGroup.class).eq(SimpleGroup::getTopicName, topic));
        List<String> collect = simpleGroups.stream().map(SimpleGroup::getGroupName).collect(Collectors.toList());

        for (String group : collect) {
            String groupOpKey = LockKeyFixString.getGroupOpKey(topic, group);
            try {
                LockFactory.get().lockAndExecute(() -> {
                    Long maxLog = logService.getLogMaxOffset(topic, group);
                    if (maxLog == null) {
                        return null;
                    }
                    int r = logService.updateOffset(group, offset);
                    logService.setMaxLogOffset(topic, group, offset);
                    log.warn("ems tip ---> 重置 topic {} group {} offset {}, 当前最大 {}, 结果 = {}",
                            topic, group, offset, maxLog, r > 0);
                    return null;
                }, groupOpKey, 10);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @Override
    public boolean setTopicRule(String t, int rule) {
        SimpleTopic simpleTopic = topicMapper.selectOne(new LambdaQueryWrapper<SimpleTopic>().eq(SimpleTopic::getTopicName, t));
        if (simpleTopic == null) {
            return false;
        }
        if (rule > TopicConstant.RULE_DENY || rule < TopicConstant.RULE_FULL) {
            return false;
        }
        simpleTopic.setRule(rule);
        int id = topicMapper.updateById(simpleTopic);
        return id <= 0;
    }

    @Override
    public List<MonitorGroup> getGroupForTopic(String topic) {
        List<SimpleGroup> simpleGroups = groupMapper.selectList(
                new LambdaQueryWrapper<>(SimpleGroup.class).eq(SimpleGroup::getTopicName, topic));
        List<String> collect = simpleGroups.stream().map(SimpleGroup::getGroupName).collect(Collectors.toList());

        Long max = msgService.getMsgMaxOffset(topic);

        List<MonitorGroup> r = new ArrayList<>();
        for (String group : collect) {
            Long maxLog = logService.getLogMaxOffset(topic, group);
            TopicGroupLog minLog = logService.getMinLog(topic, group);

            r.add(MonitorGroup.builder()
                    .topicName(topic)
                    .groupName(group)
                    .maxOffset(maxLog)
                    .minOffset(minLog.getPhysicsOffset())
                    .stock(max - maxLog)
                    .clients(clientTableService
                            .getClientList(group)
                            .stream()
                            .map(a -> MonitorClient.builder()
                                    .clientId(a.getClientId())
                                    .groupName(group)
                                    .renewTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(a.getRenewTime()))
                                    .build())
                            .collect(Collectors.toList()))
                    .build()
            );
        }
        return r;
    }

    @Override
    public List<MonitorTopic> monitorAll() {
        List<SimpleTopic> all = topicMapper.selectList(new LambdaQueryWrapper<>());
        List<MonitorTopic> r = new ArrayList<>();
        for (SimpleTopic t : all) {
            List<MonitorGroup> groupForTopic = getGroupForTopic(t.getTopicName());

            r.add(MonitorTopic.builder()
                    .groups(groupForTopic)
                    .topicName(t.getTopicName())
                    .maxOffset(msgService.getMsgMaxOffset(t.getTopicName()))
                    .minOffset(msgService.getMin(t.getTopicName()))
                    .build()
            );
        }

        return r;
    }
}
