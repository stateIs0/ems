package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.api.Msg;
import cn.think.github.simple.stream.api.simple.util.IPv4AddressUtil;
import cn.think.github.simple.stream.api.simple.util.PIDUtil;
import cn.think.github.simple.stream.api.spi.RedisClient;
import cn.think.github.simple.stream.api.util.JsonUtil;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockFactory;
import cn.think.github.simple.stream.mybatis.plus.impl.lock.LockKeyFixString;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.SimpleMsg;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.SimpleMsgMapper;
import cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString;
import cn.think.github.simple.stream.mybatis.plus.impl.util.StringUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static cn.think.github.simple.stream.mybatis.plus.impl.util.RedisKeyFixString.EMS_TOPIC_INCR_KEY;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Service
public class MsgService extends ServiceImpl<SimpleMsgMapper, SimpleMsg> {

    private final Map<String, RedisClient.AtomicLong> memoryCache = new ConcurrentHashMap<>();

    @Resource
    RedisClient redisClient;

    @Resource
    JsonUtil jsonUtil;

    public Long getMsgMaxOffset(String topic) {
        String offset = redisClient.get(buildMsgMaxOffsetRedisKey(topic));
        if (StringUtil.isNotEmpty(offset)) {
            return Long.valueOf(offset);
        }
        SimpleMsg simpleMsg = baseMapper.selectOne
                (new LambdaQueryWrapper<SimpleMsg>()
                        .eq(SimpleMsg::getTopicName, topic)
                        .orderByDesc(SimpleMsg::getPhysicsOffset)
                        .last("limit 1"));
        if (simpleMsg == null) {
            return -1L;
        }
        return simpleMsg.getPhysicsOffset();
    }

    public Long getMin(String t) {
        return baseMapper.selectOne(new LambdaQueryWrapper<SimpleMsg>()
                .eq(SimpleMsg::getTopicName, t)
                .orderBy(true, true, SimpleMsg::getPhysicsOffset)
                .last("limit 1")).getPhysicsOffset();
    }

    public String insert(Msg msg, int timeout) {
        long msgId = getMaxFromCacheWithInsert(msg, timeout);
        // save offset record for consumer.
        // 这里有可能设置的并不是最大的 offset, 但是不想上锁; 30s 缓存失效兜底兜底.
        redisClient.set(buildMsgMaxOffsetRedisKey(msg.getTopic()),
                String.valueOf(msgId),
                30, TimeUnit.SECONDS);

        return String.valueOf(msgId);
    }

    protected long getMaxFromCacheWithInsert(Msg msg, int timeout) {

        RedisClient.AtomicLong rAtomicLong = memoryCache.get(msg.getTopic());
        if (rAtomicLong == null) {
            synchronized (this) {
                if ((rAtomicLong = memoryCache.get(msg.getTopic())) == null) {
                    rAtomicLong = redisClient.getAtomicLong(String.format(EMS_TOPIC_INCR_KEY, msg.getTopic()));
                    memoryCache.put(msg.getTopic(), rAtomicLong);
                }
            }
        }

        if (rAtomicLong.get() == 0) {
            Long msgMax = getMsgMax(msg.getTopic());
            if (msgMax != 0) {
                // redis 可能挂了; 那从数据库里插入, 并获取最新的数据;
                return this.getMaxFromStoreWithInsert(msg, timeout);
            }
        }

        // 插入消息.
        int result = 0;
        long physicsOffset = 0;
        while (result == 0) {
            physicsOffset = rAtomicLong.incrementAndGet();
            SimpleMsg simpleMsg = getSimpleMsg(msg, physicsOffset);
            try {
                result = baseMapper.insert(simpleMsg);
            } catch (Exception e) {
                // redis 故障后,如果是 rdb 恢复的, 可能存在重复 id 的情况.
                if ("org.springframework.dao.DuplicateKeyException".equals(e.getClass().getName())) {
                    // Duplicate entry
                    log.debug(e.getMessage(), e);
                } else {
                    throw e;
                }
            }
        }
        return physicsOffset;
    }

    public static String buildMsgMaxOffsetRedisKey(String topic) {
        return String.format(RedisKeyFixString.TOPIC_MAX_OFFSET_CACHE, topic);
    }


    public long getMaxFromStoreWithInsert(Msg msg, int timeout) {

        String key = LockKeyFixString.saveMsgKey(msg.getTopic());

        try {
            return LockFactory.get().lockAndExecute(() -> {

                SimpleMsg s = baseMapper.selectOne(new QueryWrapper<SimpleMsg>()
                        .lambda()
                        .eq(SimpleMsg::getTopicName, msg.getTopic())
                        .orderByDesc(SimpleMsg::getPhysicsOffset)
                        .last("limit 1"));

                Long newOffset;
                if (s == null) {
                    newOffset = -1L;
                } else {
                    newOffset = s.getPhysicsOffset();
                }
                newOffset = newOffset + 1;
                SimpleMsg simpleMsg = getSimpleMsg(msg, newOffset);
                // 插入消息.
                baseMapper.insert(simpleMsg);
                RedisClient.AtomicLong atomicLong = memoryCache.get(msg.getTopic());
                if (atomicLong.get() < newOffset.floatValue()) {
                    atomicLong.incrementAndGet(newOffset - atomicLong.get());
                }
                return newOffset;
            }, key, timeout);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

    }

    public Long getMsgMax(String topic) {
        SimpleMsg s = baseMapper.selectOne(new QueryWrapper<SimpleMsg>()
                .lambda()
                .eq(SimpleMsg::getTopicName, topic)
                .orderByDesc(SimpleMsg::getPhysicsOffset)
                .last("limit 1"));
        if (s != null) {
            return s.getPhysicsOffset();
        }

        return 0L;
    }

    public List<SimpleMsg> getBatch(String t, long min, long max) {
        return baseMapper.selectList(new LambdaQueryWrapper<SimpleMsg>().
                eq(SimpleMsg::getTopicName, t)
                .between(SimpleMsg::getPhysicsOffset, min, max));
    }

    public SimpleMsg get(String t, Long newPhysicsOffset) {
        return baseMapper.selectOne(new LambdaQueryWrapper<SimpleMsg>()
                .eq(SimpleMsg::getTopicName, t)
                .eq(SimpleMsg::getPhysicsOffset, newPhysicsOffset));
    }

    private SimpleMsg getSimpleMsg(Msg msg, long physicsOffset) {
        SimpleMsg simpleMsg = new SimpleMsg();
        String ip = IPv4AddressUtil.get();
        String pid = PIDUtil.get();
        String name = Thread.currentThread().getName();
        simpleMsg.setFromIp(ip + "@" + pid + "@" + name);
        simpleMsg.setProperties(jsonUtil.write(msg.getProperties()));
        simpleMsg.setJsonBody(msg.getBody());
        simpleMsg.setTags(msg.getTags());
        simpleMsg.setTopicName(msg.getTopic());
        simpleMsg.setPhysicsOffset(physicsOffset);
        simpleMsg.setCreateTime(new Date());
        simpleMsg.setUpdateTime(new Date());
        return simpleMsg;
    }
}
