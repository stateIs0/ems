package cn.think.github.simple.stream.mybatis.plus.impl.crud.services;

import cn.think.github.simple.stream.mybatis.plus.impl.repository.dao.GroupClientTable;
import cn.think.github.simple.stream.mybatis.plus.impl.repository.mapper.GroupClientTableMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/2
 **/
@Slf4j
@Service
public class GroupClientTableService extends ServiceImpl<GroupClientTableMapper, GroupClientTable> {

    @Resource
    GroupClientTableMapper groupClientTableMapper;

    public void insert(String clientId, String group) {
        GroupClientTable t = new GroupClientTable();
        t.setClientId(clientId);
        t.setRenewTime(new Date());
        t.setState(GroupClientTable.state_up);
        t.setGroupName(group);
        t.setUpdateTime(new Date());
        t.setCreateTime(new Date());
        groupClientTableMapper.insert(t);
    }

    public List<GroupClientTable> getClientList(String g) {
        return baseMapper.selectList(new LambdaQueryWrapper<GroupClientTable>()
                .eq(GroupClientTable::getGroupName, g));
    }

    public void renew(String clientId, String groupName) {
        GroupClientTable groupClientTable = baseMapper.selectOne(new QueryWrapper<GroupClientTable>().lambda()
                .eq(GroupClientTable::getClientId, clientId));
        if (groupClientTable != null) {
            groupClientTable.setRenewTime(new Date());
            baseMapper.updateById(groupClientTable);
        } else {
            insert(clientId, groupName);
        }
    }

    public void down(String clientId) {
        GroupClientTable groupClientTable = baseMapper.selectOne(new QueryWrapper<GroupClientTable>().lambda()
                .eq(GroupClientTable::getClientId, clientId));
        log.warn("client [{}] down before......", clientId);
        if (groupClientTable == null) {
            return;
        }
        groupClientTable.setState(GroupClientTable.state_down);
        groupClientTableMapper.updateById(groupClientTable);
        log.warn("client [{}] down after......", clientId);
    }

}
