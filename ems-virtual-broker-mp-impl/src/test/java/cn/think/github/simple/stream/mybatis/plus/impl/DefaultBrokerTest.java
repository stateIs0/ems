package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Properties;

public class DefaultBrokerTest {

    @Test
    public void test1() {
        ObjectMapper objectMapper = new ObjectMapper();

        JsonUtil jsonUtil = new JsonUtil() {
            @Override
            public String write(Object o) {
                try {
                    return objectMapper.writeValueAsString(o);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public <T> T read(String json, Class<T> c) {
                try {
                    return objectMapper.readValue(json, c);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Properties load = jsonUtil.read(
                "{\"esa.mq.context.key\":\"{\\\"KEY_TENANT_ID\\\":\\\"1000-oper\\\",\\\"KEY_CURRENT_USER\\\":\\\"{\\\\\\\"userId\\\\\\\":\\\\\\\"yunjie\\\\\\\",\\\\\\\"userName\\\\\\\":\\\\\\\"云杰\\\\\\\",\\\\\\\"extra\\\\\\\":{\\\\\\\"workNum\\\\\\\":\\\\\\\"1160\\\\\\\",\\\\\\\"mail\\\\\\\":\\\\\\\"yunjie@tsign.cn\\\\\\\",\\\\\\\"name\\\\\\\":\\\\\\\"张鹤生\\\\\\\",\\\\\\\"mobile\\\\\\\":\\\\\\\"18812665881\\\\\\\",\\\\\\\"alias\\\\\\\":\\\\\\\"云杰\\\\\\\",\\\\\\\"$jacocoData\\\\\\\":\\\\\\\"[Z@13833d48\\\\\\\",\\\\\\\"groupList\\\\\\\":\\\\\\\"[DepartmentGroup(groupId=899246032, groupName=公有云研发部, parentName=研发中心, parentId=898707956, managerList=null), DepartmentGroup(groupId=899021416, groupName=公有云PBU, parentName=PBG(同步正式环境不可修改), parentId=898734515, managerList=null)]\\\\\\\",\\\\\\\"job\\\\\\\":\\\\\\\"高级JAVA开发工程师\\\\\\\",\\\\\\\"account\\\\\\\":\\\\\\\"yunjie\\\\\\\"}}\\\"}\",\"esa.dispatch.context.traceId\":\"16cbd4de-b04b-4ed5-ab54-8a48cdb95ec0\"}"
                , Properties.class);

        System.out.println(load);
    }


}