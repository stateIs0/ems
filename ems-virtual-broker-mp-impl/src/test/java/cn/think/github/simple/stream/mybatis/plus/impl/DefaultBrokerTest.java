package cn.think.github.simple.stream.mybatis.plus.impl;

import cn.think.github.simple.stream.api.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

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
    }

    @Test
    public void treeMap() {
        Map<String, List<String>> map = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return 0;
            }
        });
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");
        for (String s : list) {
            List<String> list1 = map.computeIfAbsent(s, a -> new ArrayList<>());
            list1.add(s);
        }

        Assert.assertTrue(map.size() == 1);
    }


}