package cn.think.github.simple.stream.client.support;

import cn.think.github.simple.stream.api.simple.util.IPv4AddressUtil;
import cn.think.github.simple.stream.api.simple.util.PIDUtil;
import org.junit.Test;

public class IPv4AddressUtilTest {

    @Test
    public void f() {
        String s = IPv4AddressUtil.get();
        String s1 = PIDUtil.get();
        System.out.println(s);
        System.out.println(s1);
    }

}