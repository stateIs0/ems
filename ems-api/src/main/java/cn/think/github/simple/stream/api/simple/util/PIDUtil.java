package cn.think.github.simple.stream.api.simple.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class PIDUtil {

    private static String pid = null;

    public static String get() {
        if (pid != null) {
            return pid;
        }
        RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();

        String processName = r.getName();
        long pid = Long.parseLong(processName.split("@")[0]);

        PIDUtil.pid = pid + "";
        return PIDUtil.pid;
    }
}
