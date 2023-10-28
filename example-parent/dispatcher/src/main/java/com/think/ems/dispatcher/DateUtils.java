package com.think.ems.dispatcher;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @version 1.0
 * @Author cxs
 * @Description
 * @date 2023/9/25
 **/
public class DateUtils {

    public static String current() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date());
    }
}
