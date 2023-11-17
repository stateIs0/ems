package com.think.ems.dispatcher;

import org.apache.commons.exec.*;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class Main {

    static String url = "jdbc:mysql://localhost:3306/ems";
    static String username = "root";
    static String password = "Cc123456_";


    static String idePath = System.getProperty("user.dir") + "/example-parent";

    static String[] consumers = new String[]{
            idePath + "/springboot-consumer-1/target/springboot-consumer-1.jar",
            idePath + "/springboot-consumer-2/target/springboot-consumer-2.jar",
            idePath + "/springboot-consumer-3/target/springboot-consumer-3.jar"
    };

    static String[] producers = new String[]{
            idePath + "/springboot-producer-1/target/springboot-producer-1.jar",
            idePath + "/springboot-producer-2/target/springboot-producer-2.jar",
    };


    // 一个 topic, 2 个 group 测试
    public static void main(String[] args) throws Throwable {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println(DateUtils.current() + " end ----- kill -------");
            kill();
        }));
        System.out.println(DateUtils.current() + " ---> starting .....");
        cleanData();
        startProducer();
        startConsumer();
        new Thread(() -> {
            while (true) {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
                checkResult();
            }
        }).start();
        auto_kill_recover();
        LockSupport.park();
    }

    public static void auto_kill_recover() {

        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(1);

        es.scheduleWithFixedDelay(() -> {
            killAndRe();
        }, 30, 30, TimeUnit.SECONDS);

    }

    private static void killAndRe() {
        String jarPath = consumers[new Random().nextInt(consumers.length)];


        try {
            System.out.println("auto_kill_recover --->>> " + jarPath);
            CommandLine cmdLine = new CommandLine("sh");
            cmdLine.addArgument("auto_kill_recover.sh ");
            cmdLine.addArgument(jarPath);

            DefaultExecutor executor = new DefaultExecutor();

            executor.setWatchdog(new ExecuteWatchdog(TimeUnit.SECONDS.toMillis(30)));

            PumpStreamHandler streamHandler = new PumpStreamHandler(System.out, System.err);
            executor.setStreamHandler(streamHandler);


            executor.execute(cmdLine, new ExecuteResultHandler() {
                @Override
                public void onProcessComplete(int i) {
                    System.out.println("--->>> killAndRe 执行命令 结束: " + i);
                }

                @Override
                public void onProcessFailed(ExecuteException e) {
                    System.err.println(jarPath + " ßkillAndRe 执行命令时出现异常: " + e.getMessage());
                }
            });

        } catch (IOException e) {
            System.err.println(jarPath + " -->>>> 执行命令时出现异常: " + e.getMessage());
        }
    }

    // 启动 2 个 producer
    static void startProducer() throws IOException {
        for (String producer : producers) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            System.out.println("start " + producer);
            start(producer);
        }

    }

    // 启动 3 个 consumer (2个 group)
    static void startConsumer() {
        for (String consumer : consumers) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            System.out.println("start " + consumer);
            start(consumer);
        }
    }

    static void cleanData() {
        try {
            // 1. 注册JDBC驱动程序（通常只需在应用程序启动时执行一次）
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 2. 建立数据库连接
            Connection connection = DriverManager.getConnection(url, username, password);

            // 3. 创建Statement对象
            Statement statement = connection.createStatement();

            // 4. 执行多个SQL语句
            String sqlQuery;
            sqlQuery = "truncate table ems_simple_msg;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table running_log;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table ems_topic_group_log;";
            statement.execute(sqlQuery);
            sqlQuery = "truncate table ems_retry_msg;";
            statement.execute(sqlQuery);

            sqlQuery = "truncate table ems_dead_msg;";
            statement.execute(sqlQuery);

            sqlQuery = "truncate table ems_group_client_table;";
            statement.execute(sqlQuery);

            statement.close();
            connection.close();

            Jedis jedis = new Jedis("localhost");
            jedis.flushDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void checkResult() {

        try {
            // 1. 注册JDBC驱动程序（通常只需在应用程序启动时执行一次）
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 2. 建立数据库连接
            Connection connection = DriverManager.getConnection(url, username, password);

            // 3. 创建Statement对象
            Statement statement = connection.createStatement();

            // 4. 执行多个SQL语句
            String sqlQuery;
            sqlQuery = "select count(1) from running_log;";
            ResultSet resultSet = statement.executeQuery(sqlQuery);
            while (resultSet.next()) {
                int anInt = resultSet.getInt("count(1)");
                if (anInt >= Integer.parseInt(System.getProperty("running.result", "200000"))) {
                    System.out.println(DateUtils.current() + "------>>>>> result " + anInt);
                    //System.exit(1);
                }
            }

            statement.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void start(String jarPath) {
        try {
            // 构建命令行
            CommandLine cmdLine = new CommandLine("sh");
            cmdLine.addArgument("start.sh ");
            cmdLine.addArgument(jarPath);

            DefaultExecutor executor = new DefaultExecutor();

            executor.setWatchdog(new ExecuteWatchdog(TimeUnit.SECONDS.toMillis(30)));

            PumpStreamHandler streamHandler = new PumpStreamHandler(System.out, System.err);
            executor.setStreamHandler(streamHandler);


            executor.execute(cmdLine, new ExecuteResultHandler() {
                @Override
                public void onProcessComplete(int i) {
                    System.out.println("执行命令 结束: " + i);
                }

                @Override
                public void onProcessFailed(ExecuteException e) {
                    System.err.println(jarPath + " 执行命令时出现异常: " + e.getMessage());
                }
            });
        } catch (IOException e) {
            System.err.println(jarPath + " -->>>> 执行命令时出现异常: " + e.getMessage());
        }
    }

    static void kill() {
        try {
            // 执行 jps 命令并读取其输出
            CommandLine jpsCommand = new CommandLine("sh");
            jpsCommand.addArgument("stop.sh");
            DefaultExecutor executor = new DefaultExecutor();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
            executor.setStreamHandler(streamHandler);

            executor.execute(jpsCommand);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}