package cn.think.github.simple.stream.mybatis.plus.impl.util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@Slf4j
public class IOUtils {

    public static String toString(final InputStream inputStream) {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return "";
    }

}