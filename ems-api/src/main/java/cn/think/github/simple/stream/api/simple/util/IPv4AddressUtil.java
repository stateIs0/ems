package cn.think.github.simple.stream.api.simple.util;

import lombok.extern.slf4j.Slf4j;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

@Slf4j
public class IPv4AddressUtil {

    private static String ip;

    public static String get() {
        if (ip != null) {
            return ip;
        }
        synchronized (IPv4AddressUtil.class) {
            if (ip != null) {
                return ip;
            }

            log.info("start get ip");
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

                while (networkInterfaces.hasMoreElements()) {

                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    if (networkInterface.isLoopback()) {
                        continue;
                    }

                    Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();

                    while (inetAddresses.hasMoreElements()) {
                        InetAddress inetAddress = inetAddresses.nextElement();

                        if (inetAddress instanceof Inet4Address) {
                            ip = inetAddress.getHostAddress();
                            break;
                        }
                    }
                }
            } catch (SocketException ignore) {

            }

            log.info("end get ip " + ip);

            return ip;
        }


    }
}
