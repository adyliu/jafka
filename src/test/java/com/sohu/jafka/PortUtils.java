package com.sohu.jafka;

import com.sohu.jafka.utils.Closer;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * @author: adyliu(imxylz@gmail.com)
 * @since: 2013-04-25
 */
public class PortUtils {

    public static int checkAvailablePort(int port){
        while (port++ < 65500){
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(port);
                return port;
            } catch (IOException e) {
                //ignore error
            }finally {
                Closer.closeQuietly(serverSocket);
            }
        }
        throw new RuntimeException("no available port");
    }

    public static void main(String[] args){
        int port = checkAvailablePort(80);
        System.out.println("The available port is "+port);
    }
}
