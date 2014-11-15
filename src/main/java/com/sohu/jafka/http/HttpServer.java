/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.sohu.jafka.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 */
public class HttpServer extends Thread implements Closeable{

    private final int port;
    final EventLoopGroup bossGroup = new NioEventLoopGroup();
    final EventLoopGroup workerGroup = new NioEventLoopGroup(10);
    final HttpRequestHandler handler;
    final org.slf4j.Logger logger = LoggerFactory.getLogger(getClass());
    //
    public HttpServer(int port,HttpRequestHandler handler) {
        super("jafka-httpserver");
        this.port = port;
        this.handler = handler;
    }

    public void run() {
        // Configure the server.
        //

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpServerInitializer(this));

            Channel ch = b.bind(port).sync().channel();
            //System.err.println("Open your web browser and navigate to " + "http://127.0.0.1:" + port + '/');
           logger.info("Jafka HttpServer start at port {}",port);
            ch.closeFuture().sync();
        } catch (InterruptedException ie){
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie.getMessage(),ie);
        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        logger.warn("Jafka HttpServer run over");
    }

    @Override
    public void close() throws IOException {
        logger.info("Jafka HttpServer stop port {}",port);
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 9093;
        }
        System.out.println("start server");
        new HttpServer(port,null).run();
        System.out.println("server stop");
    }
}
