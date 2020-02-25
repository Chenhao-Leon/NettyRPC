package com.netty.rpc.server;

import com.netty.rpc.protocol.RpcDecoder;
import com.netty.rpc.protocol.RpcRequest;
import com.netty.rpc.protocol.RpcResponse;
import com.netty.rpc.protocol.RpcEncoder;
import com.netty.rpc.registry.ServiceRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * RPC Server
 *
 * @author huangyong, luxiaoxun
 * @description 实现ApplicationContextAware是为了得到被注解RpcService标记的对象，
 * 实现InitializingBean是为了在获得对象后启动服务器
 */
public class RpcServer implements ApplicationContextAware, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private String serverAddress;
    private ServiceRegistry serviceRegistry;

    //接口的全限定类名->对象
    private Map<String, Object> handlerMap = new HashMap<>();
    private static ThreadPoolExecutor threadPoolExecutor;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;

    public RpcServer(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public RpcServer(String serverAddress, ServiceRegistry serviceRegistry) {
        this.serverAddress = serverAddress;
        this.serviceRegistry = serviceRegistry;
    }

    /**
     * 在Bean对象全部属性被注入后、afterPropertiesSet方法执行之前执行
     * @param ctx
     * @throws BeansException
     */
    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        Map<String, Object> serviceBeanMap = ctx.getBeansWithAnnotation(RpcService.class);
        if (MapUtils.isNotEmpty(serviceBeanMap)) {
            // TODO 遍历HashMap的key和value的原理
            for (Object serviceBean : serviceBeanMap.values()) {
                //获取带有RpcService注解类的全限定类名
                String interfaceName = serviceBean.getClass().getAnnotation(RpcService.class).value().getName();
                logger.info("Loading service: {}", interfaceName);
                handlerMap.put(interfaceName, serviceBean);
            }
        }
    }

    /**
     * 在BeanFactory将所有bean属性注入且setApplicationContext方法执行完，BeanFactory调用此方法
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    public void stop() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    public static void submit(Runnable task) {
        if (threadPoolExecutor == null) {
            synchronized (RpcServer.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(16, 16, 600L,
                            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(65536));
                }
            }
        }
        threadPoolExecutor.submit(task);
    }

    public RpcServer addService(String interfaceName, Object serviceBean) {
        if (!handlerMap.containsKey(interfaceName)) {
            logger.info("Loading service: {}", interfaceName);
            handlerMap.put(interfaceName, serviceBean);
        }

        return this;
    }

    public void start() throws Exception {
        if (bossGroup == null && workerGroup == null) {
            //用于接收连接的多线程事件循环组
            bossGroup = new NioEventLoopGroup();
            //用于处理请求的多线程事件循环组
            workerGroup = new NioEventLoopGroup();
            //ServerBootstrap是一个帮助我们启动服务器的帮助类，没有他启动服务器将会很繁琐
            ServerBootstrap bootstrap = new ServerBootstrap();
            //用于实例化一个Channel处理连接
            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    //设置响应请求的handler
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel channel) throws Exception {
                            channel.pipeline()
                                    //自动获取消息长度字段的解码器
                                    .addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0))
                                    .addLast(new RpcDecoder(RpcRequest.class))
                                    .addLast(new RpcEncoder(RpcResponse.class))
                                    .addLast(new RpcHandler(handlerMap));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            String[] array = serverAddress.split(":");
            String host = array[0];
            int port = Integer.parseInt(array[1]);

            /*ChannelFuture 异步IO操作的结果。在Netty中所有的IO操作都是异步的，这意味着所有IO调用
            会立刻返回一个ChannelFuture实例，但这并不是最终结果，当调用结束会返回最终结果。通过这个
            实例我们可以获取一些IO操作的状态信息。
            ChannelFuture有完成和未完成两种状态。开始ChannelFuture会被初始化为未完成状态，当操作结
            束无论操作成功与否状态变为完成状态。完成状态中有三种返回结果，分别是成功、取消、返回
            */
            ChannelFuture future = bootstrap.bind(host, port).sync();
            logger.info("Server started on port {}", port);

            if (serviceRegistry != null) {
                serviceRegistry.register(serverAddress, new ArrayList<>(handlerMap.keySet()));
            }

            //等待直到服务器套接字（server socket）关闭
            future.channel().closeFuture().sync();
        }
    }

}
