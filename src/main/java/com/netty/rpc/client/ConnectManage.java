package com.netty.rpc.client;

import com.netty.rpc.protocol.RpcDecoder;
import com.netty.rpc.protocol.RpcEncoder;
import com.netty.rpc.protocol.RpcRequest;
import com.netty.rpc.protocol.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 采用单例模式，用于管理连接
 * Created by luxiaoxun on 2016-03-16.
 */
public class ConnectManage {
    private static final Logger logger = LoggerFactory.getLogger(ConnectManage.class);
    private volatile static ConnectManage connectManage;

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(4);
    //ArrayBlockingQueue是数组实现的线程安全的有界的阻塞队列
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(16, 16,
            600L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(65536));

    // 实现负载均衡需要使用的容器
    private CopyOnWriteArrayList<RpcClientHandler> connectedHandlers = new CopyOnWriteArrayList<>();
    //不能使用set的原因是在移除无效结点时需要关闭该channel，所以需要通过Handler获取
    //作用：记录注册机所有结点
    private Map<InetSocketAddress, RpcClientHandler> connectedServerNodes = new ConcurrentHashMap<>();
    private Map<String, InetSocketAddress> connectedSockets = new ConcurrentHashMap<>();

    private ReentrantLock lock = new ReentrantLock();
    private Condition connected = lock.newCondition();
    private long connectTimeoutMillis = 6000;
    private AtomicInteger roundRobin = new AtomicInteger(0);
    private volatile boolean isRuning = true;

    private ConnectManage() {
    }

    public static ConnectManage getInstance() {
        if (connectManage == null) {
            synchronized (ConnectManage.class) {
                if (connectManage == null) {
                    connectManage = new ConnectManage();
                }
            }
        }
        return connectManage;
    }

    public void updateConnectedServer(List<String> allServerAddress) {
        if (allServerAddress != null) {
            if (allServerAddress.size() > 0) {
                //将服务器结点加到set容器
                HashSet<InetSocketAddress> newAllServerNodeSet = new HashSet<InetSocketAddress>();
                Map<String, InetSocketAddress> newConnectedSockets = new ConcurrentHashMap<>();
                for (int i = 0; i < allServerAddress.size(); ++i) {
                    String[] array = allServerAddress.get(i).split(" ");
                    String[] socket = array[0].split(":");
                    if (socket.length == 2) { // Should check IP and port
                        String host = socket[0];
                        int port = Integer.parseInt(socket[1]);
                        final InetSocketAddress remotePeer = new InetSocketAddress(host, port);
                        newAllServerNodeSet.add(remotePeer);
                        for (int j = 1; j < array.length; j++)
                            newConnectedSockets.put(array[j], remotePeer);
                    }
                }
                connectedSockets = newConnectedSockets;

                // 增加新的服务器结点
                for (final InetSocketAddress serverNodeAddress : newAllServerNodeSet) {
                    if (!connectedServerNodes.keySet().contains(serverNodeAddress)) {
                        connectServerNode(serverNodeAddress);
                        logger.info("发现新服务器结点" + serverNodeAddress);
                    }
                }

                // 移除无效的服务器结点
                for (int i = 0; i < connectedHandlers.size(); ++i) {
                    RpcClientHandler connectedServerHandler = connectedHandlers.get(i);
                    SocketAddress remotePeer = connectedServerHandler.getRemotePeer();
                    if (!newAllServerNodeSet.contains(remotePeer)) {
                        logger.info("Remove invalid server node " + remotePeer);
                        RpcClientHandler handler = connectedServerNodes.get(remotePeer);
                        if (handler != null) {
                            handler.close();
                        }
                        connectedServerNodes.remove(remotePeer);
                        connectedHandlers.remove(connectedServerHandler);
                        logger.info("移除服务结点" + remotePeer);
                    }
                }

            } else { // No available server node ( All server nodes are down )
                logger.error("No available server node. All server nodes are down !!!");
                for (final RpcClientHandler connectedServerHandler : connectedHandlers) {
                    SocketAddress remotePeer = connectedServerHandler.getRemotePeer();
                    RpcClientHandler handler = connectedServerNodes.get(remotePeer);
                    handler.close();
                    connectedServerNodes.remove(connectedServerHandler);
                }
                connectedHandlers.clear();
            }
        }
    }

    public void reconnect(final RpcClientHandler handler, final SocketAddress remotePeer) {
        if (handler != null) {
            connectedHandlers.remove(handler);
            connectedServerNodes.remove(handler.getRemotePeer());
        }
        connectServerNode((InetSocketAddress) remotePeer);
    }

    private void connectServerNode(final InetSocketAddress remotePeer) {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Bootstrap b = new Bootstrap();
                b.group(eventLoopGroup)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer< SocketChannel >(){
                            @Override
                            protected void initChannel(SocketChannel socketChannel) throws Exception {
                                ChannelPipeline cp = socketChannel.pipeline();
                                cp.addLast(new RpcEncoder(RpcRequest.class));
                                cp.addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0));
                                cp.addLast(new RpcDecoder(RpcResponse.class));
                                cp.addLast(new RpcClientHandler());
                            }
                        });

                ChannelFuture channelFuture = b.connect(remotePeer);
                channelFuture.addListener(new ChannelFutureListener() {
                    /**
                     * 连接成功后执行此方法
                     * @param channelFuture
                     * @throws Exception
                     */
                    @Override
                    public void operationComplete(final ChannelFuture channelFuture) throws Exception {
                        if (channelFuture.isSuccess()) {
                            logger.info("Successfully connect to remote server. remote peer = " + remotePeer);
                            RpcClientHandler handler = channelFuture.channel().pipeline().get(RpcClientHandler.class);
                            addHandler(handler);
                        }
                    }
                });
            }
        });
    }

    private void addHandler(RpcClientHandler handler) {
        InetSocketAddress remoteAddress = (InetSocketAddress) handler.getChannel().remoteAddress();
        connectedServerNodes.put(remoteAddress, handler);
        signalAvailableHandler();
    }

    private void signalAvailableHandler() {
        lock.lock();
        try {
            connected.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待直到handler成功添加
     * @return
     * @throws InterruptedException
     */
    private boolean waitingForHandler() throws InterruptedException {
        lock.lock();
        try {
            //await方法：当前线程进入等待状态，如果其他线程调用condition的signal或者signalAll
            // 方法并且当前线程获取Lock从await方法返回，如果在等待状态中被中断会抛出被中断异常
            return connected.await(this.connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }
    // 实现负载均衡
    public RpcClientHandler chooseHandler(String name){
        try {
            waitingForHandler();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return connectedServerNodes.get(connectedSockets.get(name));
//        int size = connectedHandlers.size();
//        while (isRuning && size <= 0) {
//            try {
//                boolean available = waitingForHandler();
//                if (available) {
//                    size = connectedHandlers.size();
//                }
//            } catch (InterruptedException e) {
//                logger.error("Waiting for available node is interrupted! ", e);
//                throw new RuntimeException("Can't connect any servers!", e);
//            }
//        }
//        //Round Robin 轮询调度算法实现负载均衡
//        int index = (roundRobin.getAndAdd(1) + size) % size;
//        //由于都是相同的Handler，所以可以随机返回一个使用
//        return connectedHandlers.get(index);
    }

    public void stop() {
        isRuning = false;
        for (int i = 0; i < connectedHandlers.size(); ++i) {
            RpcClientHandler connectedServerHandler = connectedHandlers.get(i);
            connectedServerHandler.close();
        }
        signalAvailableHandler();
        threadPoolExecutor.shutdown();
        eventLoopGroup.shutdownGracefully();
    }
}
