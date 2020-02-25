package com.netty.rpc.registry;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务注册
 *
 * @author huangyong
 * @author luxiaoxun
 */
public class ServiceRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private String registryAddress;

    public ServiceRegistry(String registryAddress) {
        this.registryAddress = registryAddress;
    }

    /**
     * @param data 服务提供服务器的IP地址
     */
    public void register(String data, List<String> interfaceName) {
        if (data != null) {
            ZooKeeper zk = connectServer();
            if (zk != null) {
                AddRootNode(zk); // Add root node if not exist
                createNode(zk, data, interfaceName);
            }
        }
    }

    private ZooKeeper connectServer() {
        ZooKeeper zk = null;
        try {
            //创建一个zookeeper客户端，通过此客户端实现增删服务器结点的操作
            //此构造方法是异步的，会立即返回一个结果，因此加一个监听器，当连接成功主线程才继续执行
            zk = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    //客户端连接成功latch减1
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });
            //阻塞直到客户端成功连接
            latch.await();
        } catch (IOException | InterruptedException e) {
            logger.error("", e);
        }
        return zk;
    }

    /**
     * 如果没有/registry结点则添加
     * @param zk
     */
    private void AddRootNode(ZooKeeper zk){
        try {
            Stat s = zk.exists(Constant.ZK_REGISTRY_PATH, false);
            if (s == null) {
                zk.create(Constant.ZK_REGISTRY_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.toString());
        }
    }

    /**
     * 增加一个结点，并将提供服务的IP存到该结点中
     * @param zk
     * @param data 提供服务的IP
     */
    private void createNode(ZooKeeper zk, String data, List<String> interfaceName) {
        try {
            StringBuilder dataBuilder = new StringBuilder(data);
            for(String tmp : interfaceName)
                dataBuilder.append(" ").append(tmp);
            byte[] bytes = dataBuilder.toString().getBytes();
            String path = zk.create(Constant.ZK_DATA_PATH, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.info("create zookeeper node ({} => {})", path, data);
        } catch (KeeperException | InterruptedException e) {
            logger.error("", e);
        }
    }
}