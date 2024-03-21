package com.liumingyao.springbootinit.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@DisplayName("ZooKeeper 官方客户端测试例")
public class ZookeeperTest {

    /**
     * zookeeper连接实例
     */
    private static ZooKeeper zk;

    private static String path = "/mytest";
    private static String text  = "Hello World";

    /**
     * 创建zookeeper连接
     */
    @BeforeAll
    public static void init() throws IOException, InterruptedException {
        final String HOST = "192.168.122.128:2181";
        CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper(HOST, 5000, watcher -> {
            if (watcher.getState() == Watcher.Event.KeeperState.SyncConnected){
                latch.countDown();
            }
        });
        latch.await();
    }

    /**
     * 关闭zookeeper连接
     */
    @AfterAll
    public static void destroy () throws InterruptedException{
        if (zk != null){
            zk.close();
        }
    }

    /**
     * 建立连接
     */
    @Test
    public void getState() {
        ZooKeeper.States state = zk.getState();
        Assertions.assertTrue(state.isAlive());
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void exist() throws InterruptedException, KeeperException {
        Stat stat = zk.exists("/", true);
        Assertions.assertNotNull(stat);
    }

    /**
     * 创建一个节点
     */
    @Test
    public void createZNode() throws InterruptedException, KeeperException {
        zk.create(path, text.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat stat = zk.exists(path, true);
        System.out.println(stat);
        Assertions.assertNotNull(stat);
    }

    /**
     * 删除节点
     */
    @Test
    public void deleteZNode() throws InterruptedException, KeeperException {
        zk.delete(path, zk.exists(path, true).getVersion());
        Stat stat = zk.exists(path, true);
        Assertions.assertNull(stat);
    }

    /**
     * 获取节点数据
     * getData(String path, Watcher watcher, Stat stat)
     */
    @Test
    public void getZNodeData() throws InterruptedException, KeeperException {
        byte[] data = zk.getData(path, false, null);
        String text1 = new String(data);
        Assertions.assertEquals(text, text1);
        System.out.println(text1);
    }

    /**
     * 设置节点数据
     * setData(String path, byte[] data, int version)
     */
    @Test
    public void setZNodeData() throws InterruptedException, KeeperException {
        String text = "含子节点的节点";
        zk.create(path, text.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(path + "/1", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(path + "/2", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<String> actualList = zk.getChildren(path, false);
        for (String child : actualList) {
            System.out.println(child);
        }
    }


}
