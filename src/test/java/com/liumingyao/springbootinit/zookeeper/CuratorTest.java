package com.liumingyao.springbootinit.zookeeper;

import cn.hutool.core.collection.CollectionUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CuratorTest {

    /**
     * Curator连接实例
     */
    private static CuratorFramework client = null;

    private static final String path = "/curatorTest";

    private static String text = "Hello World";

    /**
     * 创建连接
     */
    @BeforeAll
    public static void init() {
        // 重试策略
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);
        client = CuratorFrameworkFactory.builder()
                                        .connectString("192.168.122.128:2181")
                                        .sessionTimeoutMs(10000)
                                        .retryPolicy(retryPolicy)
                                        .namespace("workspace")
                                        .build();
        //指定命名空间后，client 的所有路径操作都会以 /workspace 开头
        client.start();
    }

    /**
     * 关闭连接
     */
    @AfterAll
    public static void destroy() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void isZNodeExisted() throws Exception {
        Stat stat = client.checkExists().forPath(path);
        Assertions.assertNull(stat);
    }

    /**
     * 判断服务状态
     */
    @Test
    public void getState() {
        CuratorFrameworkState state = client.getState();
        Assertions.assertEquals(CuratorFrameworkState.STARTED, state);
    }

    /**
     * 创建节点
     */
    @Test
    public void createZNode() throws Exception {
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path,text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 删除节点
     */
    @Test
    public void deleteZNode() throws Exception {
        client.delete()
                .guaranteed()                     // 如果删除失败，会继续执行，直到成功
                .deletingChildrenIfNeeded()       // 如果有子节点，则递归删除
                .withVersion(client.checkExists().forPath(path).getVersion())   // 传入版本号，如果版本号错误则拒绝删除操作，并抛出 BadVersion 异常
                .forPath(path);
    }

    @Test
    public void getZNodeData()throws Exception{
        byte[] data = client.getData().forPath(path);
        Assertions.assertEquals(text, new String(data));
        System.out.println("修改前的节点数据：" + new String(data));
    }

    @Test
    public void setZNodeData()throws Exception{
        String text2 = "try again";
        client.setData()
                .withVersion(client.checkExists().forPath(path).getVersion())
                .forPath(path, text2.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void getChildZNode() throws Exception{
        List<String> children = client.getChildren().forPath(path);
        children.forEach(System.out::println);
        List<String> expectedList = CollectionUtil.newArrayList("1", "2");
        Assertions.assertTrue(CollectionUtil.containsAll(expectedList, children));

    }

    /**
     * 创建一次性监听
     * @throws Exception
     */
    @Test
    public void createOneTimeWatch() throws Exception {
        // 设置监听器
        client.getData().usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                System.out.println("节点 " + event.getPath() + " 发生了事件：" + event.getType());
            }
        }).forPath(path);

        // 第一次修改
        client.setData()
                .withVersion(client.checkExists().forPath(path).getVersion())
                .forPath(path, "第一次修改".getBytes(StandardCharsets.UTF_8));

        // 第二次修改
        client.setData()
                .withVersion(client.checkExists().forPath(path).getVersion())
                .forPath(path, "第二次修改".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 创建永久监听
     * @throws Exception
     */
    @Test
    public void createPermanentWatch() throws Exception {
        // 设置监听器
        CuratorCache curatorCache = CuratorCache.builder(client, path).build();
        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework framework, PathChildrenCacheEvent event) throws Exception {
                System.out.println("节点 " + event.getData().getPath() + " 发生了事件：" + event.getType());
            }
        };
        CuratorCacheListener listener = CuratorCacheListener.builder()
                .forPathChildrenCache(path, client,
                        pathChildrenCacheListener)
                .build();
        curatorCache.listenable().addListener(listener);
        curatorCache.start();

        // 第一次修改
        client.setData()
                .withVersion(client.checkExists().forPath(path).getVersion())
                .forPath(path, "第一次修改".getBytes(StandardCharsets.UTF_8));

        // 第二次修改
        client.setData()
                .withVersion(client.checkExists().forPath(path).getVersion())
                .forPath(path, "第二次修改".getBytes(StandardCharsets.UTF_8));
    }


    @Test
    public void watchChileZNode() throws Exception{
        // 创建节点
        String text = "Hello World";
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)      //节点类型
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, text.getBytes(StandardCharsets.UTF_8));
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)      //节点类型
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path + "/1", text.getBytes(StandardCharsets.UTF_8));
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)      //节点类型
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path + "/2", text.getBytes(StandardCharsets.UTF_8));

        // 设置监听器
        // 第三个参数代表除了节点状态外，是否还缓存节点内容
        PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
        /*
         * StartMode 代表初始化方式:
         *    NORMAL: 异步初始化
         *    BUILD_INITIAL_CACHE: 同步初始化
         *    POST_INITIALIZED_EVENT: 异步并通知,初始化之后会触发 INITIALIZED 事件
         */
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点列表：");
        childDataList.forEach(x -> System.out.println(x.getPath()));

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                switch (event.getType()) {
                    case INITIALIZED:
                        System.out.println("childrenCache 初始化完成");
                        break;
                    case CHILD_ADDED:
                        // 需要注意的是: 即使是之前已经存在的子节点，也会触发该监听，因为会把该子节点加入 childrenCache 缓存中
                        System.out.println("增加子节点:" + event.getData().getPath());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("删除子节点:" + event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("被修改的子节点的路径:" + event.getData().getPath());
                        System.out.println("修改后的数据:" + new String(event.getData().getData()));
                        break;
                }
            }
        });

        // 第一次修改
        client.setData()
                .forPath(path + "/1", "第一次修改".getBytes(StandardCharsets.UTF_8));

        // 第二次修改
        client.setData()
                .forPath(path + "/1", "第二次修改".getBytes(StandardCharsets.UTF_8));
    }




}
