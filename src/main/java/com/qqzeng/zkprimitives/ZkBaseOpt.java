package com.qqzeng.zkprimitives;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.sql.Time;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


public class ZkBaseOpt {
    private final static Logger LOGGER = Logger.getLogger(ZkBaseOpt.class);

    private static String CONNECT_STRING = "127.0.0.1:2181";
    private static int SESSION_TIMEOUT = 2000;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static String PARENT_PATH = "/testRoot";
    private static String CHILD_PATH = "/testRoot/children";

    static final AtomicInteger seq = new AtomicInteger();

    private static ZooKeeper zk = null;

    private static void readData(EventType type) throws KeeperException, InterruptedException {
        LOGGER.info("======================= " + PARENT_PATH + "info =======================");
        if (zk.exists(PARENT_PATH, true) == null) {
            zk.create(PARENT_PATH, "testroot-data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return;
        }
        if (type != EventType.NodeDeleted ) {
            LOGGER.info(zk.getData(PARENT_PATH, true, null));
            LOGGER.info(zk.getChildren(PARENT_PATH, true));
        }
        LOGGER.info("======================= " + PARENT_PATH + "info =======================");
    }


    private static void monitorZK() throws IOException, InterruptedException {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, (event) -> {
            String logPrefix = "[Watcher-" + seq.incrementAndGet() + "]";
            KeeperState state = event.getState();
            EventType type = event.getType();
            if (KeeperState.SyncConnected == state) {
                if (type == EventType.None) {
                    countDownLatch.countDown();
                    LOGGER.info("ZooKeeper connection created !");
                    try {
                        readData(type);
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                } else if (EventType.NodeCreated == type) {
                    LOGGER.info(logPrefix + " node created");
                    try {
                        readData(type);
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                } else if (EventType.NodeDataChanged == type) {
                    LOGGER.info(logPrefix + "node data changed");
                    try {
                        readData(type);
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                } else if (EventType.NodeChildrenChanged == type) {
                    LOGGER.info(logPrefix + "node child list updated");
                    try {
                        readData(type);
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                } else if (EventType.NodeDeleted == type) {
                    LOGGER.info(logPrefix + "node deleted");
                    try {
                        readData(type);
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                }
            } else if (KeeperState.Disconnected == state) {
                LOGGER.info(logPrefix + "Zookeeper connection lost");
            } else if (KeeperState.AuthFailed == state) {
                LOGGER.info(logPrefix + "authorization failure");
            } else if (KeeperState.Expired == state) {
                LOGGER.info(logPrefix + "session expired");
            }
        });
        countDownLatch.await();
        // output zookeeper state info.
        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Live zookeeper connection...");
            }
        }, 0, 1000);
    }

    /**
     *  Some zookeeper node operations.
     */
    private static void zkOpt() throws KeeperException, InterruptedException, IOException {
        ZooKeeper zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, (event) -> {
            KeeperState state = event.getState();
            EventType type = event.getType();
            if (KeeperState.SyncConnected == state) {
                if (type == EventType.None) {
                    countDownLatch.countDown();
                    LOGGER.info("ZooKeeper connection created !");
                }
            }
        });
        Stat stat = zk.exists(PARENT_PATH, false);
        if (stat == null) {
            String path = zk.create(PARENT_PATH, "testroot-data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Create node: " + path);
        } else {
            LOGGER.info("NODE " + PARENT_PATH + " exists !");
        }

        Thread.sleep(1000);
        zk.setData(PARENT_PATH, "testroot-data-new".getBytes(), -1);

        Thread.sleep(1000);
        String childPath = zk.create(CHILD_PATH, "child-data".getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        LOGGER.info(childPath);

        Thread.sleep(1000);
        byte[] data = zk.getData(PARENT_PATH, false, null);
        LOGGER.info(PARENT_PATH + " data : " + new String(data));

        Thread.sleep(1000);
        List<String> children = zk.getChildren(PARENT_PATH, false);
        LOGGER.info(PARENT_PATH + " children nodes list : " + children);

        Thread.sleep(1000);
        int delIndex = new Random().nextInt(children.size());
        String delChildPath = PARENT_PATH + "/" + children.get(delIndex);
        zk.delete(delChildPath, -1);
        LOGGER.info(children.get(delIndex) + " exist: " + (zk.exists(delChildPath, false) != null));

        Thread.sleep(1000);
        String child2Path = zk.create(PARENT_PATH + "/b", "child-b-data".getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info(child2Path);

        Thread.sleep(1000);
        zk.delete(PARENT_PATH + "/b", -1);

        Thread.sleep(1000);
        zk.delete(PARENT_PATH, -1);

        zk.close();
    }

    /**
     * main entry point.
     */
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        // set up zookeeper monitor.
        monitorZK();

        // do some zookeeper operation
        new Thread(()->{
            try {
                zkOpt();
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }).start();

    }


}
