package com.qqzeng.zkprimitives.lock;

/**
 * Created by qqzeng.
 */

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.sql.Array;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Advanced Exclusive lock, which is corresponding to Simple Lock WITHOUT Herd Effect in paper.
 * <p>
 * Compared to Exclusive lock, the main improvement is that client will only register watch event of
 * the node just preceding it, not the lowest one. Hence, it will not cause herd effect
 * when the current lock owner releases the lock.
 * <p>
 * In this situation, each client obtains the lock in order of request arrival.
 *
 * About Test:
 * Run the client several times, such as 3 or 5. Every client instance will execute the following process:
 *      1. Create /exclusive-lock-advanced in zookeeper server if it does not exist.
 *      2. Sleep for an interval between [SLEEP_INTERVAL_BASE, SLEEP_INTERVAL_BASE + SLEEP_INTERVAL_RANGE].
 *      3. Create /exclusive-lock-advanced/lock_xxx.
 *      4. Try to acquire the lock.
 *      5. If success, then sleep for an interval between [SLEEP_INTERVAL_BASE/2, SLEEP_INTERVAL_BASE/2 + SLEEP_INTERVAL_RANGE/2].
 *      6. one round test over, continue until reaching to NUM_ROUND times.
 */
public class ExclusiveLockAdv {
    private static final Logger LOGGER = Logger.getLogger(ExclusiveLockAdv.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private ZooKeeper zk = null;
    private static final int SESSION_TIMEOUT = 10000;
    private static final String ROOT = "/exclusive-lock-advanced";
    private final Object mutex;
    private String myZnode;

    private static final int NUM_ROUND = 3;
    private static final int SLEEP_INTERVAL_BASE = 2000;
    private static final int SLEEP_INTERVAL_RANGE = 3000;

    public ExclusiveLockAdv(int nodeNum, String hostport) {
        mutex = new Object();
        this.nodeNum = nodeNum;
        this.logPrefix = "node-" + nodeNum;
        if (zk == null) {
            try {
                zk = new ZooKeeper(hostport, SESSION_TIMEOUT, (event) -> {
                    Watcher.Event.KeeperState state = event.getState();
                    Watcher.Event.EventType type = event.getType();
                    if (Watcher.Event.KeeperState.SyncConnected == state) {
                        if (type == Watcher.Event.EventType.None) {
                            countDownLatch.countDown();
                            LOGGER.info(logPrefix + " zooKeeper connection created !");
                        } else if (type == Watcher.Event.EventType.NodeDeleted) {
                            LOGGER.info(logPrefix + " lock node: " + event.getPath() +" deleted !");
                            synchronized (mutex) {
                                mutex.notify();
                            }
                        }
                    }
                });
            } catch (IOException e) {
                zk = null;
            }
        }
        if (zk != null) {
            try {
                Stat s = zk.exists(ROOT, true);
                if (s == null) {
                    String rootNode = zk.create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.info(this.logPrefix + rootNode + " create successfully !");
                }
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }
    }

    private void tryAcquireLock() throws InterruptedException, KeeperException {
        Thread.sleep(SLEEP_INTERVAL_BASE + new Random().nextInt(SLEEP_INTERVAL_RANGE));
        this.myZnode = zk.create(ROOT + "/lock_", ("node-" + nodeNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        getLock();
    }

    private void getLock() throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(ROOT, false);
        list.sort((o1, o2) -> o1.compareTo(o2));
        if (this.myZnode.equals(ROOT + "/" + list.get(0))) {
            LOGGER.info(logPrefix + "[" + myZnode + "]" + " acquires the lock successfully.");
            doWork();
            releaseLock();
        } else {
            String myID = this.myZnode.substring(this.myZnode.lastIndexOf("/")+1);
            String prevNode = list.get(list.indexOf(myID) - 1);
            waitForLock(prevNode);
        }
    }

    private void releaseLock() throws KeeperException, InterruptedException {
        LOGGER.info(this.logPrefix + " begin to release the lock.");
        zk.delete(myZnode, -1);
        LOGGER.info(this.logPrefix + " release the lock successfully.");
    }

    private void waitForLock(String node) throws InterruptedException, KeeperException {
        Stat stat = zk.exists(ROOT + "/" + node, true);
        if (stat != null) {
            synchronized (mutex) {
                LOGGER.info(this.logPrefix + " waits for the lock.");
                mutex.wait();
                getLock();
            }
        } else {
            getLock();
        }
    }

    private void doWork() throws InterruptedException {
        LOGGER.info(logPrefix + " begin to do work.");
        Thread.sleep(SLEEP_INTERVAL_BASE / 2 + new Random().nextInt(SLEEP_INTERVAL_RANGE / 2));
        LOGGER.info(logPrefix + " over do work.");
    }

    public static void main(String[] args) {
        final int nodeNum = new Random().nextInt(100);
        for (int i = 0; i < NUM_ROUND; i++) {
            LOGGER.info("Node " + nodeNum + " try to acquire the distributed exclusive lock.");
            ExclusiveLockAdv el = new ExclusiveLockAdv(nodeNum, CONNECT_STRING);
            try {
                el.tryAcquireLock();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info("node-" + nodeNum + " complete round " + i + ".");
        }
        LOGGER.info("node-" + nodeNum + " exit!");
    }
}
