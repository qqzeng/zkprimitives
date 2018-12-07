package com.qqzeng.zkprimitives.queue;

/**
 * Created by qqzeng.
 */

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Barrier, which is corresponding to Double Barrier in paper.
 * In short, before the client does some work, it must enter the barrier,
 * and wait until all clients do.
 * <p>
 *
 *  About Test:
 *  Run the client several times, such as 3 or 5. Every client instance will execute the following process:
 *       1. Create /queue-double-barrier in zookeeper server if it does not exist.
 *       2. Sleep for an interval between [SLEEP_INTERVAL_BASE, SLEEP_INTERVAL_BASE + SLEEP_INTERVAL_RANGE].
 *       3. Create /queue-double-barrier/node-i_xxx.
 *       4. Try to synchronize enter the barrier before doing some work.
 *       5. Sleep for an interval between [TASK_INTERVAL_BASE, TASK_INTERVAL_BASE + TASK_INTERVAL_RANGE].
 *       6. one round test over, continue until reaching to NUM_ROUND times.
 *
 */
public class Barrier {
    private static final Logger LOGGER = Logger.getLogger(Barrier.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;
    private final int nodeCnt;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private ZooKeeper zk = null;
    private static final int SESSION_TIMEOUT = 10000;
    private static final String ROOT = "/queue-barrier";
    private final Object mutex;
    private String myZnode;

    private static final int NUM_ROUND = 1;
    private static final int SLEEP_INTERVAL_BASE = 2000;
    private static final int SLEEP_INTERVAL_RANGE = 3000;
    private static final int TASK_INTERVAL_BASE = 4000;
    private static final int TASK_INTERVAL_RANGE = 4000;

    public Barrier(int nodeNum, String hostport, int nodeCnt) {
        this.nodeCnt = nodeCnt;
        this.mutex = new Object();
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
                        } else if (type == Watcher.Event.EventType.NodeChildrenChanged) {
                            LOGGER.info(logPrefix + " lock node: " + event.getPath() + " children nodes changed !");
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
                    String rootNode = zk.create(ROOT, ByteBuffer.allocate(4).putInt(nodeCnt).array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.info(this.logPrefix + rootNode + " create successfully !");
                }
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }
    }

    private void createNode() throws InterruptedException, KeeperException {
        Thread.sleep(SLEEP_INTERVAL_BASE + new Random().nextInt(SLEEP_INTERVAL_RANGE));
        this.myZnode = zk.create(ROOT + "/" + logPrefix + "_", ("node-" + nodeNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private boolean judgeSynchronized() throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(ROOT, false);
        return list.size() == this.nodeCnt;
    }

    private void synchronizeNode() throws KeeperException, InterruptedException {
        zk.getChildren(ROOT, true);
        synchronized (this.mutex) {
            LOGGER.info(this.logPrefix + " waits for queue synchronizing.");
            mutex.wait();
        }
    }

    public void synchronizeQueue() throws KeeperException, InterruptedException {
        createNode();
        while (!judgeSynchronized()) {
            synchronizeNode();
        }
        doWork();
    }

    private void doWork() throws InterruptedException {
        LOGGER.info(logPrefix + " begin to do work.");
        Thread.sleep(TASK_INTERVAL_BASE + new Random().nextInt(TASK_INTERVAL_RANGE));
        LOGGER.info(logPrefix + " over do work.");
    }

    public static void main(String[] args) {
        int nodeCnt = 5;
        final int nodeNum = new Random().nextInt(100);
        for (int i = 0; i < NUM_ROUND; i++) {
            LOGGER.info("Node " + nodeNum + " join Barrier Queue.");
            Barrier b = new Barrier(nodeNum, CONNECT_STRING, nodeCnt);
            try {
                b.synchronizeQueue();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info("node-" + nodeNum + " complete round " + i + ".");
        }
        LOGGER.info("node-" + nodeNum + " exit!");
    }
}
