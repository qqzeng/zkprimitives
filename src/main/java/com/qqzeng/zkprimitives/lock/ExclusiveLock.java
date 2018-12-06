package com.qqzeng.zkprimitives.lock;

/**
 * Created by qqzeng.
 */

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Exclusive lock, which is corresponding to Simple Lock in paper.
 * <p>
 * It suffers from the Herd Effect, that is to say, if there are
 * many clients waiting to acquire a lock, they will all vie for
 * the lock when it is released even though only one client can acquire the lock.
 */
public class ExclusiveLock {
    private static final Logger LOGGER = Logger.getLogger(ExclusiveLock.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;

    protected static CountDownLatch countDownLatch = new CountDownLatch(1);

    protected ZooKeeper zk = null;
    protected static int SESSION_TIMEOUT = 10000;
    protected static String ROOT = "/exclusive-lock";
    protected final Object mutex;

    public ExclusiveLock(int nodeNum, String hostport) {
        mutex = new Object();
        this.nodeNum = nodeNum;
        this.logPrefix = "node-" + nodeNum;
        if (zk == null) {
            try {
                zk = new ZooKeeper(hostport, SESSION_TIMEOUT, (event) -> {
                    Watcher.Event.KeeperState state = event.getState();
                    Watcher.Event.EventType type = event.getType();
                    LOGGER.info(type + " ============ " + event.getPath());
                    if (Watcher.Event.KeeperState.SyncConnected == state) {
                        if (type == Watcher.Event.EventType.None) {
                            countDownLatch.countDown();
                            LOGGER.info(logPrefix + " zooKeeper connection created !");
                            try {
                                zk.getData(ROOT, true, null);
                                LOGGER.info(logPrefix + " register successfully!");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else if (type == Watcher.Event.EventType.NodeCreated && event.getPath().equals(ROOT + "/lock")) {
                            LOGGER.info(logPrefix + ": ============= lock node created !");
                            synchronized (mutex) {
                                mutex.notify();
                            }
                            try {
                                tryToAcquireExclusiveLock();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            }
                        } else if (type == Watcher.Event.EventType.NodeChildrenChanged) {
                            LOGGER.info(logPrefix + ": ============================= lock node created !");
                            synchronized (mutex) {
                                mutex.notify();
                            }
                            try {
                                tryToAcquireExclusiveLock();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (KeeperException e) {
                                e.printStackTrace();
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

    private void tryToAcquireExclusiveLock() throws InterruptedException, KeeperException {
        byte[] lockNode = getLockNode();
        if (lockNode != null) {
            waitForLock();
        } else {
            String newLock = null;
            try {
                Thread.sleep(4000 + new Random().nextInt(3000));
                newLock = zk.create(ROOT + "/lock", ("node-" + nodeNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    LOGGER.error(e);
                } else {
                    throw e;
                }
            }
            if (newLock != null) {
                LOGGER.info(logPrefix + " acquires the  " + ROOT + "/lock successfully.");
                doWork();
                releaseLock();
            } else {
                waitForLock();
            }
        }
    }

    private byte[] getLockNode() throws InterruptedException, KeeperException {
        byte[] leader = null;
        try {
            leader = zk.getData(ROOT + "/lock", true, null);
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                LOGGER.error(e);
            } else {
                throw e;
            }
        }
        return leader;
    }

    private void releaseLock() throws KeeperException, InterruptedException {
        LOGGER.info(this.logPrefix + " begin to release the lock.");
        zk.delete(ROOT + "/lock", -1);
        LOGGER.info(this.logPrefix + " release the lock successfully.");
    }

    private void waitForLock() throws InterruptedException {
        synchronized (mutex) {
            LOGGER.info(this.logPrefix + " waits for the lock.");
            mutex.wait();
        }
    }

    private void doWork() throws InterruptedException {
        LOGGER.info(logPrefix + " begin to do work.");
        Thread.sleep(2000 + new Random().nextInt(2000));
        LOGGER.info(logPrefix + " over do work.");
    }

    public static void main(String[] args) {
        final int nodeNum = new Random().nextInt(10);
        LOGGER.info("Node " + nodeNum + " try to acquire the distributed exclusive lock.");
        ExclusiveLock el = new ExclusiveLock(nodeNum, CONNECT_STRING);
        try {
            el.tryToAcquireExclusiveLock();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("node-" + nodeNum + " exit!");
    }
}
