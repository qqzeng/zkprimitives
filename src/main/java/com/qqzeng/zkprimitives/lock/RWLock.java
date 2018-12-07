package com.qqzeng.zkprimitives.lock;
/**
 * Created by qqzeng.
 */
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * ReadWrite(Shared) lock, which is corresponding to Read/Write Locks in paper.
 * <p>
 * For a specific transaction, if it is write transaction, then it has to wait all
 * client which sequence number is smaller than its.
 * if it is read transaction, then it has to wait the client which executes write
 * transaction and orders just before it.
 * <p>
 * In this algorithm, many client read transaction will suffer from herd effect.
 *
 * About Test:
 * Run the client several times, such as 3 or 5. Every client instance will execute the following process:
 *      1. Create /ReadWrite-lock in zookeeper server if it does not exist.
 *      2. Sleep for an interval between [SLEEP_INTERVAL_BASE, SLEEP_INTERVAL_BASE + SLEEP_INTERVAL_RANGE].
 *      3. Create /exclusive-lock-advanced/lock_xxx.
 *      4. Try to acquire the lock.
 *      5. If success, then sleep for an interval between [TASK_INTERVAL_BASE, TASK_INTERVAL_BASE + TASK_INTERVAL_RANGE].
 *      6. one round test over, continue until reaching to NUM_ROUND times。
 * <p>
 *  You can also test read and write transaction sets according to custom order.
 *  For example, test such a transaction set: [w, r, r, r, w, w, r, r, w]
 *  see comment code in testRandom().
 */
public class RWLock {

    private static final Logger LOGGER = Logger.getLogger(ExclusiveLockAdv.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private ZooKeeper zk = null;
    private static final int SESSION_TIMEOUT = 10000;
    private static final String ROOT = "/ReadWrite-lock"; // sharedLock
    private final Object mutex;
    private String myZnode;

    private static final int NUM_ROUND = 1;
    private static final int SLEEP_INTERVAL_BASE = 2000;
    private static final int SLEEP_INTERVAL_RANGE = 3000;
    private static final int TASK_INTERVAL_BASE = 4000;
    private static final int TASK_INTERVAL_RANGE = 4000;

    /* Inner class providing readlock */
    private final RWLock.ReadLock rLock;
    /* Inner class providing writelock */
    private final RWLock.WriteLock wLock;

    public RWLock(int nodeNum, String hostport) {
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
        rLock = new ReadLock(this);
        wLock = new WriteLock(this);
    }

    public RWLock.ReadLock readLock() {
        return rLock;
    }

    public RWLock.WriteLock writeLock() {
        return wLock;
    }

    private static class Lock {
        protected void getLock() throws KeeperException, InterruptedException {
        }
    }

    private static class ReadLock extends Lock {

        private final RWLock rwLock;

        public ReadLock(RWLock rwLock) {
            this.rwLock = rwLock;
        }

        public void tryLock() throws InterruptedException, KeeperException {
            Thread.sleep(SLEEP_INTERVAL_BASE + new Random().nextInt(SLEEP_INTERVAL_RANGE));
            rwLock.myZnode = rwLock.zk.create(ROOT + "/lock-r_", ("node-" + rwLock.nodeNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            getLock();
        }

        protected void getLock() throws KeeperException, InterruptedException {
            List<String> list = rwLock.zk.getChildren(ROOT, false);
            list.sort((o1, o2) -> {
                // if the other and I are read transaction(I am the first), then I sit in front, i.e. return -1;
                if (rwLock.myZnode.equals(RWLock.ROOT + "/" + o1)) {
                    if (o1.startsWith("lock-r") && o2.startsWith("lock-r")) {
                        return -1;
                    }
                }
                // if the other and I are read transaction(I am the second), then I sit in front, i.e. return 1;
                else if (rwLock.myZnode.equals(RWLock.ROOT + "/" + o2)) {
                    if (o1.startsWith("lock-r") && o2.startsWith("lock-r")) {
                        return 1;
                    }
                }
                String seqNum1 = o1.substring(o1.lastIndexOf("_") + 1);
                String seqNum2 = o2.substring(o2.lastIndexOf("_") + 1);
                return seqNum1.compareTo(seqNum2);
            });
            System.out.println(list);
            rwLock.onGetLock(list, this);
        }
    }

    private static class WriteLock extends Lock {

        private final RWLock rwLock;

        public WriteLock(RWLock rwLock) {
            this.rwLock = rwLock;
        }

        public void tryLock() throws InterruptedException, KeeperException {
            Thread.sleep(SLEEP_INTERVAL_BASE + new Random().nextInt(SLEEP_INTERVAL_RANGE));
            rwLock.myZnode = rwLock.zk.create(ROOT + "/lock-w_", ("node-" + rwLock.nodeNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            getLock();
        }

        protected void getLock() throws KeeperException, InterruptedException {
            List<String> list = rwLock.zk.getChildren(ROOT, false);
            // if I am write transaction, then all nodes will be ordered by the sequence number.
            list.sort((o1, o2) -> {
                String seqNum1 = o1.substring(o1.lastIndexOf("_") + 1);
                String seqNum2 = o2.substring(o2.lastIndexOf("_") + 1);
                return seqNum1.compareTo(seqNum2);
            });
            rwLock.onGetLock(list, this);
        }
    }

    private void waitForLock(String node, Lock lock) throws InterruptedException, KeeperException {

        if (lock instanceof RWLock.ReadLock) {
            lock = this.rLock;
        } else {
            lock = this.wLock;
        }
        Stat stat = zk.exists(ROOT + "/" + node, true);
        if (stat != null) {
            synchronized (mutex) {
                LOGGER.info(logPrefix + " waits for the lock" + "[" + node + "].");
                mutex.wait();
                lock.getLock();
            }
        } else {
            lock.getLock();
        }
    }

    private void onGetLock(List<String> list, Lock lock) throws KeeperException, InterruptedException {
        if (myZnode.equals(RWLock.ROOT + "/" + list.get(0))) {
            LOGGER.info(logPrefix + "[" + myZnode + "]" + " acquires the lock successfully.");
            doWork();
            releaseLock();
        } else {
            String myID = myZnode.substring(myZnode.lastIndexOf("/") + 1);
            String prevNode = list.get(list.indexOf(myID) - 1);
            waitForLock(prevNode, lock);
        }
    }

    private void doWork() throws InterruptedException {
        LOGGER.info(logPrefix + " begin to do work.");
        Thread.sleep(TASK_INTERVAL_BASE + new Random().nextInt(TASK_INTERVAL_RANGE));
        LOGGER.info(logPrefix + " over do work.");
    }

    public void releaseLock() throws KeeperException, InterruptedException {
        LOGGER.info(this.logPrefix + " begin to release the lock.");
        zk.delete(myZnode, -1);
        LOGGER.info(this.logPrefix + " release the lock successfully.");
    }


    public static void testRandomRW() {
        final int nodeNum = new Random().nextInt(100);
        for (int i = 0; i < NUM_ROUND; i++) {
            LOGGER.info("Node " + nodeNum + " try to acquire the ReadWrite lock.");
            RWLock rwLock = new RWLock(nodeNum, CONNECT_STRING);
            try {
                // [w, r, r, r, w, w, r, r, w]
//                 boolean readTrue = true;
//                 if (readTrue){
                if (new Random().nextInt(10) > 3) {
                    rwLock.readLock().tryLock();
                } else {
                    rwLock.writeLock().tryLock();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info("node-" + nodeNum + " complete round " + i + ".");
        }
        LOGGER.info("node-" + nodeNum + " exit!");
    }

    public static void main(String[] args) {
        testRandomRW();
    }

}
