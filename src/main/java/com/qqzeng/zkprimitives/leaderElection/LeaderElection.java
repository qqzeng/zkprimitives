package com.qqzeng.zkprimitives.leaderElection;

/**
 * Created by qqzeng.
 */

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Leader Election, which is corresponding to the Group Membership in paper.
 * <p>
 * About Test:
 * In this demonstration, we want to have every client to step forward once in turn, not just operate a round of leader election.
 * Run the client several times, such as 3 or 5. Every client instance will execute the following process:
 * 1. Create /leader-members in zookeeper server if it does not exist.
 * 2. Sleep for an interval between [SLEEP_INTERVAL_BASE, SLEEP_INTERVAL_BASE + SLEEP_INTERVAL_RANGE].
 * 3. Try to create /leader-members/leader.
 * 4. If success, then exit.
 * 5. Else, block and wait until being triggered by watch event.
 * 6. After being notified (the predecessor has stepped down), it still tries stepping forward by doing [3-6] again.
 */
public class LeaderElection {
    private static final Logger LOGGER = Logger.getLogger(LeaderElection.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;

    private ZooKeeper zk = null;
    private static final int SESSION_TIMEOUT = 10000;
    private static final String ROOT = "/group-members";
    private Object mutex;

    private boolean isLeader;

    private static final int SLEEP_INTERVAL_BASE = 4000;
    private static final int SLEEP_INTERVAL_RANGE = 5000;

    protected static CountDownLatch countDownLatch = new CountDownLatch(1);

    public LeaderElection(int nodeNum, String hostport, String root) {
        this.mutex = new Object();
        this.nodeNum = nodeNum;
        this.logPrefix = "node-" + nodeNum;
        try {
            zk = new ZooKeeper(hostport, SESSION_TIMEOUT, (event) -> {
                Event.KeeperState state = event.getState();
                Event.EventType type = event.getType();
                if (Event.KeeperState.SyncConnected == state) {
                    if (type == Event.EventType.None) {
                        countDownLatch.countDown();
                        LOGGER.info(logPrefix + " zooKeeper connection created !");
                        createRoot(root);
                    } else if (!isLeader && type == Event.EventType.NodeChildrenChanged) {
                        LOGGER.info(logPrefix + " lock node: " + event.getPath() + " has updated its child node!");
                        synchronized (mutex) {
                            mutex.notify();
                        }
                    }
                }
            });
        } catch (IOException e) {
            zk = null;
            e.printStackTrace();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void createRoot(String root) {
        try {
            Stat s = zk.exists(root, false);
            if (s == null) {
                String rootNode = zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info(this.logPrefix + rootNode + " create successfully !");
            }
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    private void findLeader() throws InterruptedException, KeeperException {
        byte[] leader = getLeaderNode();
        if (leader != null) {
            following();
            /* if client finds the leader immediately, it should turns to follower and the leader election is over.
            *  But in this demonstration, we want to have every node to step forward once in turn.
            *  So, it also wait to try becoming a leader.
            */
            waitFor();
            findLeader();
        } else {
            String newLeader = null;
            try {
                Thread.sleep(SLEEP_INTERVAL_BASE + new Random().nextInt(SLEEP_INTERVAL_RANGE));
                newLeader = zk.create(ROOT + "/leader", ("node-" + nodeNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    LOGGER.error(e);
                } else {
                    throw e;
                }
            }
            if (newLeader != null) {
                LOGGER.info(logPrefix + " created " + ROOT + "/leader.");
                this.isLeader = true;
                leading();
            } else {
                waitFor();
                findLeader();
            }
        }
    }

    private byte[] getLeaderNode() throws InterruptedException, KeeperException {
        byte[] leader = null;
        try {
            leader = zk.getData(ROOT + "/leader", false, null);
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                LOGGER.error(e);
            } else {
                throw e;
            }
        }
        return leader;
    }

    private void waitFor() throws KeeperException, InterruptedException {
        synchronized (mutex) {
            zk.getChildren(ROOT, true);
            LOGGER.info(logPrefix + " register successfully!");
            LOGGER.info(logPrefix + " waits for NodeChildrenChanged event.");
            mutex.wait();
        }
    }

    private void leading() {
        LOGGER.info(this.logPrefix + " step forward to leader");
    }

    private void following() {
        LOGGER.info(this.logPrefix + " turn to member.");
    }

    public static void main(String[] args) {
        final int nodeNum = new Random().nextInt(100);
        LOGGER.info("Node " + nodeNum + " begin to join leader election.");
        LeaderElection le = new LeaderElection(nodeNum, CONNECT_STRING, ROOT);
        try {
            le.findLeader();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("node-" + nodeNum + " over!");
    }
}

