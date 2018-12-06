package com.qqzeng.zkprimitives.leaderElection;

/**
 * Created by qqzeng.
 */

import com.qqzeng.zkprimitives.TestMainClientV2;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class LeaderElection {
    private static final Logger LOGGER = Logger.getLogger(LeaderElection.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;

    protected static CountDownLatch countDownLatch = new CountDownLatch(1);

    protected ZooKeeper zk = null;
    protected static int SESSION_TIMEOUT = 10000;
    protected static String ROOT = "/group-members";
    protected static Integer mutex;

    public LeaderElection(int nodeNum, String hostport, String root) {
        if(zk == null){
            try {
                mutex = new Integer(-1);
                zk = new ZooKeeper(hostport, SESSION_TIMEOUT, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        Event.KeeperState state = event.getState();
                        Event.EventType type = event.getType();
                        if (Event.KeeperState.SyncConnected == state) {
                            if (type == Event.EventType.None) {
                                countDownLatch.countDown();
                                LOGGER.info(logPrefix + " zooKeeper connection created !");
                                try {
                                    zk.getData(ROOT, true, null);
                                    LOGGER.info(logPrefix + " register successfully!");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (type == Event.EventType.NodeCreated && event.getPath().equals(ROOT + "/leader")) {
                                LOGGER.info(logPrefix + " lock node: " + event.getPath() +" created !");
                                synchronized (mutex) {
                                    mutex.notify();
                                }
                                following();
                            } else if (type == Event.EventType.NodeChildrenChanged) {
                                LOGGER.info(logPrefix + " lock node: " + event.getPath() +" deleted !");
                                synchronized (mutex) {
                                    mutex.notify();
                                }
                                following();
                            }
                        }
                    }
                });
            } catch (IOException e) {
                zk = null;
            }
        }
        this.nodeNum = nodeNum;
        this.logPrefix = "node-" + nodeNum;
        if (zk != null) {
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
    }

    private  void findLeader() throws InterruptedException, KeeperException {
        byte[] leader = null;
        leader = getLeaderNode(leader);
        if (leader != null) {
            following();
        } else {
            String newLeader = null;
            try {
                Thread.sleep(4000 + new Random().nextInt(3000));
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
                zk.create(ROOT + "/test", "testData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                leading();
            } else {
                zk.getData(ROOT, true, null);
                LOGGER.info("waiting...........");
                Thread.sleep(10000);
            }
        }
    }

    private byte[] getLeaderNode(byte[] leader) throws InterruptedException, KeeperException {
        try {
            leader = zk.getData(ROOT + "/leader", true, null);
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                LOGGER.error(e);
            } else {
                throw e;
            }
        }
        return leader;
    }



    private void leading() {
        LOGGER.info(this.logPrefix + " step forward to Leader");
    }

    private void following() {
        LOGGER.info(this.logPrefix + " turn to member.");
    }

    public static void main(String[] args) {
        final int nodeNum = new Random().nextInt(10);
        LOGGER.info("Node " + nodeNum + " begin to join leader election.");
        LeaderElection le = new LeaderElection(nodeNum, CONNECT_STRING, ROOT);
        try {
            le.findLeader();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.info("node-" + nodeNum + " over!");
    }
}

