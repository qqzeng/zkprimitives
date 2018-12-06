package com.qqzeng.zkprimitives.leaderElection;

/**
 * Created by qqzeng.
 */

import com.qqzeng.zkprimitives.TestMainClientV1;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.Random;

/**
 *
 */
public class LeaderElectionV1 extends TestMainClientV1 {
    private static final Logger LOGGER = Logger.getLogger(LeaderElectionV1.class);

    private static final String CONNECT_STRING = "127.0.0.1:2181";

    private int nodeNum;
    private String logPrefix;

    public LeaderElectionV1(int nodeNum, Integer mutex, String hostport, String root) {
        super(hostport, mutex);
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

    private void findLeader() throws InterruptedException, KeeperException {
        byte[] leader = null;
        leader = getLeaderNode(leader);
        if (leader != null) {
            following();
        } else {
            String newLeader = null;
            try {
                Thread.sleep(new Random().nextInt(1000));
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
                leading();
            } else {
//                leader = getLeaderNode(leader);
//                if (leader != null) {
//                    following();
//                    return;
//                }
                synchronized (mutex) {
                    LOGGER.info(logPrefix + " waits for leader notification...");
                    mutex.wait();
                }
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

    @Override
    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        Event.EventType type = event.getType();
        if (Event.KeeperState.SyncConnected == state) {
            LOGGER.info(type + " === " + event.getPath()) ;
            if (type == Event.EventType.None) {
                countDownLatch.countDown();
                LOGGER.info(this.logPrefix  + " zooKeeper connection created !");
                try {
                    zk.getData(ROOT, true, null);
                    LOGGER.info(logPrefix + " register watch event.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (type == Event.EventType.NodeChildrenChanged) {
                LOGGER.info(logPrefix + " lock node: " + event.getPath() +" created !");
                super.process(event);
                following();
            }
//            else if (type == Event.EventType.NodeCreated && event.getPath().equals(ROOT + "/leader")) {
//                LOGGER.info(logPrefix + " lock node: " + event.getPath() +" deleted !");
//                super.process(event);
//                following();
//            }
        }
    }

    void leading() {
        LOGGER.info(this.logPrefix + " step forward.");
    }

    void following() {
        LOGGER.info(this.logPrefix  + " turn to member.");
    }

    public static void main(String[] args) {
        Integer mutex = new Integer(-1);
        for (int i = 1; i < 4; i++) {
            final int nodeNum = i;
            new Thread(() -> {
                LOGGER.info("Node " + nodeNum + " begin to join leader election.");
                LeaderElectionV1 le = new LeaderElectionV1(nodeNum, mutex, CONNECT_STRING, ROOT);
                try {
                    le.findLeader();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                LOGGER.info(nodeNum + "-node over!" );
            }).start();
        }
    }
}

