package com.qqzeng.zkprimitives;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.log4j.Logger;

public class ZookeeperBase {

    private final static Logger LOGGER = Logger.getLogger(ZookeeperBase.class);

    //    private static String HOST_PORT = "127.0.0.1:2181";
    private static String HOST_PORT = "127.0.0.1:2181";
    private static int CONNECTION_TIMEOUT = 2000;
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        try {
            // create connection with zookeeper server.
            ZooKeeper zk = new ZooKeeper(HOST_PORT,
                    CONNECTION_TIMEOUT, new Watcher() {
                // watch event call.
                public void process(WatchedEvent event) {
                    // acquire connection state.
                    KeeperState keeperState = event.getState();
                    // get event type.
                    EventType eventType = event.getType();
                    if (KeeperState.SyncConnected == keeperState) {
                        if (EventType.None == eventType) {
                            connectedSemaphore.countDown();
                            LOGGER.info("Event: " + event.getType() + " triggered!");
                        }
                    }

                }
            });
            connectedSemaphore.await();
            LOGGER.info(zk.toString());
            // create a directory node.
            zk.create("/testRootPath", "testRoot-Data".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            // create a sub-directory of /testRootPath, and get data from /testRootPath
            zk.create("/testRootPath/testChildPathOne", "testChild-DataOne".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info(new String(zk.getData("/testRootPath", false, null)));

            // list children nodes list of /testRootPath.
            LOGGER.info(zk.getChildren("/testRootPath", true));

            // update data of children node.
            zk.setData("/testRootPath/testChildPathOne", "modifyChild-DataOne".getBytes(), -1);
            LOGGER.info("Node state: " + zk.exists("/testRootPath", true) );

            // create a sub-directory again with MODE CreateMode.PERSISTENT.
            zk.create("/testRootPath/testChildPathTwo", "testChild-DataTwo".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info(new String(zk.getData("/testRootPath/testChildPathTwo", true, null)));

            // delete children nodes by matching any versions.
            zk.delete("/testRootPath/testChildPathTwo", -1);
            zk.delete("/testRootPath/testChildPathOne", -1);

            // remove parent node.
            zk.delete("/testRootPath", -1);

            // close connection with zookeeper server.
            zk.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

}
