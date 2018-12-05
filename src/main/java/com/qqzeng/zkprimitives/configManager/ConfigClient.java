package com.qqzeng.zkprimitives.configManager;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/***
 *  Configclient based on Zookeeper which demonstrates dynamic configuration manager.
 */
public class ConfigClient {
    private final static Logger LOGGER = Logger.getLogger(ConfigClient.class);


    private static String HOST_PORT = "127.0.0.1:2181";
    private static int CONNECTION_TIMEOUT = 2000;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static final AtomicInteger seq = new AtomicInteger();

    private static String url = "";
    private static String username = "";
    private static String password = "";

    private static String AUTH_METHOD = "digest";
    private static String PWD = "pwd";

    private static ZooKeeper zk = null;

    /**
     * Read configuration info from zk server.
     */
    private static void readConfig() throws KeeperException, InterruptedException {
        url = new String(zk.getData("/myconfig/url", true, null));
        username = new String(zk.getData("/myconfig/username", true, null));
        password = new String(zk.getData("/myconfig/pwd", true, null));
    }

    /**
     *  Main entry point.
     */
    public static void main(String[] args) throws Exception {
        zk = new ZooKeeper(HOST_PORT, CONNECTION_TIMEOUT, (event) -> {
            String logPrefix = "[Watcher-" + seq.incrementAndGet() + "]";
            KeeperState state = event.getState();
            EventType type = event.getType();
            if (KeeperState.SyncConnected == state) {
                if (type == EventType.None) {
                    countDownLatch.countDown();
                    LOGGER.info("ZooKeeper connection created !");
                    try {
                        readConfig();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (EventType.NodeCreated == type) {
                    LOGGER.info(logPrefix + " node created");
                } else if (EventType.NodeDataChanged == type) {
                    LOGGER.info(logPrefix + "node data changed");
                    try {
                        readConfig();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (EventType.NodeChildrenChanged == type) {
                    LOGGER.info(logPrefix + "node child list updated");
                    try {
                        readConfig();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (EventType.NodeDeleted == type) {
                    LOGGER.info(logPrefix + "node deleted");
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
        zk.addAuthInfo(AUTH_METHOD, PWD.getBytes());
        LOGGER.info("Authorization success !");
        // monitor global configuration state.
        configMonitor();

        // update configuration data synchronously.
        new Thread(() -> {
            try {
                updateClient();
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }).start();
    }

    /**
     *  Monitor configuration info update.
     */
    private static void configMonitor() {
        // output config update info.
        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                LOGGER.info("Live zookeeper connection...");
                LOGGER.info("url  :" + url);
                LOGGER.info("username  :" + username);
                LOGGER.info("password  :" + password);
            }
        }, 0, 2000);
    }

    /**
     * Client used to update configuration info.
     */
    private static void updateClient() throws IOException, InterruptedException, KeeperException {
        CountDownLatch connectedSemaphore = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(HOST_PORT, CONNECTION_TIMEOUT,
                (event) -> {
                    KeeperState state = event.getState();
                    EventType type = event.getType();
                    if (state == KeeperState.SyncConnected) {
                        if (type == EventType.None) {
                            connectedSemaphore.countDown();
                        }
                    }
                });
        connectedSemaphore.await(50000, TimeUnit.MILLISECONDS);
        zk.addAuthInfo(AUTH_METHOD, PWD.getBytes());


        LOGGER.info(new String(zk.getData("/myconfig/url", false, null)));
        LOGGER.info(new String(zk.getData("/myconfig/pwd", false, null)));
        LOGGER.info(new String(zk.getData("/myconfig/username", false, null)));

        Thread.sleep(5000);
        zk.setData("/myconfig/username", ("new-username2").getBytes(), -1);
        zk.setData("/myconfig/pwd", ("new-pwd2").getBytes(), -1);
        LOGGER.info("Update configuration data.");
        zk.close();
    }

}
