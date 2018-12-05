package com.qqzeng.zkprimitives.configManager;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/***
 * ConfigServer aimed to initializing configuration info.
 */
public class ConfigServer {
	private final static Logger LOGGER = Logger.getLogger(ConfigServer.class);

	private static String HOST_PORT = "127.0.0.1:2181";
	private static int CONNECTION_TIMEOUT = 2000;
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

	private static String URL = "10.1.18.2";
	private static String USERNAME = "admin";
	private static String PASSWORD = "123456";

	private static String AUTH_METHOD = "digest";
	private static String PWD = "pwd";

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {

		ZooKeeper zk = new ZooKeeper(HOST_PORT, CONNECTION_TIMEOUT,
				(event) -> {
						KeeperState state = event.getState();
						EventType type = event.getType();
						if (state == KeeperState.SyncConnected) {
							if (type == EventType.None) {
								connectedSemaphore.countDown();
								LOGGER.info("Zookeeper connection created !");
							}
						}
				});

		connectedSemaphore.await(50000, TimeUnit.MILLISECONDS);

		zk.addAuthInfo(AUTH_METHOD, PWD.getBytes());

		// create configuration node and store data.
		if (zk.exists("/myconfig", false) == null) {
			zk.create("/myconfig", null, Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if (zk.exists("/myconfig/url", false) == null) {
			zk.create("/myconfig/url", URL.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);

		}
		if (zk.exists("/myconfig/pwd", false) == null) {
			zk.create("/myconfig/pwd", PASSWORD.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if (zk.exists("/myconfig/username", false) == null) {
			zk.create("/myconfig/username", USERNAME.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		LOGGER.info(new String(zk.getData("/myconfig/url", false, null)));
		LOGGER.info(new String(zk.getData("/myconfig/pwd", false, null)));
		LOGGER.info(new String(zk.getData("/myconfig/username", false, null)));
		zk.close();
	}
}
