package com.qqzeng.zkprimitives;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


public class TestMainClientV1 implements Watcher {

    protected static CountDownLatch countDownLatch = new CountDownLatch(1);

    protected static ZooKeeper zk = null;
    protected static int SESSION_TIMEOUT = 10000;
    protected static String ROOT = "/group-members";
    protected static Integer mutex;

    public TestMainClientV1(String connectString, Integer mutex) {
        this.mutex = mutex;
        if(zk == null){
            try {
                zk = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
            } catch (IOException e) {
                zk = null;
            }
        }
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

}
