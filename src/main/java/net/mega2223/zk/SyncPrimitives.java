package net.mega2223.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public abstract class SyncPrimitives implements Watcher {
    // Campos compartilhados pelas subclasses
    protected static ZooKeeper zk = null;
    protected static final Integer mutex = Integer.valueOf(-1);

    protected String root;

    protected SyncPrimitives(String address) {
        if (zk == null) {
            try {
                zk = new ZooKeeper(address, 3000, this);
            } catch (Exception e) {
                e.printStackTrace();
                zk = null;
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }
}