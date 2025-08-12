package net.mega2223.sync;

import net.mega2223.Main;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher {
    static final ZooKeeper zk = Main.zk;
    protected static Integer mutex;

    String root;

    public SyncPrimitive() {
        mutex = new Integer(-1);
    }

    /**
     * É chamado quando pedimos o getChildren que fique de olho,
     * notificamos nosso mutex para resumir a thread nas nossas subclasses
     * */
    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

}
