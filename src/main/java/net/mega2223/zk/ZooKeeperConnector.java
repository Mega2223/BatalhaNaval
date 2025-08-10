package net.mega2223.zk;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public class ZooKeeperConnector {
    private ZooKeeper zk;

    /**
     * Conecta ao ZooKeeper.
     * @param hostPort ex: "localhost:2181"
     * @param watcher Watcher para receber eventos
     * @throws IOException
     */
    public void connect(String hostPort, Watcher watcher) throws IOException {
        zk = new ZooKeeper(hostPort, 3000, watcher);
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    public void close() throws InterruptedException {
        if (zk != null) zk.close();
    }
}
