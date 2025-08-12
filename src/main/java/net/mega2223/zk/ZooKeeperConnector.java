package net.mega2223.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooKeeperConnector {
    private ZooKeeper zk;

    public void connect(String hostPort, Watcher externalWatcher) throws IOException, InterruptedException {
        CountDownLatch connected = new CountDownLatch(1);

        zk = new ZooKeeper(hostPort, 3000, (WatchedEvent event) -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
            if (externalWatcher != null) externalWatcher.process(event);
        });

        if (!connected.await(5, TimeUnit.SECONDS)) {
            throw new IOException("Falha ao conectar ao ZooKeeper em " + hostPort + " (timeout)");
        }
    }

    public ZooKeeper getZooKeeper() { return zk; }

    public void close() throws InterruptedException { if (zk != null) zk.close(); }
}
