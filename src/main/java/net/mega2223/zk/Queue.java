package net.mega2223.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Queue extends SyncPrimitives {
    public Queue(String address, String name) {
        super(address);
        this.root = name;
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception e) { e.printStackTrace(); }
        }
    }

    public void produce(String msg) throws KeeperException, InterruptedException {
        byte[] value = new byte[0];
        try { value = msg.getBytes("UTF-8"); } catch (UnsupportedEncodingException ignored) {}
        zk.create(root + "/msg-", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public byte[] consume() throws KeeperException, InterruptedException {
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.isEmpty()) {
                    mutex.wait();
                } else {
                    String minString = null;
                    int min = Integer.MAX_VALUE;
                    for (String s : list) {
                        String seq = s.substring(s.lastIndexOf('-') + 1);
                        int v = Integer.parseInt(seq);
                        if (v < min) { min = v; minString = s; }
                    }
                    String path = root + "/" + minString;
                    Stat stat = new Stat();
                    byte[] data = zk.getData(path, false, stat);
                    zk.delete(path, stat.getVersion());
                    return data;
                }
            }
        }
    }
}
