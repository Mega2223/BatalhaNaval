package net.mega2223.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Barrier extends SyncPrimitives {
    int size;
    String id;
    String memberPath;

    public Barrier(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;
        try {
            Stat s = zk.exists(root, false);
            if (s == null) zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) { e.printStackTrace(); }
        try {
            id = InetAddress.getLocalHost().getHostName() + "-" + System.currentTimeMillis();
        } catch (UnknownHostException e) {
            id = "node-" + System.currentTimeMillis();
        }
    }

    public void enter() throws KeeperException, InterruptedException {
        memberPath = zk.create(root + "/member-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() < size) mutex.wait();
                else return;
            }
        }
    }

    public void leave() throws KeeperException, InterruptedException {
        zk.delete(memberPath, -1);
        while (true) {
            synchronized (mutex) {
                List<String> l = zk.getChildren(root, true);
                if (l.size() > 0) mutex.wait();
                else return;
            }
        }
    }
}