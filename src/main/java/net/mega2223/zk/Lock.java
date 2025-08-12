package net.mega2223.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class Lock extends SyncPrimitives {
    String lockRoot;
    String myPath;

    public Lock(String address, String name) {
        super(address);
        this.lockRoot = name;
        try {
            Stat s = zk.exists(lockRoot, false);
            if (s == null) zk.create(lockRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) { e.printStackTrace(); }
    }

    public String lock() throws KeeperException, InterruptedException {
        myPath = zk.create(lockRoot + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            synchronized (mutex) {
                List<String> children = zk.getChildren(lockRoot, false);
                int min = Integer.MAX_VALUE; String minChild = null;
                for (String s : children) {
                    String seq = s.substring(s.lastIndexOf('-') + 1);
                    int v = Integer.parseInt(seq);
                    if (v < min) { min = v; minChild = s; }
                }
                String mineSeq = myPath.substring(myPath.lastIndexOf('-') + 1);
                int mine = Integer.parseInt(mineSeq);
                if (mine == min) {
                    return myPath;
                } else {
                    int pred = -1; String predName = null;
                    for (String s : children) {
                        int v = Integer.parseInt(s.substring(s.lastIndexOf('-') + 1));
                        if (v < mine && v > pred) { pred = v; predName = s; }
                    }
                    Stat stat = zk.exists(lockRoot + "/" + predName, this);
                    if (stat != null) mutex.wait();
                }
            }
        }
    }

    public void unlock() {
        try {
            if (myPath != null) {
                zk.delete(myPath, -1);
                myPath = null;
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
}
