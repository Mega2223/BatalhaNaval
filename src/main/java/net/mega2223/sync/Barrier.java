package net.mega2223.sync;

import net.mega2223.Main;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Barrier extends SyncPrimitive {
    String name;
    String nodePath;
    int size;

    public Barrier(String root, int size) {
        super();
        this.root = root;
        this.size = size;
        // Create barrier node

        try {
            Stat s = zk.exists(root, false);
            if (s == null) {
//                nodePath = zk.create(root, "Barrier".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                nodePath = zk.create(root, Main.PROCESS_ID.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            System.out.println("Keeper exception when instantiating queue: " + e);
        } catch (InterruptedException e) {
            System.out.println("Interrupted exception");
        }

        try {
            name = InetAddress.getLocalHost().getCanonicalHostName() + ":" + Main.PROCESS_ID;
            //
        } catch (UnknownHostException e) {
            System.out.println(e);
        }
    }

    public boolean enter() throws KeeperException, InterruptedException {
        zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            //System.out.println("t");
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, this);
                System.out.println("Entrando na barreira \"" + root + "\" ( " + list.size() + " / " + size + " )");
                if (list.size() < size) {
                    mutex.wait();
                } else {
                    System.out.println("Saindo da barreira :)");
                    return true;
                }
            }
        }
    }
}
