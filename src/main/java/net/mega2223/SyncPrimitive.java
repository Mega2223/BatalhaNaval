package net.mega2223;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SyncPrimitive implements Watcher {
    static final ZooKeeper zk = Main.zk;
    static Integer mutex;

    String root;

    public SyncPrimitive() {
        mutex = new Integer(-1);
    }

    /**
     * É chamado quando pedimos o getChildren que fique de olho,
     * notificamos nosso mutex para resumir a thread lá embaixo
     * */
    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public static class Barrier extends SyncPrimitive{
        String name;
        int size;

        Barrier(String root, int size) {
            super();
            this.root = root;
            this.size = size;
            // Create barrier node

            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, "Teste".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                System.out.println("Keeper exception when instantiating queue: " + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }

            try {
                name = InetAddress.getLocalHost().getCanonicalHostName();
                //
            } catch (UnknownHostException e) {
                System.out.println(e);
            }
        }

        boolean enter() throws KeeperException, InterruptedException{
            zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                //System.out.println("t");
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, this);
                    System.out.println("Entrando na barreira \"" + root + "\" ( "+ list.size() +" / "+ size+" )");
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
}
