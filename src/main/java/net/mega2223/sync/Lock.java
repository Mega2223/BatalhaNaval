package net.mega2223.sync;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

public class Lock extends SyncPrimitive {

    boolean ownsLock = false;

    public Lock(String name) {
        this.root = name;
        try {
            Stat s = zk.exists(root, false);
            if (s == null) {
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            System.out.println("Keeper exception when instantiating queue: " + e.toString());
        } catch (InterruptedException e) {
            System.out.println("Interrupted exception");
        }
    }

    Random r = new Random();

    public boolean lock() throws InterruptedException{
        synchronized (mutex){
            System.out.println("Tentando adquirir uma lock em " + root);
            while(true){
                try{
                    zk.getChildren(root,this);
                    zk.create(
                            root+"/owner",
                            new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL
                    );
                    System.out.println("Consegui a lock de " + root);
                    ownsLock = true;
                    return true;
                } catch (KeeperException ignored) {
                    try {
                        zk.getChildren(root,this);
                    } catch (KeeperException ignored2) {}
                    mutex.wait(
                            20+r.nextInt(100)
                    );
                }
            }
        }
    }

    public boolean unlock() throws InterruptedException, KeeperException {
        if(ownsLock){
            System.out.println("Saindo da lock " + root);
            zk.delete(root+"/owner",-1);
            ownsLock = false;
            Thread.sleep(100);
            return true;
        }
        return false;
    }
}
