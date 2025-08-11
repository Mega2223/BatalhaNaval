package net.mega2223;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

public class Main {
    public static ZooKeeper zk;
    public static String PROCESS_ID;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper("localhost", 3000, event -> {
            System.out.println("\nEVENT");
            System.out.println(event);
            System.out.println(event.getType());
            System.out.println(event.getState());
            System.out.println("END EVENT\n");
        });

        if(zk.exists("/id",false) == null){
            System.out.println("creating id repo");
            zk.create("/id",new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        new BattleShip().print();
        PROCESS_ID = zk.create("/id",new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("PROCESS_ID = " + PROCESS_ID);

        new BattleClient();

        zk.close();
    }
}