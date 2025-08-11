package net.mega2223;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

public class Main {
    public static ZooKeeper zk;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper("localhost", 3000, event -> {
            System.out.println("\nEVENT");
            System.out.println(event);
            System.out.println(event.getType());
            System.out.println(event.getState());
            System.out.println("END EVENT\n");
        });

        if(zk.exists("/games",false) == null){
            System.out.println("creating games");
            zk.create("/games",new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        System.out.println("zk = " + zk);

        List<String> currentGames = zk.getChildren("/games",false);
        System.out.println("Games: " + currentGames.size());
        String game = null;
        for(String act : currentGames){
            String loc = "/games/" + act;
            byte data = zk.getData(loc, false, null)[0];
            System.out.println("GAME: " + act + "  DATA: " + data);
            if(data == 0){
                game = loc;
                break;
            }
        }

        if(game == null){
            byte[] data = new byte[1];
            //System.out.println("CREATED "+result);
            game = zk.create("/games/game", data,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            // Ephemerals nao podem ter sequenciais
            //zk.create(game+"/barrier",data,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        SyncPrimitive.Barrier b = new SyncPrimitive.Barrier(game+"/barrier",2);

        b.enter();
        zk.setData(game,new byte[]{1},-1);

        int i = 0;
        while (i < 100){
            try {
                Thread.sleep(1000);
                System.out.println("zzz " + i);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            i++;
        }
        zk.close();
    }
}