package net.mega2223;

import net.mega2223.sync.Barrier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class BattleClient {
    static final ZooKeeper zk = Main.zk;

    String gameRoot = null;
    BattleshipGame game;

    public BattleClient() throws InterruptedException, KeeperException {
        if(zk.exists("/games",false) == null){
            System.out.println("creating games node");
            zk.create("/games",new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        System.out.println("zk = " + zk);

        List<String> currentGames = zk.getChildren("/games",false);
        System.out.println("Games: " + currentGames.size());

        for(String act : currentGames){
            String loc = "/games/" + act;
            byte data = zk.getData(loc, false, null)[0];
            System.out.println("GAME: " + act + "  DATA: " + data);
            if(data == 0){
                gameRoot = loc;
                break;
            }
        }

        if(gameRoot == null){
            byte[] data = new byte[1];
            //System.out.println("CREATED "+result);
            gameRoot = zk.create("/games/game", data,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            // Ephemerals nao podem ter sequenciais
            //zk.create(game+"/barrier",data,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        Barrier join = new Barrier(gameRoot+"/join_barrier",2);
        join.enter();

        System.out.println("Starting game " + gameRoot);
        zk.setData(gameRoot,new byte[]{1},-1);

        Barrier begin = new Barrier(gameRoot+"/begin_barrier",2);
        begin.enter();

        game = new BattleshipGame();

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
    }
}
