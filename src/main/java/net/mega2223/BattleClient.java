package net.mega2223;

import net.mega2223.sync.Barrier;
import net.mega2223.sync.Lock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class BattleClient {
    static final ZooKeeper zk = Main.zk;

    String gameRoot = null;
    String fields = null;
    String myFieldRoot = null;
    BattleshipField myField, enemyField;
    int id, enemyID;
    Integer leaderID = -1;

    public BattleClient() throws InterruptedException, KeeperException, IOException {
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
            // Ephemerals nao podem ter subnos
            fields = zk.create(gameRoot+"/fields", new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            myFieldRoot = zk.create(fields + "/" + Main.PROCESS_ID, new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        Barrier join = new Barrier(gameRoot+"/join_barrier",2);
        join.enter();

        System.out.println("Starting game " + gameRoot);
        zk.setData(gameRoot,new byte[]{1},-1);

        Barrier begin = new Barrier(gameRoot+"/begin_barrier",2);
        begin.enter();

        myField = new BattleshipField(false);
        enemyField = new BattleshipField(true);

        id = Integer.parseInt(Main.PROCESS_ID);

        ElectionListener coordElection = new ElectionListener(gameRoot+"/elections",id) {
            @Override
            public void onLeaderSelected(int leader) {
                System.out.println("Resultado de eleição da partida " + gameRoot + " -> " + leader);
                leaderID = leader;
                //System.out.println(leaderID);
            }
        };

        System.out.println("Aguardando eleição de coordenador");

        while (leaderID == -1){
            Thread.sleep(100);
        }

        System.out.println(leaderID + " selecionado como coordenador");
        boolean amLeader = leaderID == Integer.parseInt(Main.PROCESS_ID);
        if(amLeader){
            System.out.println("Eu sou o líder");
        }
        //TODO if leaderID == id distributeresources...


        Lock turnLock = new Lock(gameRoot+"/lock");

        BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in));

        int i = 0;
        while (i < 100){
            turnLock.lock();

            System.out.println("Seu campo");
            myField.print();
            System.out.println("Inimigo");
            enemyField.print();

            System.out.println("Seu turno, insira o (x,y) onde deseja bombardear");

            int x, y;
            while (true){
                try{
                    String[] data = consoleReader.readLine().split(" ");
                    x = Integer.parseInt(data[0]); y = Integer.parseInt(data[1]);
                    break;
                } catch (NumberFormatException ignored){
                    System.out.println("Coordenadas inválidas, tente novamente");
                }
            }

            enemyField.bomb(x,y);
            byte[] data = {(byte) x, (byte) y};

            turnLock.unlock();
            i++;
        }
    }

}
