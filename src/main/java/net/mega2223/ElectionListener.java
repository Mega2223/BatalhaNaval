package net.mega2223;

import net.mega2223.sync.SyncPrimitive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public abstract class ElectionListener extends SyncPrimitive {

    static final ZooKeeper zk = Main.zk;
    public static final int ELECTION_UNRESPONSIVE_LIMIT_MILIS = 10000;

    int id;
    String root;
    String leader; // Nó efêmero de posse do líder / coordenador,
    // se o mesmo cai quem tá assistindo os descendentes de root vai perceber a perda do lider
    String election; // Criado para puxar uma eleição, todos os clientes estão ouvindo o nó root

    public ElectionListener(String root, int id) throws InterruptedException, KeeperException {
        this.root = root;
        this.id = id;
        leader = root + "/leader";
        election = root + "/election";

        Thread t = new Thread(() -> { // não deve bloquear a lógica principal de onde for chamada
            try {
                begin();
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
    }

    public void begin() throws InterruptedException, KeeperException {
        System.out.println("Eleição declarada");
        if(zk.exists(root,null) == null){
            zk.create(root,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        while(true){
            synchronized (mutex){

                boolean noLeader = zk.exists(leader, null) == null;
                boolean noElection = zk.exists(election, null) == null;
                if(noLeader && noElection){
                    System.out.println(root + " não tem líder, chamando eleição");
                    startElection();
                } else if (noLeader) {
                    participateInElection();
                }

                zk.getChildren(root,this);
                mutex.wait();
            }

        }

    }

    public void startElection() throws InterruptedException, KeeperException {
        if(zk.exists(election,null) == null){
            zk.create(election,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        participateInElection();
    }

    public void participateInElection() throws InterruptedException, KeeperException {
        List<String> candidates = zk.getChildren(election,this);
        for(String candidate : candidates){
            int candidateID = byteSequenceToInt(zk.getData(candidate,false,null));
            if(candidateID > id){
                Thread.sleep(ELECTION_UNRESPONSIVE_LIMIT_MILIS);
                return;
            }
        }
        byte[] idAsByteArray = intToByteSequence(id);
        String myIDNode = zk.create(election+"/",idAsByteArray,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        mutex.wait(ELECTION_UNRESPONSIVE_LIMIT_MILIS);

        // Ou alguem com ID mais alto entrou na eleição ou deu o nosso limite de tempo
        candidates = zk.getChildren(election,null);
        for(String candidate : candidates){
            int candidateID = byteSequenceToInt(zk.getData(election+"/"+candidate,false,null));
            if(candidateID > id){
                Thread.sleep(ELECTION_UNRESPONSIVE_LIMIT_MILIS);
                return;
            }
        }
        // Se ele chegou aqui, ninguém com id mais alto que ele reportou ao node de eleição
        // Logo, ele é o valentão
        System.out.println("Lider da eleição " + election + " é " + id);
        zk.create(leader,intToByteSequence(id),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        for(String child : zk.getChildren(election,null)){
            zk.delete(election+"/"+child,-1);
        }
        zk.delete(election,-1);
    }

    public static byte[] intToByteSequence(int value){
        // Usa operações bitwise para converter o nosso ID (integer) em uma sequencia de bytes
        return new byte[]{
                (byte) value,
                (byte) (value >> 8),
                (byte) (value >> 16),
                (byte) (value >> 24)
        };
    }
    public static int byteSequenceToInt(byte[] sequence){
        // Faz o inverso da operação acima
        return sequence[0] + (sequence[1] << 8) + (sequence[2] << 16) + (sequence[3] << 24);
    }

    public abstract void onLeaderSelected(String leader);
}
