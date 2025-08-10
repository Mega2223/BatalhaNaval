package net.mega2223.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * SyncPrimitives: Barrier, Queue, Lock, Leader
 */
public class SyncPrimitives implements Watcher {
    static ZooKeeper zk = null;
    static final Integer mutex = Integer.valueOf(-1);

    String root;

    public SyncPrimitives(String address) {
        if (zk == null) {
            try {
                zk = new ZooKeeper(address, 3000, this);
            } catch (Exception e) {
                e.printStackTrace();
                zk = null;
            }
        }
    }

    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }


    /* ---------------- Barrier ---------------- */
    public static class Barrier extends SyncPrimitives {
        int size;
        String id;
        String memberPath; // Adicionado para armazenar o caminho do membro

        public Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;
            try {
                Stat s = zk.exists(root, false);
                if (s == null) zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                id = InetAddress.getLocalHost().getHostName() + "-" + System.currentTimeMillis();
            } catch (UnknownHostException e) {
                id = "node-" + System.currentTimeMillis();
            }
        }

        public void enter() throws KeeperException, InterruptedException {
            // Unifica a criação do nó e a lógica de espera.
            // O nó é criado aqui, e não no cliente.
            memberPath = zk.create(root + "/member-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    // É crucial usar o segundo parâmetro do getChildren(path, watch) como 'true'
                    // para garantir que o cliente seja notificado (via process()) sobre mudanças.
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() < size) {
                        mutex.wait();
                    } else {
                        return;
                    }
                }
            }
        }

        public void leave() throws KeeperException, InterruptedException {
            // O método leave() agora deleta apenas o nó do próprio cliente.
            // A barreira se fecha quando todos os clientes saem, e não de uma só vez.
            zk.delete(memberPath, -1);
            while (true) {
                synchronized (mutex) {
                    List<String> l = zk.getChildren(root, true);
                    if (l.size() > 0) {
                        mutex.wait();
                    } else {
                        return;
                    }
                }
            }
        }
    }

    /* ---------------- Queue ---------------- */
    public static class Queue extends SyncPrimitives {
        public Queue(String address, String name) {
            super(address);
            this.root = name;
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception e) { e.printStackTrace(); }
            }
        }

        // produce a UTF-8 string payload
        public void produce(String msg) throws KeeperException, InterruptedException {
            byte[] value = new byte[0];
            try { value = msg.getBytes("UTF-8"); } catch (UnsupportedEncodingException ignored) {}
            zk.create(root + "/msg-", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        }

        // consume returns the message data and deletes the znode; blocks until available
        public byte[] consume() throws KeeperException, InterruptedException {
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        mutex.wait();
                    } else {
                        String minString = null;
                        int min = Integer.MAX_VALUE;
                        for (String s : list) {
                            String seq = s.substring(s.lastIndexOf('-') + 1);
                            int v = Integer.parseInt(seq);
                            if (v < min) { min = v; minString = s; }
                        }
                        String path = root + "/" + minString;
                        Stat stat = new Stat();
                        byte[] data = zk.getData(path, false, stat);
                        zk.delete(path, stat.getVersion());
                        return data;
                    }
                }
            }
        }
    }

    /* ---------------- Lock ---------------- */
    public static class Lock extends SyncPrimitives {
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

        // create ephemeral sequential lock node and block until we are lowest
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

        // unlock by deleting the previously created node
        public void unlock() {
            try {
                if (myPath != null) {
                    zk.delete(myPath, -1);
                    myPath = null;
                }
            } catch (Exception e) { e.printStackTrace(); }
        }
    }

    /* ---------------- Leader Election ---------------- */
    public static class Leader extends SyncPrimitives {
        String electionRoot;
        String leaderNode;
        String myPath;
        String id;

        public Leader(String address, String electionRoot, String leaderNode, String id) {
            super(address);
            this.electionRoot = electionRoot;
            this.leaderNode = leaderNode;
            this.id = id;
            try {
                Stat s = zk.exists(electionRoot, false);
                if (s == null) zk.create(electionRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception e) { e.printStackTrace(); }
        }

        // participa da eleição e retorna true se for eleito, sem bloquear
        public boolean elect() throws KeeperException, InterruptedException {
            // 1. Cria um nó sequencial para participar
            myPath = zk.create(electionRoot + "/n-", id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            // 2. Obtém a lista de todos os nós de eleição e ordena-os
            List<String> list = zk.getChildren(electionRoot, false);
            list.sort((s1, s2) -> {
                int seq1 = Integer.parseInt(s1.substring(s1.lastIndexOf('-') + 1));
                int seq2 = Integer.parseInt(s2.substring(s2.lastIndexOf('-') + 1));
                return Integer.compare(seq1, seq2);
            });

            // 3. Verifica se o nosso nó é o primeiro na lista (o menor)
            String myNodeName = myPath.substring(myPath.lastIndexOf('/') + 1);
            String leaderNodeName = list.get(0);

            if (myNodeName.equals(leaderNodeName)) {
                // Se o nosso nó é o líder, criamos o nó de referência
                Stat s = zk.exists(leaderNode, false);
                if (s == null) zk.create(leaderNode, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                else zk.setData(leaderNode, id.getBytes(), -1);
                return true;
            } else {
                // Se não somos o líder, retornamos false imediatamente
                return false;
            }
        }

        // get current leader id (if exists)
        public String getLeaderId() throws KeeperException, InterruptedException {
            Stat s = zk.exists(leaderNode, false);
            if (s != null) {
                byte[] d = zk.getData(leaderNode, false, s);
                try { return new String(d, "UTF-8"); } catch (Exception e) { return null; }
            }
            return null;
        }
    }
}
