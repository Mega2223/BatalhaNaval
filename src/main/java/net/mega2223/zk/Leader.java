package net.mega2223.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class Leader extends SyncPrimitives {
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

    public boolean elect() throws KeeperException, InterruptedException {
        myPath = zk.create(electionRoot + "/n-", id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> list = zk.getChildren(electionRoot, false);
        list.sort((s1, s2) -> {
            int seq1 = Integer.parseInt(s1.substring(s1.lastIndexOf('-') + 1));
            int seq2 = Integer.parseInt(s2.substring(s2.lastIndexOf('-') + 1));
            return Integer.compare(seq1, seq2);
        });

        String myNodeName = myPath.substring(myPath.lastIndexOf('/') + 1);
        String leaderNodeName = list.get(0);

        if (myNodeName.equals(leaderNodeName)) {
            Stat s = zk.exists(leaderNode, false);
            if (s == null) zk.create(leaderNode, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            else zk.setData(leaderNode, id.getBytes(), -1);
            return true;
        } else {
            return false;
        }
    }

    public String getLeaderId() throws KeeperException, InterruptedException {
        Stat s = zk.exists(leaderNode, false);
        if (s != null) {
            byte[] d = zk.getData(leaderNode, false, s);
            try { return new String(d, "UTF-8"); } catch (Exception e) { return null; }
        }
        return null;
    }
}
