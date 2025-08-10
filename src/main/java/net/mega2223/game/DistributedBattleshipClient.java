package net.mega2223.game;

import net.mega2223.zk.ZooKeeperConnector;
import net.mega2223.zk.SyncPrimitives;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class DistributedBattleshipClient implements Watcher {
    private final String hostPort;
    private ZooKeeperConnector connector;
    private SyncPrimitives.Queue queue;
    private SyncPrimitives.Barrier barrier;
    private SyncPrimitives.Leader leader;
    private SyncPrimitives.Lock lock;

    private String playerId;
    private String opponentId;
    private Board board;
    private ZooKeeper zk;

    // Guarda o último "lastmove" já exibido neste cliente, para evitar ler resultados antigos.
    private volatile String lastSeenMove = null;

    public DistributedBattleshipClient(String hostPort) throws IOException {
        this.hostPort = hostPort;
        this.connector = new ZooKeeperConnector();
        this.playerId = "p-" + UUID.randomUUID().toString().substring(0,6);
        this.board = new Board();
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (this) {
            notifyAll();
        }
    }

    public void start() throws Exception {
        System.out.println("Esperando Conexão...");

        connector.connect(hostPort, this);
        zk = connector.getZooKeeper();
        System.out.println("Conexão Estabelecida!");

        cleanupZNodes();

        createIfNotExists("/battleship");
        createIfNotExists("/battleship/players");
        createIfNotExists("/battleship/boards");
        createIfNotExists("/battleship/moves");
        createIfNotExists("/battleship/ready");
        createIfNotExists("/battleship/election");
        System.out.println("Caminhos Base Existem!");

        zk.create("/battleship/players/" + playerId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Nó Filho Criado");

        queue = new SyncPrimitives.Queue(hostPort, "/battleship/moves");
        barrier = new SyncPrimitives.Barrier(hostPort, "/battleship/ready", 2);
        leader = new SyncPrimitives.Leader(hostPort, "/battleship/election", "/battleship/leader", playerId);
        lock = new SyncPrimitives.Lock(hostPort, "/battleship/lock");
        System.out.println("Primitivas Iniciadas");

        System.out.println("Começando o Jogo!!!");
        promptPlaceShips();

        Stat s = zk.exists("/battleship/boards/" + playerId, false);
        if (s == null) zk.create("/battleship/boards/" + playerId, board.serialize().getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        else zk.setData("/battleship/boards/" + playerId, board.serialize().getBytes("UTF-8"), -1);

        System.out.println("Esperando outro jogador (barrier)...");
        barrier.enter();
        System.out.println("Ambos prontos! Prosseguindo para eleição de leader...");

        boolean iamLeader = leader.elect();
        if (iamLeader) {
            System.out.println("Fui eleito leader (referee). Vou consumir fila de moves.");
            runReferee();
        } else {
            findOpponent();
            System.out.println("Não sou leader. Aguardando turno. Meu id = " + playerId + ", opponent = " + opponentId);
            runPlayerLoop();
        }
    }

    private void runReferee() throws Exception {
        Stat s = zk.exists("/battleship/turn", true);
        if (s == null) {
            List<String> players = zk.getChildren("/battleship/players", false);
            String firstPlayer = players.get(0);
            System.out.println("Definindo o primeiro jogador como: " + firstPlayer);
            zk.create("/battleship/turn", firstPlayer.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        System.out.println("Referee inicializado. Aguardando turno ou mensagens na fila...");
        Scanner sc = new Scanner(System.in);

        while (true) {
            // Verifica e exibe qualquer resultado novo antes de checar o turno
            checkAndPrintLastMove();

            byte[] turnData = zk.getData("/battleship/turn", true, s);
            String currentTurn = new String(turnData, "UTF-8");

            if (currentTurn.equals(playerId)) {
                System.out.println("É o seu turno (líder)! Digite a coordenada (ex A5): ");
                String coord = sc.nextLine().trim().toUpperCase();
                if (coord.length() == 0) continue;
                queue.produce(playerId + "|" + coord);
                System.out.println("Move enviado, aguardando confirmação...");

                // O líder deve esperar por uma jogada na fila (pode ser a dele ou do oponente)
                byte[] data = queue.consume();
                String lastMove = processMove(data);

                // Exibe o resultado processado (garante que o leader também veja o feedback)
                if (lastMove != null && !lastMove.equals(lastSeenMove)) {
                    System.out.println("Resultado: " + lastMove);
                    lastSeenMove = lastMove;
                }

                // Depois de processar a jogada (que atualiza o /battleship/turn), aguardar
                // explicitamente até que o turno deixe de ser o líder antes de permitir nova jogada.
                waitForTurnChange(playerId);
            } else {
                System.out.println("Aguardando turno de " + currentTurn + "...");

                // O líder espera por uma jogada do oponente
                byte[] data = queue.consume();
                String lastMove = processMove(data);

                if (lastMove != null && !lastMove.equals(lastSeenMove)) {
                    System.out.println("Resultado: " + lastMove);
                    lastSeenMove = lastMove;
                }
                // após processar a jogada do oponente, caso o turno volte para o líder,
                // a próxima iteração vai detectá-lo; não é necessário mais espera aqui.
            }
        }
    }

    /**
     * Processa um byte[] de move (formato "from|coord") e atualiza o tabuleiro/oponente,
     * escreve /battleship/lastmove com "from|coord|OUTCOME" e atualiza /battleship/turn.
     * Retorna a string lastMove escrita (ou null se nada foi processado).
     */
    private String processMove(byte[] data) throws Exception {
        String msg = new String(data, "UTF-8");
        String[] parts = msg.split("\\|");
        String from = parts[0];
        String coord = parts[1];

        lock.lock();
        try {
            String opponent = findOpponentFor(from);
            if (opponent == null) {
                return null;
            }

            Stat st = zk.exists("/battleship/boards/" + opponent, false);
            byte[] boardData = zk.getData("/battleship/boards/" + opponent, false, st);
            Board oppBoard = Board.deserialize(new String(boardData, "UTF-8"));

            Board.ShotResult result = oppBoard.shoot(coord);
            zk.setData("/battleship/boards/" + opponent, oppBoard.serialize().getBytes("UTF-8"), -1);

            String lastMove = from + "|" + coord + "|" + result.outcome;
            Stat stLast = zk.exists("/battleship/lastmove", false);
            if (stLast == null) zk.create("/battleship/lastmove", lastMove.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            else zk.setData("/battleship/lastmove", lastMove.getBytes("UTF-8"), -1);

            if (result.isWinner) {
                if (zk.exists("/battleship/winner", false) == null)
                    zk.create("/battleship/winner", from.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("Jogador " + from + " venceu! Encerrando referee.");
                return lastMove;
            }

            // Lógica para atualizar o turno para o próximo jogador
            String nextTurn = opponent;
            zk.setData("/battleship/turn", nextTurn.getBytes(), -1);

            return lastMove;
        } finally {
            lock.unlock();
        }
    }

    private void runPlayerLoop() throws Exception {
        findOpponent();
        if (opponentId == null) {
            System.out.println("Não foi possível encontrar o oponente. Encerrando.");
            return;
        }

        System.out.println("Não sou leader. Aguardando turno. Meu id = " + playerId + ", opponent = " + opponentId);

        Stat st = zk.exists("/battleship/turn", true);
        while (st == null) {
            System.out.println("Esperando o líder iniciar o jogo...");
            synchronized (this) {
                wait();
            }
            st = zk.exists("/battleship/turn", true);
        }

        Scanner sc = new Scanner(System.in);
        while (true) {
            // Sempre checar e imprimir qualquer resultado novo que ainda não exibimos
            checkAndPrintLastMove();

            byte[] turnData = zk.getData("/battleship/turn", true, st);
            String currentTurn = new String(turnData, "UTF-8");

            if (!currentTurn.equals(playerId)) {
                System.out.println("Aguardando turno atual: " + currentTurn);
                synchronized (this) {
                    wait();
                }
                continue;
            }

            System.out.println("É o seu turno! Digite a coordenada (ex A5): ");
            String coord = sc.nextLine().trim().toUpperCase();
            if (coord.length() == 0) continue;

            // Antes de produzir, memoriza o último resultado visto
            String prevSeen = lastSeenMove;

            queue.produce(playerId + "|" + coord);
            System.out.println("Move enviado, aguardando resultado...");

            // Espera até que /battleship/lastmove mude (ou seja criado) para algo diferente do prevSeen
            Stat s = zk.exists("/battleship/lastmove", true);
            while (true) {
                // Se houver um lastmove e for novo, checkAndPrintLastMove vai imprimir e atualizar lastSeenMove
                checkAndPrintLastMove();
                if (lastSeenMove != null && !lastSeenMove.equals(prevSeen)) break;

                synchronized (this) {
                    wait();
                }
                s = zk.exists("/battleship/lastmove", true);
            }

            // Verifica se alguém venceu
            Stat sw = zk.exists("/battleship/winner", false);
            if (sw != null) {
                byte[] w = zk.getData("/battleship/winner", false, sw);
                String winner = new String(w, "UTF-8");
                if (winner.equals(playerId)) System.out.println("Você venceu!");
                else System.out.println("Você perdeu. Jogador vencedor: " + winner);
                break;
            }

            // Aguarda explicitamente até que o /battleship/turn deixe de ser o meu
            waitForTurnChange(playerId);
        }
    }

    /**
     * Checa o nó /battleship/lastmove e, se houver um novo valor que ainda não foi exibido,
     * imprime-o e atualiza lastSeenMove. Coloca watch para futuras mudanças.
     */
    private void checkAndPrintLastMove() throws KeeperException, InterruptedException {
        Stat s = zk.exists("/battleship/lastmove", true);
        if (s != null) {
            byte[] data = zk.getData("/battleship/lastmove", true, s);
            String lm = new String(data, java.nio.charset.StandardCharsets.UTF_8);
            if (!lm.equals(lastSeenMove)) {
                System.out.println("Resultado: " + lm);
                lastSeenMove = lm;
            }
        }
    }

    /**
     * Espera até que o valor de /battleship/turn deixe de ser previousTurn.
     * Usa watch e wait/notify (o process() do watcher notifica this).
     */
    private void waitForTurnChange(String previousTurn) throws KeeperException, InterruptedException {
        Stat st = zk.exists("/battleship/turn", true);
        while (true) {
            // Se o nó existir, leia e compare
            if (st != null) {
                byte[] data = zk.getData("/battleship/turn", true, st);
                String cur = new String(data, java.nio.charset.StandardCharsets.UTF_8);
                if (!cur.equals(previousTurn)) return;
            } else {
                // se não existir, espere até que seja criado
                st = zk.exists("/battleship/turn", true);
                if (st == null) {
                    synchronized (this) {
                        wait();
                    }
                    continue;
                } else {
                    continue;
                }
            }
            // aguarda notificação (process)
            synchronized (this) {
                wait();
            }
            // re-loop: o watch foi disparado, voltamos a checar o dado
            st = zk.exists("/battleship/turn", true);
        }
    }

    private void createIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat s = zk.exists(path, false);
        if (s == null) zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void promptPlaceShips() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Posicione 3 navios (exemplo posições: A1 A1 (single), A3 A5 (len3))");
        int ships = 3;
        for (int i=0;i<ships;i++){
            boolean ok=false;
            while (!ok) {
                System.out.printf("Navio %d - digite start e end (ex: A1 A1): ", i+1);
                String line = sc.nextLine().trim();
                String[] parts = line.split("\\s+");
                if (parts.length < 2) { System.out.println("Formato inválido"); continue; }
                ok = board.placeShip(parts[0], parts[1], "ship"+(i+1));
                if (!ok) System.out.println("Não foi possível posicionar, tente outra posição.");
                else System.out.println("Navio posicionado.");
            }
        }
    }

    private void findOpponent() throws KeeperException, InterruptedException {
        for (String child : zk.getChildren("/battleship/players", false)) {
            if (!child.equals(playerId)) {
                opponentId = child;
                return;
            }
        }
        opponentId = null;
    }

    private String findOpponentFor(String player) throws KeeperException, InterruptedException {
        for (String child : zk.getChildren("/battleship/players", false)) {
            if (!child.equals(player)) return child;
        }
        return null;
    }

    // Limpa os nós persistentes e a fila de movimentos para garantir um novo estado de jogo
    private void cleanupZNodes() throws KeeperException, InterruptedException {
        String[] pathsToClean = {
                "/battleship/turn",
                "/battleship/lastmove",
                "/battleship/winner",
                "/battleship/leader"
        };

        System.out.println("Limpando nós de jogos anteriores...");
        for (String path : pathsToClean) {
            if (zk.exists(path, false) != null) {
                try {
                    zk.delete(path, -1);
                    System.out.println("  > Nó persistente removido: " + path);
                } catch (KeeperException.NoNodeException ignored) {
                    // Ignore se o nó já não existir
                }
            }
        }

        // Limpa a fila de movimentos e seus nós filhos
        if (zk.exists("/battleship/moves", false) != null) {
            System.out.println("  > Limpando fila de movimentos em /battleship/moves...");
            try {
                // Exclui todos os nós filhos da fila para garantir que está vazia
                List<String> children = zk.getChildren("/battleship/moves", false);
                for (String child : children) {
                    zk.delete("/battleship/moves/" + child, -1);
                    System.out.println("    - Movimento obsoleto removido: " + child);
                }
            } catch (KeeperException.NoNodeException ignored) {
                // Se a fila não tiver nós filhos, a operação será ignorada
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java game.DistributedBattleshipClient <zookeeper-host:2181>");
            return;
        }
        DistributedBattleshipClient client = new DistributedBattleshipClient(args[0]);
        client.start();
    }
}
