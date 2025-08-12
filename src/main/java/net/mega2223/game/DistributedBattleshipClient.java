package net.mega2223.game;

import net.mega2223.zk.ZooKeeperConnector;
import net.mega2223.zk.Queue;
import net.mega2223.zk.Barrier;
import net.mega2223.zk.Leader;
import net.mega2223.zk.Lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class DistributedBattleshipClient implements Watcher {
    private final String hostPort;
    private ZooKeeperConnector connector;
    private Queue queue;
    private Barrier barrier;
    private Leader leader;
    private Lock lock;

    private String playerId;
    private String opponentId;
    private Board board;
    private ZooKeeper zk;

    // Guarda o último "lastmove" já exibido neste cliente, para evitar ler resultados antigos.
    private volatile String lastSeenMove = null;

    public DistributedBattleshipClient(String hostPort) throws IOException {
        this.hostPort = hostPort;
        this.connector = new ZooKeeperConnector();
        this.playerId = "p-" + UUID.randomUUID().toString().substring(0, 6);
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

        queue = new Queue(hostPort, "/battleship/moves");
        barrier = new Barrier(hostPort, "/battleship/ready", 2);
        leader = new Leader(hostPort, "/battleship/election", "/battleship/leader", playerId);
        lock = new Lock(hostPort, "/battleship/lock");
        System.out.println("Primitivas Iniciadas");

        System.out.println("Começando o Jogo!!!");
        promptPlaceShips();

        Stat s = zk.exists("/battleship/boards/" + playerId, false);
        byte[] myBoard = board.serialize().getBytes(StandardCharsets.UTF_8);
        if (s == null) {
            zk.create("/battleship/boards/" + playerId, myBoard, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData("/battleship/boards/" + playerId, myBoard, -1);
        }

        System.out.println("Esperando outro jogador (barrier)...");
        barrier.enter();
        System.out.println("Ambos prontos! Prosseguindo para eleição de leader...");

        // garanta opponentId para ambos (inclusive o líder)
        findOpponent();

        boolean iamLeader = leader.elect();
        if (iamLeader) {
            System.out.println("Fui eleito leader (referee). Vou consumir fila de moves.");
            runReferee();
        } else {
            System.out.println("Não sou leader. Aguardando turno. Meu id = " + playerId + ", opponent = " + opponentId);
            runPlayerLoop();
        }
    }

    private void runReferee() throws Exception {
        // líder começa sempre
        Stat s = zk.exists("/battleship/turn", true);
        if (s == null) {
            String firstPlayer = playerId; // líder inicia
            System.out.println("Definindo o primeiro jogador (líder): " + firstPlayer);
            zk.create("/battleship/turn", firstPlayer.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        System.out.println("Referee inicializado. Aguardando turno ou mensagens na fila...");
        Scanner sc = new Scanner(System.in);

        while (true) {
            // (1) imprime resultados novos (para todos) e arma watch
            checkAndPrintLastMove();

            // (2) se já há vencedor, encerra líder também
            Stat sw = zk.exists("/battleship/winner", false);
            if (sw != null) {
                byte[] w = zk.getData("/battleship/winner", false, sw);
                System.out.println("Jogo finalizado. Vencedor: " + new String(w, StandardCharsets.UTF_8));
                connector.close();
                return;
            }

            byte[] turnData = zk.getData("/battleship/turn", true, s);
            String currentTurn = new String(turnData, StandardCharsets.UTF_8);

            if (currentTurn.equals(playerId)) {
                System.out.println("É o seu turno (líder)! Digite a coordenada (ex A5): ");
                String coord = sc.nextLine().trim().toUpperCase();
                if (coord.length() == 0) continue;
                queue.produce(playerId + "|" + coord);
                System.out.println("Move enviado, aguardando confirmação...");

                // Espera uma jogada da fila e processa
                byte[] data = queue.consume();
                String lastMove = processMove(data);

                if (lastMove != null) {
                    System.out.println("Resultado: " + lastMove);
                    lastSeenMove = lastMove;
                    printBoardsSnapshot(); // líder imprime imediatamente

                    if (lastMove.endsWith("|WIN")) {
                        System.out.println("Jogo finalizado. Encerrando líder.");
                        connector.close();
                        return;
                    }
                }

                // Aguarda o turno mudar
                waitForTurnChange(playerId);
            } else {
                System.out.println("Aguardando turno de " + currentTurn + "...");

                byte[] data = queue.consume();
                String lastMove = processMove(data);

                if (lastMove != null) {
                    System.out.println("Resultado: " + lastMove);
                    lastSeenMove = lastMove;
                    printBoardsSnapshot(); // líder imprime imediatamente

                    if (lastMove.endsWith("|WIN")) {
                        System.out.println("Jogo finalizado. Encerrando líder.");
                        connector.close();
                        return;
                    }
                }
            }
        }
    }

    /**
     * Processa um byte[] de move (formato "from|coord") e atualiza o tabuleiro/oponente,
     * escreve /battleship/lastmove com "from|coord|OUTCOME" e atualiza /battleship/turn.
     * Retorna a string lastMove escrita (ou null se nada foi processado).
     */
    private String processMove(byte[] data) throws Exception {
        String msg = new String(data, StandardCharsets.UTF_8);
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
            Board oppBoard = Board.deserialize(new String(boardData, StandardCharsets.UTF_8));

            Board.ShotResult result = oppBoard.shoot(coord);
            zk.setData("/battleship/boards/" + opponent, oppBoard.serialize().getBytes(StandardCharsets.UTF_8), -1);

            String lastMove = from + "|" + coord + "|" + result.outcome;
            Stat stLast = zk.exists("/battleship/lastmove", false);
            byte[] lmBytes = lastMove.getBytes(StandardCharsets.UTF_8);
            if (stLast == null) {
                zk.create("/battleship/lastmove", lmBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData("/battleship/lastmove", lmBytes, -1);
            }

            if (result.isWinner) {
                if (zk.exists("/battleship/winner", false) == null) {
                    zk.create("/battleship/winner", from.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                System.out.println("Jogador " + from + " venceu! Encerrando referee.");
                // não dá return aqui; quem consome decide encerrar ao ver |WIN
            } else {
                // Próximo turno = oponente
                zk.setData("/battleship/turn", opponent.getBytes(StandardCharsets.UTF_8), -1);
            }
            return lastMove;
        } finally {
            lock.unlock();
        }
    }

    private void runPlayerLoop() throws Exception {
        if (opponentId == null) findOpponent();
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

            // também checa vencedor para encerrar imediatamente
            Stat sw = zk.exists("/battleship/winner", false);
            if (sw != null) {
                byte[] w = zk.getData("/battleship/winner", false, sw);
                String winner = new String(w, StandardCharsets.UTF_8);
                if (winner.equals(playerId)) System.out.println("Você venceu!");
                else System.out.println("Você perdeu. Jogador vencedor: " + winner);
                connector.close();
                break;
            }

            byte[] turnData = zk.getData("/battleship/turn", true, st);
            String currentTurn = new String(turnData, StandardCharsets.UTF_8);

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

            // Espera até que /battleship/lastmove mude para algo diferente do prevSeen
            Stat s2 = zk.exists("/battleship/lastmove", true);
            while (true) {
                checkAndPrintLastMove();
                if (lastSeenMove != null && !lastSeenMove.equals(prevSeen)) break;

                synchronized (this) {
                    wait();
                }
                s2 = zk.exists("/battleship/lastmove", true);
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
            String lm = new String(data, StandardCharsets.UTF_8);
            if (!lm.equals(lastSeenMove)) {
                System.out.println("Resultado: " + lm);
                lastSeenMove = lm;

                // imprime tabuleiros após cada jogada aplicada pelo árbitro
                printBoardsSnapshot();
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
            if (st != null) {
                byte[] data = zk.getData("/battleship/turn", true, st);
                String cur = new String(data, StandardCharsets.UTF_8);
                if (!cur.equals(previousTurn)) return;
            } else {
                st = zk.exists("/battleship/turn", true);
                if (st == null) {
                    synchronized (this) { wait(); }
                    continue;
                } else {
                    continue;
                }
            }
            synchronized (this) { wait(); }
            st = zk.exists("/battleship/turn", true);
        }
    }

    private void createIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat s = zk.exists(path, false);
        if (s == null) zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void promptPlaceShips() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Posicione sua frota completa. Ex: A1 A5 (horizontal 5), B2 B2 (single).");
        System.out.println(board.prettyPrint());

        // Se você deixou só 3 tipos no ShipType, este loop já usa exatamente esses 3
        for (ShipType t : ShipType.values()) {
            boolean ok = false;
            while (!ok) {
                System.out.printf("%s (%d células) - digite start e end (ex: A1 A%d): ",
                        t.name, t.length, t.length);
                String line = sc.nextLine().trim().toUpperCase();
                String[] parts = line.split("\\s+");
                if (parts.length < 2) {
                    System.out.println("Formato inválido");
                    continue;
                }
                ok = board.placeShip(parts[0], parts[1], new Ship(t));
                if (!ok) {
                    System.out.println("Posição inválida (tamanho/linha/colisão/vizinhança). Tente outra.");
                }
                System.out.println(board.prettyPrint());
            }
        }
    }

    // ---------- helpers de impressão ----------
    private Board fetchBoardFor(String pid) throws KeeperException, InterruptedException {
        Stat st = zk.exists("/battleship/boards/" + pid, false);
        if (st == null) return null;
        byte[] data = zk.getData("/battleship/boards/" + pid, false, st);
        return Board.deserialize(new String(data, StandardCharsets.UTF_8));
    }

    private void printBoardsSnapshot() {
        try {
            if (opponentId == null) {
                findOpponent(); // garante opponentId
            }
            Board my = fetchBoardFor(playerId);
            Board opp = (opponentId != null) ? fetchBoardFor(opponentId) : null;

            System.out.println("\n=== Estado após a jogada ===");
            if (opp != null) {
                System.out.println("Tabuleiro do oponente (fog of war):");
                System.out.println(opp.prettyPrintFogOfWar());
            }
            if (my != null) {
                System.out.println("Seu tabuleiro:");
                System.out.println(my.prettyPrint());
            }
            System.out.println("============================\n");
        } catch (Exception e) {
            System.out.println("(Não foi possível imprimir os tabuleiros agora: " + e.getMessage() + ")");
        }
    }

    // ---------- descoberta de oponente ----------
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

    // Limpa nós persistentes e a fila de movimentos para garantir um novo estado de jogo
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
                } catch (KeeperException.NoNodeException ignored) { }
            }
        }

        // Limpa a fila de movimentos e seus nós filhos
        if (zk.exists("/battleship/moves", false) != null) {
            System.out.println("  > Limpando fila de movimentos em /battleship/moves...");
            try {
                List<String> children = zk.getChildren("/battleship/moves", false);
                for (String child : children) {
                    zk.delete("/battleship/moves/" + child, -1);
                    System.out.println("    - Movimento obsoleto removido: " + child);
                }
            } catch (KeeperException.NoNodeException ignored) { }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java net.mega2223.game.DistributedBattleshipClient <zookeeper-host:2181>");
            return;
        }
        DistributedBattleshipClient client = new DistributedBattleshipClient(args[0]);
        client.start();
    }
}
