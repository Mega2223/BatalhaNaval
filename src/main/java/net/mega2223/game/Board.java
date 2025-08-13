package net.mega2223.game;

import java.util.*;

public class Board {
    private static final char FOG  = '~';
    private static final char SHIP = 'S';
    private static final char HIT  = 'X';
    private static final char MISS = 'O';
    private static final int SIZE = 10;

    private final char[][] board = new char[SIZE][SIZE];
    private final Map<Point, Ship> shipAt = new HashMap<>();
    private final List<Ship> fleet = new ArrayList<>();
    private int totalShipCells = 0;

    public Board() {
        for (int i = 0; i < SIZE; i++) Arrays.fill(board[i], FOG);
        for (ShipType t : ShipType.values()) fleet.add(new Ship(t));
    }

    /** Posiciona um navio com as mesmas regras do org.game: tamanho exato, em linha, sem encostar (nem diagonal). */
    public synchronized boolean placeShip(String start, String end, Ship ship) {
        Point p1 = toPoint(start), p2 = toPoint(end);
        if (p1 == null || p2 == null) return false;
        if (p1.row != p2.row && p1.col != p2.col) return false;

        int len = (p1.row == p2.row) ? Math.abs(p1.col - p2.col) + 1 : Math.abs(p1.row - p2.row) + 1;
        if (len != ship.getLength()) return false;

        Point startPoint;
        boolean horizontal = (p1.row == p2.row);
        if (horizontal) startPoint = (p1.col <= p2.col) ? p1 : p2;
        else            startPoint = (p1.row <= p2.row) ? p1 : p2;

        // valida colisão e proximidade (8 vizinhos)
        int r = startPoint.row, c = startPoint.col;
        for (int i = 0; i < len; i++) {
            if (board[r][c] == SHIP) return false;
            for (int dr = -1; dr <= 1; dr++) {
                for (int dc = -1; dc <= 1; dc++) {
                    if (dr == 0 && dc == 0) continue;
                    int rr = r + dr, cc = c + dc;
                    if (rr >= 0 && rr < SIZE && cc >= 0 && cc < SIZE) {
                        if (board[rr][cc] == SHIP) return false;
                    }
                }
            }
            if (horizontal) c++; else r++;
        }

        // grava
        r = startPoint.row; c = startPoint.col;
        for (int i = 0; i < len; i++) {
            board[r][c] = SHIP;
            shipAt.put(new Point(r, c), ship);
            if (horizontal) c++; else r++;
        }
        totalShipCells += len;
        return true;
    }

    public static class ShotResult {
        public final String outcome; // "HIT" | "MISS" | "ALREADY" | "WIN"
        public final boolean isWinner;
        private ShotResult(String outcome, boolean isWinner){ this.outcome = outcome; this.isWinner = isWinner; }
        public static ShotResult hit(){ return new ShotResult("HIT", false); }
        public static ShotResult miss(){ return new ShotResult("MISS", false); }
        public static ShotResult already(){ return new ShotResult("ALREADY", false); }
        public static ShotResult winner(){ return new ShotResult("WIN", true); }
        public static ShotResult invalid(){ return new ShotResult("MISS", false); } // trata inválido como erro de tiro
    }

    public synchronized ShotResult shoot(String coord) {
        Point p = toPoint(coord);
        if (p == null) return ShotResult.invalid();
        char cur = board[p.row][p.col];
        if (cur == SHIP) {
            board[p.row][p.col] = HIT;
            totalShipCells--;
            Ship s = shipAt.get(p);
            if (s != null) s.hit();
            if (totalShipCells == 0) return ShotResult.winner();
            return ShotResult.hit();
        } else if (cur == FOG) {
            board[p.row][p.col] = MISS;
            return ShotResult.miss();
        } else {
            return ShotResult.already();
        }
    }

    public synchronized String prettyPrint() {
        return prettyPrintInternal(false);
    }
    public synchronized String prettyPrintFogOfWar() {
        return prettyPrintInternal(true);
    }
    private String prettyPrintInternal(boolean fog) {
        StringBuilder sb = new StringBuilder();
        sb.append("   ");
        for (int j = 1; j <= SIZE; j++) sb.append(String.format("%2d ", j));
        sb.append("\n");
        for (int i = 0; i < SIZE; i++) {
            sb.append((char)('A' + i)).append("  ");
            for (int j = 0; j < SIZE; j++) {
                char v = board[i][j];
                if (fog && v == SHIP) v = FOG;
                sb.append(v).append("  ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public synchronized String serialize() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) sb.append(board[i][j]);
            sb.append('\n');
        }
        return sb.toString();
    }
    public static Board deserialize(String s) {
        Board b = new Board();
        // limpa frota reconstruída (vamos só contar células para totalShipCells)
        b.fleet.clear();
        b.shipAt.clear();

        String[] lines = s.split("\n");
        int ships = 0;
        for (int i = 0; i < Math.min(SIZE, lines.length); i++) {
            String l = lines[i];
            for (int j = 0; j < Math.min(SIZE, l.length()); j++) {
                char ch = l.charAt(j);
                b.board[i][j] = ch;
                if (ch == SHIP) ships++;
            }
        }
        b.totalShipCells = ships;
        return b;
    }

    private static Point toPoint(String pos) {
        if (pos == null) return null;
        pos = pos.trim().toUpperCase(Locale.ROOT);
        if (pos.length() < 2 || pos.length() > 3) return null;
        int row = pos.charAt(0) - 'A';
        int col;
        try {
            col = Integer.parseInt(pos.substring(1)) - 1;
        } catch (NumberFormatException e) { return null; }
        if (row < 0 || row >= SIZE || col < 0 || col >= SIZE) return null;
        return new Point(row, col);
    }

    private static final class Point {
        final int row, col;
        Point(int r, int c){ this.row = r; this.col = c; }
        @Override public boolean equals(Object o){
            if(!(o instanceof Point)) return false;
            Point p2 = (Point)o;
            return p2.row==row && p2.col==col;
        }
        @Override public int hashCode(){ return Objects.hash(row, col); }
    }
}
