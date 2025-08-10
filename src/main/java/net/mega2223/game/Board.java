package net.mega2223.game;

import java.util.*;

public class Board {
    private static final char FOG = '~';
    private static final char SHIP = 'S';
    private static final char HIT = 'X';
    private static final char MISS = 'O';

    private final int size = 10;
    private final char[][] board;
    private final Map<String, Integer> shipHealth = new HashMap<>();
    private int totalShipCells = 0;

    public Board() {
        board = new char[size][size];
        for (int i=0;i<size;i++) Arrays.fill(board[i], FOG);
    }

    public synchronized boolean placeShip(String start, String end, String name) {
        Point p1 = toPoint(start);
        Point p2 = toPoint(end);
        if (p1 == null || p2 == null) return false;
        if (p1.row != p2.row && p1.col != p2.col) return false;
        int len = (p1.row == p2.row) ? Math.abs(p1.col - p2.col)+1 : Math.abs(p1.row - p2.row)+1;
        int r = Math.min(p1.row, p2.row), c = Math.min(p1.col, p2.col);
        for (int i=0;i<len;i++){
            int rr = r + (p1.row==p2.row?0:i);
            int cc = c + (p1.row==p2.row?i:0);
            if (board[rr][cc] != FOG) return false;
        }
        for (int i=0;i<len;i++){
            int rr = r + (p1.row==p2.row?0:i);
            int cc = c + (p1.row==p2.row?i:0);
            board[rr][cc] = SHIP;
        }
        shipHealth.put(name, len);
        totalShipCells += len;
        return true;
    }

    public static class ShotResult {
        public final String outcome;
        public final boolean isWinner;
        private ShotResult(String outcome, boolean isWinner){
            this.outcome = outcome; this.isWinner = isWinner;
        }
        public static ShotResult hit(){ return new ShotResult("HIT", false); }
        public static ShotResult miss(){ return new ShotResult("MISS", false); }
        public static ShotResult already(){ return new ShotResult("ALREADY", false); }
        public static ShotResult winner(){ return new ShotResult("WIN", true); }
        public static ShotResult invalid(){ return new ShotResult("INVALID", false); }
    }

    public synchronized ShotResult shoot(String coord) {
        Point p = toPoint(coord);
        if (p == null) return ShotResult.invalid();
        char cur = board[p.row][p.col];
        if (cur == SHIP) {
            board[p.row][p.col] = HIT;
            totalShipCells--;
            if (totalShipCells == 0) return ShotResult.winner();
            return ShotResult.hit();
        } else if (cur == FOG) {
            board[p.row][p.col] = MISS;
            return ShotResult.miss();
        } else {
            return ShotResult.already();
        }
    }

    public synchronized String serialize() {
        StringBuilder sb = new StringBuilder();
        for (int i=0;i<size;i++){
            for (int j=0;j<size;j++){
                sb.append(board[i][j]);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public static Board deserialize(String s) {
        Board b = new Board();
        String[] lines = s.split("\n");
        for (int i=0;i<Math.min(lines.length,10); i++){
            String l = lines[i];
            for (int j=0;j<Math.min(l.length(),10); j++){
                b.board[i][j] = l.charAt(j);
            }
        }
        return b;
    }

    private Point toPoint(String pos) {
        pos = pos.trim().toUpperCase();
        if (pos.length() < 2) return null;
        char r = pos.charAt(0);
        int row = r - 'A';
        try {
            int col = Integer.parseInt(pos.substring(1)) - 1;
            if (row < 0 || row >=10 || col < 0 || col >=10) return null;
            return new Point(row, col);
        } catch (NumberFormatException e) { return null; }
    }

    private static class Point { int row, col; Point(int r, int c){row=r;col=c;} }
}
