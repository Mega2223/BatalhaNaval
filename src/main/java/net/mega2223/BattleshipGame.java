package net.mega2223;

public class BattleshipGame {
    byte[][] map = new byte[10][10];

    public static final byte WATER = 0;
    public static final byte BOAT = 1;
    public static final byte RUINS = 2;
    public static final byte NOT_RUINS = 3;

    public BattleshipGame(){

    }

    public void print(){
        StringBuilder b = new StringBuilder();
        for (int y = 0; y < map.length; y++) {
            b.append("[ ");
            for (int x = 0; x < map[y].length; x++) {
                switch (map[y][x]){
                    case WATER:
                        b.append(" -");
                        break;
                    case BOAT:
                        b.append(" A");
                        break;
                    case RUINS:
                        b.append(" X");
                        break;
                    case NOT_RUINS:
                        b.append(" o");
                        break;
                }
            }
            b.append(" ]\n");
        }
        System.out.print(b);
    }

    public boolean placeBoat(int x, int y){
        if(y >= map.length || x >= map[0].length){
            return false;
        }
        map[y][x] = BOAT;
        return true;
    }

    public boolean placeBoat(int bX, int bY, int eX, int eY){
        if(bY >= map.length || bX >= map[0].length || eY >= map.length || eX >= map[0].length){
            return false;
        }

        for (int y = bY; y < eY; y++) {
            for (int x = bX; x < eX; x++) {
                if(map[y][x] != WATER){
                    return false;
                }
            }
        }

        for (int y = bY; y < eY; y++) {
            for (int x = bX; x < eX; x++) {
                map[y][x] = BOAT;
            }
        }
        return true;
    }


}
