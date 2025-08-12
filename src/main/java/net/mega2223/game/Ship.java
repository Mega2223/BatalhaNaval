package net.mega2223.game;

class Ship {
    final ShipType type;
    int health;

    Ship(ShipType type) {
        this.type = type;
        this.health = type.length;
    }

    String getName() { return type.name; }
    int getLength() { return type.length; }
    void hit() { health--; }
    boolean sunk() { return health <= 0; }
}
