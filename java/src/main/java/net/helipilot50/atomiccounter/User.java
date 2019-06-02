package net.helipilot50.atomiccounter;

public class User {
    String id;
    String name;
    Long visitCount;

    public User(String id, String name, Long visitCount){
        this.name = name;
        this.id = id;
        this.visitCount = visitCount;
    }

    @Override
    public String toString() {
        return String.format("{id: %s, name: %s, visitCount: %d}", this.id, this.name, this.visitCount);
    }

}