package org.orthomcl.data.layout;

import net.lliira.common.graphics.layout.Node;

public class Sequence implements Node {

    private final int groupId;
    private final int id;
    private double x;
    private double y;

    public Sequence(int groupId, int id) {
        this.groupId = groupId;
        this.id = id;
    }

    public int getGroupId() {
        return groupId;
    }

    public int getId() {
        return id;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

}
