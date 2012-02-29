package org.orthomcl.data.layout;

public class Pair {

    private final int idA;
    private final int idB;

    public Pair(int idA, int idB) {
        this.idA = idA;
        this.idB = idB;
    }

    @Override
    public int hashCode() {
        return idA ^ idB;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Pair) {
            Pair pair = (Pair) obj;
            return (idA == pair.idA && idB == pair.idB)
                    || (idA == pair.idB && idB == pair.idA);
        } else return false;
    }
}
