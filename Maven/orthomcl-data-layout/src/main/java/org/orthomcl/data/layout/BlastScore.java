package org.orthomcl.data.layout;

import net.lliira.common.graphics.layout.Edge;

public class BlastScore implements Edge {

    private final int idA;
    private final int idB;

    private double evalueMant;
    private double evalueExp;
    
    
    private Sequence sequenceA;
    private Sequence sequenceB;
    private double weight;

    public BlastScore(int idA, int idB) {
        this.idA = idA;
        this.idB = idB;
    }

    public double getWeight() {
        return weight;
    }

    public double getEvalueMant() {
        return evalueMant;
    }

    public void setEvalueMant(double evalueMant) {
        if (evalueMant < 0.001) evalueMant = 1;
        this.evalueMant = evalueMant;
    }

    public double getEvalueExp() {
        return evalueExp;
    }

    public void setEvalueExp(double evalueExp) {
        this.evalueExp = evalueExp;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Sequence getNodeA() {
        return sequenceA;
    }

    public Sequence getNodeB() {
        return sequenceB;
    }

    public int getIdA() {
        return idA;
    }

    public int getIdB() {
        return idB;
    }

    public void setSequenceA(Sequence sequenceA) {
        this.sequenceA = sequenceA;
    }

    public void setSequenceB(Sequence sequenceB) {
        this.sequenceB = sequenceB;
    }
}
