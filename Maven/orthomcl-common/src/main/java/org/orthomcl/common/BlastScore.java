package org.orthomcl.common;

import net.lliira.common.graphics.layout.Edge;
import net.lliira.common.graphics.layout.Node;

import org.json.JSONException;
import org.json.JSONObject;

public class BlastScore extends GenePair implements Edge {

    private final Group group;

    private float evalueMant;
    private int evalueExp;
    private double weight = 0;
    private EdgeType type = EdgeType.Normal;

    public BlastScore(Group group, final int queryId, final int subjectId) {
        super(queryId, subjectId);
        this.group = group;
    }

    public float getEvalueMant() {
        return evalueMant;
    }

    public void setEvalueMant(float evalueMant) {
        this.evalueMant = evalueMant;
    }

    public int getEvalueExp() {
        return evalueExp;
    }

    public void setEvalueExp(int evalueExp) {
        this.evalueExp = evalueExp;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public EdgeType getType() {
        return type;
    }

    public void setType(EdgeType type) {
        this.type = type;
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("queryId", getQueryId());
        json.put("subjectId", getSubjectId());
        json.put("evalue", evalueMant + "E" + evalueExp);
        json.put("type", type.ordinal());
        return json;
    }

    @Override
    public Node getNodeA() {
        return group.getGenes().get(queryId);
    }

    @Override
    public Node getNodeB() {
        return group.getGenes().get(subjectId);
    }
}
