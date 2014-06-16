package org.orthomcl.data;

import org.json.JSONException;
import org.json.JSONObject;
import org.orthomcl.data.layout.Edge;
import org.orthomcl.data.layout.Node;

public class BlastScore extends GenePair implements Edge {

  private final Group group;

  private float evalueMant;
  private int evalueExp;
  private double weight = 0;
  private EdgeType type = EdgeType.Normal;

  public BlastScore(Group group, int queryId, int subjectId) {
    super(queryId, subjectId);
    this.group = group;
  }

  public BlastScore(Group group, JSONObject jsScore) throws JSONException {
    this(group, jsScore.getInt("queryId"), jsScore.getInt("subjectId"));
    String[] evalue = jsScore.getString("evalue").split("E");
    evalueMant = Float.valueOf(evalue[0]);
    evalueExp = Integer.valueOf(evalue[1]);
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

  /**
   * @param evalueExp the evalueExp to set
   */
  public void setEvalueExp(int evalueExp) {
    this.evalueExp = evalueExp;
  }
  
  public void setEvalue(float mant, int exp) {
    this.evalueMant = mant;
    this.evalueExp = exp;
  }

  @Override
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
