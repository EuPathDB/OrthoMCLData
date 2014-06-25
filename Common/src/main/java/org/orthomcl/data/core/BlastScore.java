package org.orthomcl.data.core;

import java.text.DecimalFormat;

import org.json.JSONException;
import org.json.JSONObject;
import org.orthomcl.data.layout.Edge;
import org.orthomcl.data.layout.Node;

public class BlastScore extends GenePair implements Edge {

  private static final DecimalFormat FORMAT = new DecimalFormat("0.00");

  private static final String EVALUE_DIVIDER = "/";

  private Group group;
  private float evalueMant;
  private int evalueExp;
  private float evalueMant2;
  private int evalueExp2;
  private double preferredLength;
  private EdgeType type = EdgeType.Normal;

  public BlastScore(String queryId, String subjectId) {
    super(queryId, subjectId);
  }

  public BlastScore(JSONObject jsScore) throws JSONException {
    this(jsScore.getString("Q"), jsScore.getString("S"));
    String[] evalues = jsScore.getString("E").split(EVALUE_DIVIDER);
    String[] evalue = evalues[0].split("[eE]");
    evalueMant = Float.valueOf(evalue[0]);
    evalueExp = Integer.valueOf(evalue[1]);
    if (evalues.length == 2) {
      String[] evalue2 = evalues[1].split("[eE]");
      evalueMant2 = Float.valueOf(evalue2[0]);
      evalueExp2 = Integer.valueOf(evalue2[1]);
    }
  }

  /**
   * @return the group
   */
  public Group getGroup() {
    return group;
  }

  /**
   * @param group
   *          the group to set
   */
  public void setGroup(Group group) {
    this.group = group;
  }

  public float getEvalueMant() {
    return evalueMant;
  }

  public void setEvalueMant(float evalueMant) {
    if (evalueMant == 0)
      evalueMant = 1;
    this.evalueMant = evalueMant;
  }

  public int getEvalueExp() {
    return evalueExp;
  }

  /**
   * @param evalueExp
   *          the evalueExp to set
   */
  public void setEvalueExp(int evalueExp) {
    this.evalueExp = evalueExp;
  }

  public void setEvalue(float mant, int exp) {
    if (mant == 0)
      mant = 1;
    this.evalueMant = mant;
    this.evalueExp = exp;
  }

  public void setEvalue2(float mant, int exp) {
    if (mant == 0)
      mant = 1;
    this.evalueMant2 = mant;
    this.evalueExp2 = exp;
  }

  public String getEvalue() {
    String evalue = evalueMant + "E" + evalueExp;
    if (evalueMant != evalueMant2 || evalueExp != evalueExp2)
      evalue += EVALUE_DIVIDER + evalueMant2 + "E" + evalueExp2;
    return evalue;
  }

  @Override
  public double getPreferredLength() {
    return preferredLength;
  }

  public void setPreferredLength(double preferredLength) {
    this.preferredLength = preferredLength;
  }

  public EdgeType getType() {
    return type;
  }

  public void setType(EdgeType type) {
    this.type = type;
  }

  public JSONObject toJSON() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("Q", getQueryId());
    json.put("S", getSubjectId());
    json.put("E", getEvalue());
    json.put("T", type.getCode());
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

  @Override
  public String toString() {
    return type.getCode() + " E=" + getEvalue() + " PL=" + FORMAT.format(preferredLength);
  }
}
