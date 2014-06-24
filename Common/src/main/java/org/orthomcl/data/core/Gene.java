package org.orthomcl.data.core;

import java.text.DecimalFormat;

import org.json.JSONException;
import org.json.JSONObject;
import org.orthomcl.data.layout.Node;
import org.orthomcl.data.layout.Vector;

public class Gene implements Node {
  
  private static final DecimalFormat FORMAT = new DecimalFormat("0.00");

  private final String sourceId;
  private final Vector point;
  
  private int taxonId;

  public Gene(String sourceId) {
    this.sourceId = sourceId;
    this.point = new Vector();
  }

  public Gene(JSONObject jsGene) throws JSONException {
    this.sourceId = jsGene.getString("sourceId");
    double x = jsGene.getDouble("x");
    double y = jsGene.getDouble("y");
    this.point = new Vector(x, y);
  }

  /**
   * @return the taxonId
   */
  public int getTaxonId() {
    return taxonId;
  }

  /**
   * @param taxonId the taxonId to set
   */
  public void setTaxonId(int taxonId) {
    this.taxonId = taxonId;
  }

  @Override
  public Vector getPoint() {
    return point;
  }

  public String getSourceId() {
    return sourceId;
  }

  public JSONObject toJSON() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("id", sourceId);
    json.put("x", FORMAT.format(point.x));
    json.put("y", FORMAT.format(point.y));
    return json;
  }
}
