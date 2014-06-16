package org.orthomcl.data;

import org.json.JSONException;
import org.json.JSONObject;
import org.orthomcl.data.layout.Node;
import org.orthomcl.data.layout.Vector;

public class Gene implements Node {

  private final int id;
  private final String sourceId;
  private final Vector point;

  private int organismId;
  private int length;
  private String description;

  public Gene(int id, String sourceId) {
    this.id = id;
    this.sourceId = sourceId;
    this.point = new Vector();
  }

  public Gene(JSONObject jsGene) throws JSONException {
    this.id = jsGene.getInt("id");
    this.sourceId = jsGene.getString("sourceId");
    this.organismId = jsGene.getInt("organismId");
    this.length = jsGene.getInt("length");
    this.description = jsGene.getString("description");
    double x = jsGene.getDouble("x");
    double y = jsGene.getDouble("y");
    this.point = new Vector(x, y);
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  @Override
  public Vector getPoint() {
    return point;
  }

  public String getSourceId() {
    return sourceId;
  }

  public int getOrganismId() {
    return organismId;
  }

  public void setOrganismId(int organismId) {
    this.organismId = organismId;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public JSONObject toJSON() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("id", id);
    json.put("sourceId", sourceId);
    json.put("organismId", organismId);
    json.put("length", length);
    json.put("description", description);
    json.put("x", point.x);
    json.put("y", point.y);
    return json;
  }
}
