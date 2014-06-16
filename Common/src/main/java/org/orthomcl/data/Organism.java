/**
 * 
 */
package org.orthomcl.data;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author jerric
 * 
 */
public class Organism {

  private final int id;
  private String abbreviation;
  private String name;

  public Organism(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @param abbreviation
   *          the abbreviation to set
   */
  public void setAbbreviation(String abbreviation) {
    this.abbreviation = abbreviation;
  }

  public String getAbbreviation() {
    return abbreviation;
  }

  public JSONObject toJSON() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("id", id);
    json.put("name", name);
    json.put("abbrev", abbreviation);
    return json;
  }
}
