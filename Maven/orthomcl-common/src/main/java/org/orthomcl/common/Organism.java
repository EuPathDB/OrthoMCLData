/**
 * 
 */
package org.orthomcl.common;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author jerric
 * 
 */
public class Organism {

    private final int id;
    private String name;
    private String abbreviation;

    public Organism(final int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public int getId() {
        return id;
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("abbrev", abbreviation);
        return json;
    }
}
