package org.orthomcl.common;

import net.lliira.common.graphics.layout.Node;
import net.lliira.common.graphics.layout.Vector;

import org.json.JSONException;
import org.json.JSONObject;

public class Gene implements Node {

    private final int id;
    private final Vector point;
    
    private String sourceId;
    private int organismId;
    private int length;
    private String description;

    public Gene(final int id) {
        this.id = id;
        this.point = new Vector();
    }

    @Override
    public Vector getPoint() {
        return point;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
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

    public int getId() {
        return id;
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
        return json;
    }
}
