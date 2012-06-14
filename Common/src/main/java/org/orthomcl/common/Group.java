package org.orthomcl.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.lliira.common.graphics.layout.Edge;
import net.lliira.common.graphics.layout.Graph;
import net.lliira.common.graphics.layout.Node;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Hello world!
 * 
 */
public class Group implements Graph {
    private final int id;
    private final Map<Integer, Gene> genes;
    private final Map<GenePair, BlastScore> scores;

    private String name;

    public Group(final int id) {
        this.id = id;
        this.genes = new HashMap<Integer, Gene>();
        this.scores = new HashMap<GenePair, BlastScore>();
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<Integer, Gene> getGenes() {
        return genes;
    }

    public Map<GenePair, BlastScore> getScores() {
        return scores;
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);

        JSONArray jsGenes = new JSONArray();
        for (Gene gene : genes.values()) {
            jsGenes.put(gene.toJSON());
        }
        json.put("genes", jsGenes);

        JSONArray jsScores = new JSONArray();
        for (BlastScore score : scores.values()) {
            jsScores.put(score.toJSON());
        }
        json.put("scores", jsScores);

        return json;
    }

    @Override
    public Collection<? extends Edge> getEdges() {
        return scores.values();
    }

    @Override
    public Collection<? extends Node> getNodes() {
        return genes.values();
    }
}
