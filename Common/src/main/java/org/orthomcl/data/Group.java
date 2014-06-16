package org.orthomcl.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.orthomcl.data.layout.Edge;
import org.orthomcl.data.layout.Graph;
import org.orthomcl.data.layout.Node;

/**
 * represent a graph of an OrthoMCL group.
 * 
 */
public class Group implements Graph {

  private final int id;
  private final String name;
  private final Map<Integer, Gene> genes;
  private final Map<GenePair, BlastScore> scores;
  
  public Group(int id, String name) {
    this.id = id;
    this.name = name;
    this.genes = new HashMap<>();
    this.scores = new HashMap<>();
  }

  public Group(JSONObject jsGroup) throws JSONException {
    this.id = jsGroup.getInt("id");
    this.name = jsGroup.getString("name");

    genes = new LinkedHashMap<>();
    JSONArray jsGenes = jsGroup.getJSONArray("genes");
    for (int i = 0; i < jsGenes.length(); i++) {
      JSONObject jsGene = jsGenes.getJSONObject(i);
      Gene gene = new Gene(jsGene);
      genes.put(gene.getId(), gene);
    }

    scores = new LinkedHashMap<GenePair, BlastScore>();
    JSONArray jsScores = jsGroup.getJSONArray("scores");
    for (int i = 0; i < jsScores.length(); i++) {
      JSONObject jsScore = jsScores.getJSONObject(i);
      BlastScore score = new BlastScore(this, jsScore);
      scores.put(score, score);
    }
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  public String getName() {
    return name;
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
  
  public void addGene(Gene gene) {
    genes.put(gene.getId(), gene);
  }
  
  public void addBlastScore(BlastScore blastScore) {
    scores.put(blastScore, blastScore);
  }
  
  public String getLayout() {
    
  }
  
}
