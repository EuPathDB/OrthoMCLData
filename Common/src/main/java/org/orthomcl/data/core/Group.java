package org.orthomcl.data.core;

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

  public static final double MAX_PREFERRED_LENGTH = 230;

  private final int id;
  private final String name;
  private final Map<String, Gene> genes;
  private final Map<GenePair, BlastScore> scores;

  private String layout;

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
      genes.put(gene.getSourceId(), gene);
    }

    scores = new LinkedHashMap<GenePair, BlastScore>();
    JSONArray jsScores = jsGroup.getJSONArray("scores");
    for (int i = 0; i < jsScores.length(); i++) {
      JSONObject jsScore = jsScores.getJSONObject(i);
      BlastScore score = new BlastScore(jsScore);
      score.setGroup(this);
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

  @Override
  public double getMaxPreferredLength() {
    return MAX_PREFERRED_LENGTH;
  }

  /**
   * @return the layout
   */
  public String getLayout() {
    return layout;
  }

  /**
   * @param layout
   *          the layout to set
   */
  public void setLayout(String layout) {
    this.layout = layout;
  }

  public Map<String, Gene> getGenes() {
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
    genes.put(gene.getSourceId(), gene);
  }

  public void addBlastScore(BlastScore score) {
    // convert evalue into preferred length.
    double length = getMaxPreferredLength() + (Math.log10(score.getEvalueMant()) + score.getEvalueExp());
    if (scores.containsKey(score)) { // duplicate score, compute the average log(evalue) as weight
      BlastScore oldScore = scores.get(score);
      oldScore.setPreferredLength((oldScore.getPreferredLength() + length) / 2);
      oldScore.setEvalue2(score.getEvalueMant(), score.getEvalueExp());
    }
    else { // new score, compute log(evalue) as weight
      score.setPreferredLength(length);
      scores.put(score, score);
    }
  }
  
  @Override
  public String toString() {
    return name;
  }
}
