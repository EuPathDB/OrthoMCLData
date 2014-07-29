package org.orthomcl.data.layout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ForceGraph implements Graph {

  private final Graph graph;
  private final Map<Integer, ForceNode> nodes;
  private final List<ForceEdge> edges;

  public ForceGraph(Graph graph) throws GraphicsException {
    this.graph = graph;
    this.nodes = new HashMap<Integer, ForceNode>();
    this.edges = new ArrayList<ForceEdge>();

    int nodeId = 1;
    Map<Node, Integer> nodeMap = new HashMap<Node, Integer>();
    for (Node node : graph.getNodes()) {
      ForceNode inNode = new ForceNode(node, nodeId);
      nodes.put(nodeId, inNode);
      nodeMap.put(node, nodeId);
      nodeId++;
    }
    for (Edge edge : graph.getEdges()) {
      ForceNode nodeA = nodes.get(nodeMap.get(edge.getNodeA()));
      ForceNode nodeB = nodes.get(nodeMap.get(edge.getNodeB()));
      ForceEdge inEdge = new ForceEdge(edge, nodeA, nodeB);
      inEdge.setWeight(edge.getPreferredLength());
      edges.add(inEdge);
      nodeA.addNeighbour(nodeB.getId(), inEdge);
      nodeB.addNeighbour(nodeA.getId(), inEdge);
    }
  }
  
  @Override
  public double getMaxPreferredLength() {
    return graph.getMaxPreferredLength();
  }

  public Graph getGraph() {
    return graph;
  }

  @Override
  public Collection<ForceNode> getNodes() {
    return nodes.values();
  }

  @Override
  public Collection<ForceEdge> getEdges() {
    return edges;
  }

  public int getNodeCount() {
    return nodes.size();
  }

}
