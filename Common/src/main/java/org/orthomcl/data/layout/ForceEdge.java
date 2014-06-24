package org.orthomcl.data.layout;

import java.awt.geom.Line2D;

public class ForceEdge implements Edge {

  private final Edge edge;
  private final ForceNode nodeA;
  private final ForceNode nodeB;

  private double weight;
  private int crossings;
  private double length;

  public ForceEdge(Edge edge, ForceNode nodeA, ForceNode nodeB) throws GraphicsException {
    if (edge == null)
      throw new GraphicsException("edge cannot be null");
    if (nodeA == null)
      throw new GraphicsException("node A cannot be null");
    if (nodeB == null)
      throw new GraphicsException("node B cannot be null");
    this.edge = edge;
    this.nodeA = nodeA;
    this.nodeB = nodeB;
    this.weight = edge.getWeight();
  }

  public Edge getEdge() {
    return edge;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  @Override
  public ForceNode getNodeA() {
    return nodeA;
  }

  @Override
  public ForceNode getNodeB() {
    return nodeB;
  }

  /**
   * @return the crossings
   */
  public int getCrossings() {
    return crossings;
  }

  /**
   * @param crossings
   *          the crossings to set
   */
  public void setCrossings(int crossings) {
    this.crossings = crossings;
  }

  public void incrementCrossing() {
    this.crossings++;
  }

  /**
   * @return the length
   */
  public double getLength() {
    return length;
  }

  /**
   * @param length
   *          the length to set
   */
  public void setLength(double length) {
    this.length = length;
  }

  public double getStress() {
    return Math.abs(length - weight) / weight;
  }

  public Vector getMedian() {
    Vector pA = nodeA.getPoint(), pB = nodeB.getPoint();
    return new Vector((pA.x + pB.x) / 2, (pA.y + pB.y) / 2);
  }

  public Line2D getLine() {
    return new Line2D.Double(nodeA.getPoint(), nodeB.getPoint());
  }
}
