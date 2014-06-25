package org.orthomcl.data.layout;

import java.awt.geom.Line2D;
import java.text.DecimalFormat;

public class ForceEdge implements Edge {

  private static final DecimalFormat FORMAT = new DecimalFormat("0.00");

  private final Edge edge;
  private final ForceNode nodeA;
  private final ForceNode nodeB;

  private double preferredLength;
  private int crossings;

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
    this.preferredLength = edge.getPreferredLength();
  }

  public Edge getEdge() {
    return edge;
  }

  public double getPreferredLength() {
    return preferredLength;
  }

  public void setWeight(double weight) {
    this.preferredLength = weight;
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
    double dx = nodeB.getPoint().x - nodeA.getPoint().x;
    double dy = nodeB.getPoint().y - nodeA.getPoint().y;
    return Math.sqrt(dx * dx + dy * dy);
  }

  public double getStress() {
    return Math.abs(getLength() - preferredLength) / preferredLength;
  }

  public Vector getMedian() {
    Vector pA = nodeA.getPoint(), pB = nodeB.getPoint();
    return new Vector((pA.x + pB.x) / 2, (pA.y + pB.y) / 2);
  }

  public Line2D getLine() {
    return new Line2D.Double(nodeA.getPoint(), nodeB.getPoint());
  }
  
  @Override
  public String toString() {
    return edge.toString() + " L=" + FORMAT.format(getLength());
  }
}
