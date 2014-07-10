package org.orthomcl.data.layout;

class EdgeKey {

  private Node nodeA;
  private Node nodeB;

  EdgeKey(Edge edge) {
    this(edge.getNodeA(), edge.getNodeB());
  }

  EdgeKey(Node nodeA, Node nodeB) {
    this.nodeA = nodeA;
    this.nodeB = nodeB;
  }

  @Override
  public int hashCode() {
    return nodeA.hashCode() ^ nodeB.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof EdgeKey) {
      EdgeKey key = (EdgeKey) obj;
      return (key.nodeA.equals(nodeA) && key.nodeB.equals(nodeB)) ||
          (key.nodeA.equals(nodeB) && key.nodeB.equals(nodeA));
    }
    else
      return false;
  }
}
