package org.orthomcl.data.layout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * This layout requires that very edge must have a positive weight.
 */
public class SpringLayout implements Layout {

  private static final double SCALE = 100;
  private static final double NON_DEGE_SCALE = SCALE * 1.01;

  private final Graph graph;
  private final ForceGraph internalGraph;

  private final Random random;

  private double maxWeight = -Double.MAX_VALUE;
  private double minWeight = Double.MAX_VALUE;
  private double segment = 1.85;
  private double minMoves = 1E-4;
  private long maxIterations = 10000;
  private boolean canceled = false;
  private boolean stopped = false;

  public SpringLayout(Graph graph) throws GraphicsException {
    this(new Random(), graph);
  }

  public SpringLayout(Random random, Graph graph) throws GraphicsException {
    this.graph = graph;
    this.internalGraph = new ForceGraph(graph);
    this.random = random;

    for (Edge edge : internalGraph.getEdges()) {
      double weight = edge.getWeight();
      if (weight <= 0)
        throw new GraphicsException("Weight must be positive: " + weight);
      if (maxWeight < weight)
        maxWeight = weight;
      if (minWeight > weight)
        minWeight = weight;
    }
  }

  /**
   * @return the maxWeight
   */
  double getMaxWeight() {
    return maxWeight;
  }

  /**
   * @param maxWeight
   *          the maxWeight to set
   */
  void setMaxWeight(double maxWeight) {
    this.maxWeight = maxWeight;
  }

  /**
   * @return the minMoves
   */
  public double getMinMoves() {
    return minMoves;
  }

  /**
   * @param minMoves
   *          the minMoves to set
   */
  public void setMinMoves(double minMoves) {
    this.minMoves = minMoves;
  }

  /**
   * @return the maxIterations
   */
  public long getMaxIterations() {
    return maxIterations;
  }

  /**
   * @param maxIterations
   *          the maxIterations to set
   */
  public void setMaxIterations(long maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * @return the segment
   */
  public double getSegment() {
    return segment;
  }

  /**
   * @param segment
   *          the segment to set
   */
  public void setSegment(double segment) {
    this.segment = segment;
  }

  /**
   * @return the network
   */
  public Graph getGraph() {
    return graph;
  }

  public boolean isStopped() {
    return stopped;
  }

  /**
   * the canceled to set
   */
  public void cancel() {
    this.canceled = true;
  }

  public void process(LayoutObserver observer) {
    initialize();
    int iteration = 0;
    canceled = false;
    stopped = false;
    double moves = 0;
    Vector energy = new Vector();
    double damping = Math.max(2, internalGraph.getNodeCount() / segment);
    Collection<ForceNode> nodes = internalGraph.getNodes();
    Collection<ForceEdge> edges = internalGraph.getEdges();
    while (iteration < maxIterations) {
      if (canceled)
        break;

      // notify the observer of the initial state;
      if (observer != null)
        observer.step(internalGraph, iteration, moves, energy);

      iteration++;

      moves = 0; // reset moves for the new iteration.

      // compute force
      computeForces(nodes, edges);
      // apply speed to each node
      for (ForceNode node : nodes) {
        Vector force = node.getForce();
        force.scale(1 / damping);
        node.getPoint().add(force);
        moves += Math.sqrt(force.x * force.x + force.y * force.y);
      }
      // compute energy
      energy.setLocation(0, 0);
      for (ForceEdge edge : edges) {
        energy.x += edge.getStress();
      }

      if (moves < minMoves)
        break;
    }
    stopped = true;
    // notify the observer of the final state
    if (observer != null)
      observer.finish(internalGraph, iteration, moves, energy);
  }

  /**
   * @return the minimal allowed distance
   */
  private void initialize() {
    double ratio = SCALE / maxWeight;
    for (ForceEdge edge : internalGraph.getEdges()) {
      edge.setWeight(edge.getWeight() * ratio);
    }
    // position the nodes around a circle
    double step = 2 * Math.PI / internalGraph.getNodeCount();
    double angle = 0;
    List<ForceNode> nodes = new ArrayList<>(internalGraph.getNodes());
    Collections.sort(nodes, new Comparator<ForceNode>() {
      @Override
      public int compare(ForceNode node1, ForceNode node2) {
        return (int) Math.signum(node1.getAverageWeight() - node2.getAverageWeight());
      }
    });
    for (ForceNode node : nodes) {
      double x = Math.sin(angle) * SCALE / 2;
      double y = Math.cos(angle) * SCALE / 2;
      node.getPoint().setLocation(x, y);
      angle += step;
    }
  }

  private void computeForces(Collection<ForceNode> nodes, Collection<ForceEdge> edges) {
    // reset force to (0, 0)
    for (ForceNode node : nodes) {
      node.getForce().setLocation(0, 0);
      node.setStress(0);
    }
    // reset crossing count to 0
    for (ForceEdge edge : edges) {
      edge.setCrossings(0);
    }

    // compute spring forces
    computeSpringForces(nodes.toArray(new ForceNode[0]));
  }

  private void computeSpringForces(ForceNode[] nodes) {
    for (int i = 0; i < nodes.length - 1; i++) {
      ForceNode nodeI = nodes[i];
      for (int j = i + 1; j < nodes.length; j++) {
        // compute the speed
        ForceNode nodeJ = nodes[j];
        double dx = nodeJ.getPoint().x - nodeI.getPoint().x;
        double dy = nodeJ.getPoint().y - nodeI.getPoint().y;
        if (dx == 0 && dy == 0) {
          dx = (random.nextBoolean() ? 1 : -1) * minMoves;
          dy = (random.nextBoolean() ? 1 : -1) * minMoves;
        }
        double dist = Math.sqrt(dx * dx + dy * dy);

        // if there is no edge between the nodes, no upper boundary
        ForceEdge edge = nodeI.getEdge(nodeJ.getId());
        if (edge == null && dist > NON_DEGE_SCALE)
          continue;
        double weight = (edge != null) ? edge.getWeight() : NON_DEGE_SCALE;

        double factor = (dist - weight) / dist;
        Vector force = new Vector(factor * dx, factor * dy);

        // apply speed to both nodes
        nodeI.getForce().add(force);
        nodeJ.getForce().subtract(force);

        // cache length for later use
        if (edge != null)
          edge.setLength(dist);
      }
    }

  }
}
