package org.orthomcl.data.layout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * This layout requires that very edge must have a positive weight.
 */
public class SpringLayout implements Layout {

  private static final Logger LOG = Logger.getLogger(SpringLayout.class);

  public static final double SCALE = 100;
  public static final double NON_DEGE_SCALE = SCALE * 1.01;

  private final Graph graph;
  private final ForceGraph forceGraph;

  private final Random random;

  private double maxWeight = -Double.MAX_VALUE;
  private double minWeight = Double.MAX_VALUE;
  private double minMoves = 0.01;
  private long maxIterations = 20000;
  private boolean canceled = false;
  private boolean stopped = false;

  public SpringLayout(Graph graph) throws GraphicsException {
    this(graph, new Random());
  }

  public SpringLayout(Graph graph, Random random) throws GraphicsException {
    this.graph = graph;
    this.forceGraph = new ForceGraph(graph);
    this.random = random;

    for (Edge edge : forceGraph.getEdges()) {
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
    LOG.debug("Initializing force-directed graph...");

    int iteration = 0;
    canceled = false;
    stopped = false;
    initialize();
    double globalStress = computeGlobalStress();
    LOG.debug("Initial: global stress=" + globalStress);
    observer.step(forceGraph, iteration, globalStress);

    while (iteration < maxIterations) {
      if (stopped || canceled)
        break;

      // move the graph, if break if the graph cannot be moved any further.
      if (!move())
        break;

      iteration++;

      // notify the observer of the initial state;
      if (observer != null) {
        globalStress = computeGlobalStress();
        observer.step(forceGraph, iteration, globalStress);
      }
    }
    stopped = true;
    // notify the observer of the final state
    if (observer != null) {
      globalStress = computeGlobalStress();
      observer.finish(forceGraph, iteration, globalStress);
    }
  }

  /**
   * @return the minimal allowed distance
   */
  private void initialize() {
    // compute size
    double range = maxWeight * (Math.ceil(Math.sqrt(forceGraph.getNodeCount())) - 1);
    for (ForceNode node : forceGraph.getNodes()) {
      node.getPoint().setLocation(random.nextDouble() * range, random.nextDouble() * range);
    }
  }

  private double computeGlobalStress() {
    List<ForceNode> nodes = new ArrayList<>(forceGraph.getNodes());
    double globalStress = 0;
    for (int i = 0; i < nodes.size() - 1; i++) {
      ForceNode nodeA = nodes.get(i);
      for (int j = i + 1; j < nodes.size(); j++) {
        ForceNode nodeB = nodes.get(j);

        // compute force relative to nodeA
        ForceEdge edge = nodeA.getEdge(nodeB.getId());

        // compute the distance between two nodes
        double dx = nodeB.getPoint().x - nodeA.getPoint().x;
        double dy = nodeB.getPoint().y - nodeA.getPoint().y;

        // skip unlinked distant nodes
        if (edge == null && (Math.abs(dx) > maxWeight || Math.abs(dy) > maxWeight))
          continue;

        // compute stress
        double weight = (edge != null) ? edge.getWeight() : maxWeight;
        double dist = Math.sqrt(dx * dx + dy * dy);
        globalStress += Math.abs(dist - weight);
      }
    }
    return globalStress;
  }

  private Vector computeForce(ForceNode nodeA, ForceNode nodeB) {
    ForceEdge edge = nodeA.getEdge(nodeB.getId());

    // compute the distance between two nodes
    double dx = nodeB.getPoint().x - nodeA.getPoint().x;
    double dy = nodeB.getPoint().y - nodeA.getPoint().y;

    // skip unlinked distant nodes
    if (edge == null && (Math.abs(dx) > maxWeight || Math.abs(dy) > maxWeight))
      return null;

    if (dx == 0 && dy == 0) {
      dx = (random.nextBoolean() ? 1 : -1) * minMoves;
      dy = (random.nextBoolean() ? 1 : -1) * minMoves;
    }
    double dist = Math.sqrt(dx * dx + dy * dy);

    // if there is no edge between the nodes, no upper boundary
    if (edge == null && dist > maxWeight)
      return null;

    double weight = (edge != null) ? edge.getWeight() : maxWeight;

    double factor = (dist - weight) / Math.max(1, dist);
    Vector force = new Vector(factor * dx, factor * dy);
    return force;
  }

  /**
   * Move the graph for one iteration
   * 
   * @return return true is the graph can still be moved; false if the graph cannot be moved any further.
   */
  private boolean move() {
    Collection<ForceNode> nodes = forceGraph.getNodes();
    double maxMove = 0;
    for (ForceNode currentNode : nodes) {
      // compute the overall force of the current node;
      Vector overallForce = new Vector();
      int forceCount = 0;
      for (ForceNode node : nodes) {
        if (node == currentNode)
          continue;

        // compute the force of the current node
        Vector force = computeForce(currentNode, node);
        if (force != null) {
          overallForce.add(force);
          forceCount++;
        }
      }

      // move the current node in the direction of the current force; Only move an average distance of over
      // all affected forces.
      overallForce.scale(1D / forceCount);
      currentNode.getPoint().add(overallForce);

      double strength = overallForce.getStrength();
      if (maxMove < strength)
        maxMove = strength;
    }
    return (maxMove >= minMoves);
  }
}
