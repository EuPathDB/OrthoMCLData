package org.orthomcl.data.layout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * This layout requires that very edge must have a positive weight.
 */
public class SpringLayout implements Layout {

  private static final Logger LOG = Logger.getLogger(SpringLayout.class);

  private final ForceGraph graph;

  private final Random random;

  private double minMoves = 0.01;
  private long maxIterations = 20000;
  private boolean canceled = false;
  private boolean stopped = false;

  public SpringLayout(Graph graph) throws GraphicsException {
    this(graph, new Random());
  }

  public SpringLayout(Graph graph, Random random) throws GraphicsException {
    this.graph = new ForceGraph(graph);
    this.random = random;
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
  @Override
  public void setMinMoves(double minMoves) {
    this.minMoves = minMoves;
  }

  /**
   * @return the maxIterations
   */
  @Override
  public long getMaxIterations() {
    return maxIterations;
  }

  /**
   * @param maxIterations
   *          the maxIterations to set
   */
  @Override
  public void setMaxIterations(long maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * @return the network
   */
  @Override
  public Graph getGraph() {
    return graph;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  /**
   * the canceled to set
   */
  @Override
  public void cancel() {
    this.canceled = true;
  }

  @Override
  public void process(LayoutObserver observer) {
    LOG.debug("Initializing force-directed graph for " + graph.getGraph() + "...");

    int iteration = 0;
    canceled = false;
    stopped = false;
    initialize();
    if (observer != null) {
      double globalStress = computeGlobalStress();
      LOG.debug("Initial: global stress=" + globalStress);
      observer.step(graph, iteration, globalStress);
    }

    while (iteration < maxIterations) {
      if (stopped || canceled)
        break;

      // move the graph, if break if the graph cannot be moved any further.
      if (!move())
        break;

      iteration++;

      // notify the observer of the initial state;
      if (observer != null) {
        double globalStress = computeGlobalStress();
        observer.step(graph, iteration, globalStress);
      }
    }
    stopped = true;
    // notify the observer of the final state
    if (observer != null) {
      double globalStress = computeGlobalStress();
      observer.finish(graph, iteration, globalStress);
    }
  }

  /**
   * @return the minimal allowed distance
   */
  private void initialize() {
    Collection<ForceNode> nodes = graph.getNodes();
    if (nodes.size() <= 2) {
      Iterator<ForceNode> it = nodes.iterator();
      // only need to set one node, the other use the default (0, 0)
      ForceNode node = it.next();
      node.getPoint().setLocation(0, graph.getMaxPreferredLength());
    }
    else {
      // compute size
      double range = graph.getMaxPreferredLength() * (Math.ceil(Math.sqrt(nodes.size())) - 1);
      for (ForceNode node : nodes) {
        node.getPoint().setLocation(random.nextDouble() * range, random.nextDouble() * range);
      }
    }
  }

  private double computeGlobalStress() {
    List<ForceNode> nodes = new ArrayList<>(graph.getNodes());
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
        double maxLength = graph.getMaxPreferredLength();
        if (edge == null && (Math.abs(dx) > maxLength || Math.abs(dy) > maxLength))
          continue;

        // compute stress
        double preferredLength = (edge != null) ? edge.getPreferredLength() : maxLength;
        double dist = Math.sqrt(dx * dx + dy * dy);
        globalStress += Math.abs(dist - preferredLength);
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
    double maxLength = graph.getMaxPreferredLength();
    if (edge == null && (Math.abs(dx) > maxLength || Math.abs(dy) > maxLength))
      return null;

    if (dx == 0 && dy == 0) {
      dx = (random.nextBoolean() ? 1 : -1) * minMoves;
      dy = (random.nextBoolean() ? 1 : -1) * minMoves;
    }
    double dist = Math.sqrt(dx * dx + dy * dy);

    // if there is no edge between the nodes, no upper boundary
    if (edge == null && dist > maxLength)
      return null;

    double preferredLength = (edge != null) ? edge.getPreferredLength() : maxLength;

    double factor = (dist - preferredLength) / Math.max(1, dist);
    Vector force = new Vector(factor * dx, factor * dy);
    return force;
  }

  /**
   * Move the graph for one iteration
   * 
   * @return return true is the graph can still be moved; false if the graph cannot be moved any further.
   */
  private boolean move() {
    Collection<ForceNode> nodes = graph.getNodes();
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
      if (forceCount > 0) {
        overallForce.scale(1D / forceCount);
        currentNode.getPoint().add(overallForce);
      }

      double strength = overallForce.getStrength();
      if (maxMove < strength)
        maxMove = strength;
    }
    return (maxMove >= minMoves);
  }
}
