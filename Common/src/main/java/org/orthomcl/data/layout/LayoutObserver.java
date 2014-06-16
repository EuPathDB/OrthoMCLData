package org.orthomcl.data.layout;

public interface LayoutObserver {

  void step(Graph graph, int iteration, double moves, Vector energy);

  void finish(Graph graph, int iteration, double moves, Vector energy);

}
