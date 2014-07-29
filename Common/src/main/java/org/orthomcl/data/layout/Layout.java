package org.orthomcl.data.layout;

public interface Layout {

  long getMaxIterations();

  void setMaxIterations(long maxIterations);

  void setMinMoves(double minMoves);

  void process(LayoutObserver observer);

  void cancel();

  boolean isStopped();

  Graph getGraph();
}
