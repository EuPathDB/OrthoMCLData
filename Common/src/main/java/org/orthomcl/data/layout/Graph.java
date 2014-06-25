package org.orthomcl.data.layout;

import java.util.Collection;

public interface Graph {
  
  double getMaxPreferredLength();

  Collection<? extends Node> getNodes();

  Collection<? extends Edge> getEdges();
}
