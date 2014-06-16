package org.orthomcl.data.layout;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class LayoutUtility {

  public static <N extends Node> Map<N, Vector> scale(Collection<N> nodes, double bminx, double bminy,
      double bmaxx, double bmaxy) {
    double width = bmaxx - bminx;
    double height = bmaxy - bminy;

    double _minx = Double.MAX_VALUE, _maxx = -Double.MAX_VALUE;
    double _miny = Double.MAX_VALUE, _maxy = -Double.MAX_VALUE;
    for (Node node : nodes) {
      Vector point = node.getPoint();
      if (point.getX() < _minx)
        _minx = point.getX();
      if (point.getX() > _maxx)
        _maxx = point.getX();
      if (point.getY() < _miny)
        _miny = point.getY();
      if (point.getY() > _maxy)
        _maxy = point.getY();
    }

    // scale nodes
    double ratiox = width / (_maxx - _minx);
    double ratioy = height / (_maxy - _miny);
    double ratio = Math.min(ratiox, ratioy);

    Map<N, Vector> locations = new LinkedHashMap<>();
    for (N node : nodes) {
      Vector point = node.getPoint();
      double x = (point.getX() - _minx) * ratio + bminx;
      double y = (point.getY() - _miny) * ratio + bminy;
      locations.put(node, new Vector(x, y));
    }
    return locations;
  }
}
