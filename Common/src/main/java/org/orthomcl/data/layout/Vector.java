package org.orthomcl.data.layout;

import java.awt.geom.Point2D.Double;

public class Vector extends Double {

  /**
	 * 
	 */
  private static final long serialVersionUID = 7027484553610116182L;

  public Vector() {
    super();
  }

  public Vector(double x, double y) {
    super(x, y);
  }

  public Vector(Vector v) {
    super(v.x, v.y);
  }

  public void add(Vector v) {
    x += v.x;
    y += v.y;
  }

  public void subtract(Vector v) {
    x -= v.x;
    y -= v.y;
  }

  public void scale(double scale) {
    x *= scale;
    y *= scale;
  }
}
