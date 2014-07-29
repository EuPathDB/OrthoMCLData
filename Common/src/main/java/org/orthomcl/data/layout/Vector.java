package org.orthomcl.data.layout;

import java.awt.geom.Point2D.Double;
import java.text.DecimalFormat;

public class Vector extends Double {

  /**
	 * 
	 */
  private static final long serialVersionUID = 7027484553610116182L;

  private final DecimalFormat format = new DecimalFormat("0.000");

  private double strength = -1;

  public Vector() {
    this(0, 0);
  }

  public Vector(Vector v) {
    this(v.x, v.y);
  }

  public Vector(double x, double y) {
    super(x, y);
  }

  public Vector add(Vector v) {
    x += v.x;
    y += v.y;
    strength = -1; // reset length
    return this;
  }

  public Vector subtract(Vector v) {
    x -= v.x;
    y -= v.y;
    strength = -1; // reset length
    return this;
  }

  public Vector scale(double scale) {
    x *= scale;
    y *= scale;
    strength = -1; // reset length
    return this;
  }

  public double getStrength() {
    if (strength < 0)
      strength = x * x + y * y;
    return strength;
  }

  @Override
  public String toString() {
    return "(" + format.format(x) + ", " + format.format(y) + ")";
  }

}
