package org.orthomcl.data.common.layout.viewer;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Map;

import javax.swing.JPanel;

import org.orthomcl.data.common.layout.core.Gene;
import org.orthomcl.shared.model.layout.ForceEdge;
import org.orthomcl.shared.model.layout.ForceGraph;
import org.orthomcl.shared.model.layout.ForceNode;
import org.orthomcl.shared.model.layout.Graph;
import org.orthomcl.shared.model.layout.LayoutObserver;
import org.orthomcl.shared.model.layout.LayoutUtility;
import org.orthomcl.shared.model.layout.Vector;

public class GroupLayoutPanel extends JPanel implements LayoutObserver, ComponentListener {

  private static final double MIN_WEIGHT = 5;
  private static final double MAX_WEIGHT = 185;

  /**
	 * 
	 */
  private static final long serialVersionUID = 672332572838076301L;

  private static final double MARGIN = 100;
  private static final long INTERVAL = 5;

  private final RenderingHints renderHints;
  private final DecimalFormat format = new DecimalFormat("0.000");

  private BufferedImage back;
  private BufferedImage front;
  private long lastRun;

  private boolean showGeneInfo = false;
  private boolean showScoreInfo = false;

  public GroupLayoutPanel() {
    setSize(new Dimension(10, 10));
    addComponentListener(this);

    renderHints = new RenderingHints(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    renderHints.put(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
    initializeBuffer();
  }

  public void initializeBuffer() {
    int width = getWidth();
    int height = getHeight();
    front = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
    back = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
    lastRun = System.currentTimeMillis();
  }

  public void setShowGeneInfo(boolean showGeneInfo) {
    this.showGeneInfo = showGeneInfo;
  }

  public void setShowScoreInfo(boolean showScoreInfo) {
    this.showScoreInfo = showScoreInfo;
  }

  @Override
  public void step(Graph graph, int iteration, double globalStress) {
    drawNetwork(graph, iteration, globalStress);

    // determine how long it should sleep
    long spent = System.currentTimeMillis() - lastRun;
    long sleep = INTERVAL - spent;
    if (sleep > 5) {
      try {
        Thread.sleep(sleep);
      }
      catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    lastRun = System.currentTimeMillis();
  }

  @Override
  public void finish(Graph graph, int iteration, double globalStress) {
    drawNetwork(graph, iteration, globalStress);
  }

  public void drawNetwork(Graph graph, int iteration, double globalStress) {
    ForceGraph internalGraph = (ForceGraph) graph;
    int width = getWidth();
    int height = getHeight();
    Graphics2D g = (Graphics2D) back.getGraphics();
    // g.setRenderingHints(renderHints);
    g.setColor(Color.WHITE);
    g.fillRect(0, 0, width, height);

    Collection<ForceEdge> edges = internalGraph.getEdges();
    Collection<ForceNode> inNodes = internalGraph.getNodes();
    Map<ForceNode, Vector> nodes = LayoutUtility.scale(inNodes, 10, 20, width - MARGIN, height - 10);
    // draw edges
    for (ForceEdge edge : edges) {
      double preferredLength = edge.getEdge().getPreferredLength();
      g.setColor(getEdgeColor(preferredLength));
      Point2D.Double pa = nodes.get(edge.getNodeA());
      Point2D.Double pb = nodes.get(edge.getNodeB());
      g.draw(new Line2D.Double(pa, pb));
      if (showScoreInfo) {
        int mx = (int) ((pa.x + pb.x) / 2), my = (int) ((pa.y + pb.y) / 2);
        g.drawString(edge.toString(), mx, my);
        // g.drawString("S:" + format.format(stress), mx, my + 10);
      }
    }

    // draw nodes
    for (ForceNode node : inNodes) {
      Point2D.Double p = nodes.get(node);
      g.setColor(Color.GREEN);
      g.fill(new Ellipse2D.Double(p.x - 4, p.y - 4, 7, 7));
      if (showGeneInfo) {
        g.setColor(Color.BLACK);
        g.drawString(((Gene) node.getNode()).getSourceId(), (int) p.x + 3, (int) p.y + 5);
      }
    }

    g.setColor(Color.BLACK);
    g.drawString(
        "#Nodes: " + nodes.size() + "  #Round: " + iteration + " Stress: " + format.format(globalStress), 5,
        10);

    // flip front & back buffer
    BufferedImage temp = front;
    front = back;
    back = temp;

    // revalidate();
    repaint();
  }

  private Color getEdgeColor(double preferredLength) {
    int scale = (int) Math.round(255 * (preferredLength - MIN_WEIGHT) / (MAX_WEIGHT - MIN_WEIGHT));
    if (scale < 0)
      scale = 0;
    else if (scale > 255)
      scale = 255;
    return new Color(255 - scale, 0, scale);
  }

  @Override
  public void paint(Graphics g) {
    super.paint(g);
    g.setColor(Color.WHITE);
    g.fillRect(0, 0, WIDTH, HEIGHT);
    g.drawImage(front, 0, 0, null);
  }

  @Override
  public void componentResized(ComponentEvent e) {
    initializeBuffer();
  }

  @Override
  public void componentMoved(ComponentEvent e) {}

  @Override
  public void componentShown(ComponentEvent e) {}

  @Override
  public void componentHidden(ComponentEvent e) {}

}
