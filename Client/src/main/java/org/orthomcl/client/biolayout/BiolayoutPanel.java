package org.orthomcl.client.biolayout;

import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.util.Map;

import javax.swing.JPanel;

import net.lliira.common.graphics.layout.LayoutUtility;
import net.lliira.common.graphics.layout.Node;
import net.lliira.common.graphics.layout.Vector;

import org.orthomcl.common.BlastScore;
import org.orthomcl.common.EdgeType;
import org.orthomcl.common.Gene;
import org.orthomcl.common.Group;

public class BiolayoutPanel extends JPanel {

    /**
     * 
     */
    private static final long serialVersionUID = -8414257304395252274L;

    private static final Color COLOR_BACKGROUND = Color.WHITE;
    private static final Color COLOR_STATUS = new Color(0, 0, 200);
    private static final Color COLOR_ORTHOLOG = Color.RED;
    private static final Color COLOR_COORTHOLOG = Color.YELLOW;
    private static final Color COLOR_INPARALOG = Color.GREEN;
    private static final Color COLOR_NORMAL = Color.GRAY;
    private static final Color COLOR_NODE_BORDER = Color.BLACK;
    private static final Color COLOR_NODE = Color.BLUE;

    private static final Font FONT_STATUS = new Font("Dialog", Font.BOLD, 13);
    private static final int MARGIN = 20;
    private static final int NODE_SIZE = 9;

    private String status;
    private Group group;
    private Map<Node, Vector> locations;

    public BiolayoutPanel() {
        setDoubleBuffered(true); // turn on double buffering
    }

    public void setStatus(String status) {
        this.status = status;
        repaint();
    }

    public void setGroup(Group group) {
        this.group = group;
        if (group != null) {
            int width = getWidth();
            int height = getHeight();
            locations = LayoutUtility.scale(group.getNodes(), MARGIN, MARGIN,
                    width - MARGIN, height - MARGIN);
        }
        repaint();
    }

    @Override
    protected void paintComponent(Graphics g) {
        int width = getWidth();
        int height = getHeight();

        // reset the background
        g.setColor(COLOR_BACKGROUND);
        g.fillRect(0, 0, width, height);

        // draw graph
        Graphics2D g2d = (Graphics2D) g;
        if (group != null) drawGroup(g2d);
        if (status != null) drawStatus(g2d);
    }

    private void drawGroup(Graphics2D g) {
        // draw edges
        for (BlastScore score : group.getScores().values()) {
            EdgeType type = score.getType();
            if (type == EdgeType.Ortholog) g.setColor(COLOR_ORTHOLOG);
            else if (type == EdgeType.Coortholog) g.setColor(COLOR_COORTHOLOG);
            else if (type == EdgeType.Inparalog) g.setColor(COLOR_INPARALOG);
            else g.setColor(COLOR_NORMAL);
            Vector from = locations.get(score.getNodeA());
            Vector to = locations.get(score.getNodeB());
            g.drawLine((int) from.x, (int) from.y, (int) to.x, (int) to.y);
        }

        // draw nodes
        for (Gene gene : group.getGenes().values()) {
            Vector p = gene.getPoint();
            g.setColor(COLOR_NODE_BORDER);
            g.drawOval((int) p.x - 5, (int) p.y - 5, NODE_SIZE, NODE_SIZE);
            g.setColor(COLOR_NODE);
            g.fillOval((int) p.x - 4, (int) p.y - 4, NODE_SIZE - 2,
                    NODE_SIZE - 2);
        }
    }

    private void drawStatus(Graphics2D g) {
        g.setColor(COLOR_STATUS);
        g.setFont(FONT_STATUS);
        FontMetrics metrics = g.getFontMetrics(FONT_STATUS);
        int height = metrics.getHeight();
        int width = metrics.stringWidth(status);
        int x = (getWidth() - width) / 2;
        int y = (getHeight() - height) / 2;
        g.drawString(status, x, y);
    }
}
