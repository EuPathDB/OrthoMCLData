import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;
import org.apidb.orthomcl.load.plugin.OrthoMCLException;
import org.apidb.orthomcl.load.plugin.biolayout.EdgeType;
import org.apidb.orthomcl.load.plugin.biolayout.Group;
import org.apidb.orthomcl.load.plugin.biolayout.Node;

/**
 * 
 */

/**
 * @author xingao This class has to be put under default package, in order to
 *         access BioLayout classes.
 */
public class BiolayoutProcessor {

    private static class HiddenFrame extends LayoutFrame {

        /**
         * 
         */
        private static final long serialVersionUID = -4131734598815916578L;

        /*
         * (non-Javadoc)
         * 
         * @see LayoutFrame#incrementProgress(int)
         */
        public void incrementProgress(int i_iteration) {}

        /*
         * (non-Javadoc)
         * 
         * @see LayoutFrame#prepareProgressBar(int, java.lang.String)
         */
        public void prepareProgressBar(int i_max, String i_title) {}

        /*
         * (non-Javadoc)
         * 
         * @see LayoutFrame#resetProgressBar()
         */
        public void resetProgressBar() {}

        /*
         * (non-Javadoc)
         * 
         * @see LayoutFrame#startProgressBar()
         */
        public void startProgressBar() {}

        /*
         * (non-Javadoc)
         * 
         * @see java.awt.Window#setVisible(boolean)
         */
        public void setVisible(boolean b) {
            super.setVisible(false);
        }

    }

    /**
     * 
     */
    private static final long serialVersionUID = -418177851347137182L;

    private static final Logger logger = Logger.getLogger(BiolayoutProcessor.class);

    private static final int WIDTH = 800;
    private static final int HEIGHT = 600;
    private static final int RADIUS = 5;

    // private static final Logger logger =
    // Logger.getLogger(BiolayoutProcessor.class);

    private LayoutClasses classes;
    private NetworkContainer network;
    private Graph graph;

    public BiolayoutProcessor() throws SecurityException, NoSuchFieldException,
            IllegalArgumentException, IllegalAccessException {
        LayoutFrame frame = new HiddenFrame();
        classes = frame.getClasses();
        graph = frame.getGraph();
        graph.setPreferredSize(new Dimension(WIDTH, HEIGHT));
        graph.setSize(WIDTH, HEIGHT);

        Field field = LayoutFrame.class.getDeclaredField("m_nc");
        field.setAccessible(true);
        network = (NetworkContainer) field.get(frame);

        GlobalEnv.DIRECTIONAL = false;
        GlobalEnv.BACKGROUND_COLOR = new Color(222, 222, 222);
        GlobalEnv.BACKGROUND_ANTIALIASING = true;
    }

    public void saveData(Group group, OutputStream svgStream,
            OutputStream imgStream) throws IOException, OrthoMCLException {
        // reset the network
        network.clean();
        GlobalEnv.WEIGHTED = true;

        // parse the data into network
        parseData(group);

        network.optimize();
        graph.updateGraphics(network);
        scaleGraph(group);
        logger.debug("biolayout image created");

        // save the layout file
        saveSVG(group, svgStream);
        svgStream.flush();

        // save layout image
        saveImage(imgStream);
        imgStream.flush();
    }

    private void parseData(Group group) throws OrthoMCLException {
        // create nodes and edges
        for (org.apidb.orthomcl.load.plugin.biolayout.Edge edge : group.edges.values()) {
            String queryId = Integer.toString(edge.queryId).intern();
            String subjectId = Integer.toString(edge.subjectId).intern();
            network.addNetworkConnection(queryId, subjectId, edge.weight);
        }

        // assign sequence-taxons
        for (Node node : group.nodes.values()) {
            String nodeName = Integer.toString(node.sequenceId).intern();
            Vertex vertex = (Vertex) network.m_hashMap.get(nodeName);

            if (vertex != null) {
                // update class ref in the vertex, and create class if needed
                classes.updateClass(vertex, node.taxonId, node.abbreviation);
            } else {
                System.err.println("Sequence: " + node.sourceId
                        + " doesn't have any edge to "
                        + " other nodes in the same group");
            }
        }
    }

    private void saveSVG(Group group, OutputStream svgStream) {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(svgStream));

        outputEdges(writer, group, EdgeType.Ortholog);
        writer.flush();
        outputEdges(writer, group, EdgeType.Coortholog);
        writer.flush();
        outputEdges(writer, group, EdgeType.Inparalog);
        writer.flush();
        outputEdges(writer, group, EdgeType.Normal);
        writer.flush();
        outputNodes(writer, group);
        writer.flush();
    }

    private void outputNodes(PrintWriter writer, Group group) {
        Map<Integer, List<Integer>> taxons = group.getNodeIdsByTaxons();
        for (int taxonId : taxons.keySet()) {
            List<Integer> taxon = taxons.get(taxonId);
            Node node = group.nodes.get(taxon.get(0));
            writer.print("<g id=\"" + taxonId + "\" class=\"taxon\"");
            writer.print("  abbrev=\"" + node.abbreviation + "\"");
            writer.println("  name=\"" + node.organism + "\">");
            for (int seqId : taxon) {
                node = group.nodes.get(seqId);
                writer.print("<circle id=\"" + seqId + "\" class=\"gene\"");
                writer.print("  cx=\"" + node.x + "\" cy=\"" + node.y + "\"");
                writer.print("  r=\"5\" name=\"" + node.sourceId + "\"");
                writer.println("  description=\"" + node.description + "\" />");
            }
            writer.println("</g>");
        }
    }

    private void outputEdges(PrintWriter writer, Group group, EdgeType type) {
        writer.println("<g id=\"" + type + "\">");
        for (org.apidb.orthomcl.load.plugin.biolayout.Edge edge : group.edges.values()) {
            if (edge.type != type) continue;
            Node query = group.nodes.get(edge.queryId);
            Node subject = group.nodes.get(edge.subjectId);
            writer.print("<line class=\"edge\"");
            writer.print("  x1=\"" + query.x + "\" y1=\"" + query.y + "\"");
            writer.print("  x2=\"" + subject.x + "\" y2=\"" + subject.y + "\"");
            writer.print("  query=\"" + edge.queryId + "\"");
            writer.print("  subject=\"" + edge.subjectId + "\"");
            writer.print("  evalue=\"" + edge.evalueMant + "E");
            writer.println(edge.evalueExp + "\" />");
        }
        writer.println("</g>");
    }

    private void scaleGraph(Group group) {
        // get minimal rect that covers all nodes
        Iterator<?> it = graph.getGraphNodeSet().iterator();
        if (!it.hasNext()) return;
        GraphNode gNode = (GraphNode) it.next();
        double minx = gNode.getX();
        double maxx = gNode.getX();
        double miny = gNode.getY();
        double maxy = gNode.getY();
        while (it.hasNext()) {
            gNode = (GraphNode) it.next();
            double x = gNode.getX();
            double y = gNode.getY();
            if (minx > x) minx = x;
            else if (maxx < x) maxx = x;
            if (miny > y) miny = y;
            else if (maxy < y) maxy = y;
        }

        // compute the scale factor
        final int margin = 20;
        final int topMargin = 80;
        double scalex = (WIDTH - 2D * margin) / (maxx - minx);
        double scaley = (HEIGHT - 3D * margin) / (maxy - miny);

        // scale the nodes
        for (Object obj : graph.getGraphNodeSet()) {
            gNode = (GraphNode) obj;
            int x = (int) Math.round((gNode.getX() - minx) * scalex + margin);
            int y = (int) Math.round((gNode.getY() - miny) * scaley + margin);
            gNode.setLocation((int) x, (int) y);
            gNode.m_nodeSize = 2 * RADIUS;
            int seqId = Integer.parseInt(gNode.getNodeName());
            Node node = group.nodes.get(seqId);
            node.x = x;
            node.y = y + topMargin;
        }
    }

    private void saveImage(OutputStream stream) throws IOException {
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT,
                BufferedImage.TYPE_INT_ARGB);
        Graphics graphics = image.getGraphics();
        graphics.setColor(new Color(222, 222, 222));
        graphics.fillRect(0, 0, WIDTH, HEIGHT);
        graph.updateBackImage(graphics);
        ImageIO.write(image, "PNG", stream);
        stream.flush();
    }
}
