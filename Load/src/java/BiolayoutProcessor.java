import java.awt.Color;
import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;
import org.apidb.orthomcl.load.plugin.OrthoMCLException;
import org.apidb.orthomcl.load.plugin.GenerateBioLayoutPlugin.EdgeType;
import org.apidb.orthomcl.load.plugin.GenerateBioLayoutPlugin.OrthomclEdge;
import org.apidb.orthomcl.load.plugin.GenerateBioLayoutPlugin.Sequence;
import org.apidb.orthomcl.load.plugin.GenerateBioLayoutPlugin.Taxon;

/**
 * 
 */

/**
 * @author xingao
 * This class has to be put under default package, in order to access BioLayout
 * classes.
 */
public class BiolayoutProcessor {

    private class HiddenFrame extends LayoutFrame {

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

    //private static final Logger logger = Logger.getLogger(BiolayoutProcessor.class);

    private LayoutClasses classes;
    private NetworkContainer network;
    private Graph graph;

    public BiolayoutProcessor() throws SecurityException, NoSuchFieldException,
            IllegalArgumentException, IllegalAccessException {
        LayoutFrame frame = new HiddenFrame();
        classes = frame.getClasses();
        graph = frame.getGraph();
        graph.setPreferredSize(new Dimension(1000, 1000));

        Field field = LayoutFrame.class.getDeclaredField("m_nc");
        field.setAccessible(true);
        network = (NetworkContainer) field.get(frame);

        GlobalEnv.DIRECTIONAL = false;
        GlobalEnv.BACKGROUND_COLOR = new Color(222, 222, 222);
        GlobalEnv.BACKGROUND_ANTIALIASING = true;
    }

    public void saveData(Map<Integer, Taxon> taxons,
            Map<Integer, Sequence> sequences,
            Map<OrthomclEdge, OrthomclEdge> edges, String[] svgTemplate,
            OutputStream svgStream, OutputStream imgStream) throws IOException,
            OrthoMCLException {
        // reset the network
        network.clean();
        GlobalEnv.WEIGHTED = true;

        // parse the data into network
        parseData(taxons, sequences, edges);

        network.optimize();
        graph.updateGraphics(network);
        scaleGraph();

        // save the layout file
        saveSVG(svgTemplate, taxons, sequences, edges, svgStream);
        svgStream.flush();

        // save layout image
        saveImage(imgStream);
        imgStream.flush();
    }

    private void parseData(Map<Integer, Taxon> taxons,
            Map<Integer, Sequence> sequences,
            Map<OrthomclEdge, OrthomclEdge> edges) throws OrthoMCLException {
        // create nodes and edges
        for (OrthomclEdge edge : edges.keySet()) {
            String queryId = Integer.toString(edge.QueryId).intern();
            String subjectId = Integer.toString(edge.SubjectId).intern();
            network.addNetworkConnection(queryId, subjectId, edge.Weight);
        }

        // assign sequence-taxons
        for (int sequenceId : sequences.keySet()) {
            Taxon taxon = taxons.get(sequences.get(sequenceId).TaxonId);
            String nodeName = Integer.toString(sequenceId).intern();
            Vertex vertex = (Vertex) network.m_hashMap.get(nodeName);

            if (vertex != null) {
                // update class ref in the vertex, and create class if needed
                classes.updateClass(vertex, taxon.Id, taxon.Abbreviation);
            } else {
                System.err.println("Sequence: "
                        + sequenceId
                        + " doesn't have any edge to other nodes in the same group");
            }

        }
    }

    private void saveSVG(String[] svgTemplate, Map<Integer, Taxon> taxons,
            Map<Integer, Sequence> sequences,
            Map<OrthomclEdge, OrthomclEdge> edges, OutputStream svgStream) {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(svgStream));

        // output header
        int width = graph.getBackPanel().getWidth();
        int height = graph.getBackPanel().getHeight();
        String header = svgTemplate[0].replace("$$CanvasWidth$$",
                Integer.toString(width));
        writer.println(header.replace("$$CanvasHeight$$",
                Integer.toString(height)));

        // output node array
        writeNodes(svgTemplate, taxons, sequences, edges, writer);

        // output edges
        writeEdges(edges, writer);
        writer.flush();

        // output the middle sectoin
        writer.println(svgTemplate[1]);
        writer.flush();

        // draw the nodes, grouped by taxons
        for (Taxon taxon : taxons.values()) {
            VertexClass vertexClass = classes.getClassByID(taxon.Id);
            if (vertexClass == null) continue;

            // get the color for this taxon
            Color color = vertexClass.m_classColor;
            String strColor = String.format("%1$06x",
                    (color.getRGB() & 0xFFFFFF));

            writer.print("<g id=\"node_genome" + taxon.Id);
            writer.println("\" style=\"stroke:black;fill:#" + strColor + "\">");

            // draw the sequences in the taxon
            for (Object obj : graph.getGraphNodeSet()) {
                GraphNode node = (GraphNode) obj;
                int sequenceId = Integer.parseInt(node.getNodeName());
                Sequence sequence = sequences.get(sequenceId);
                if (sequence.TaxonId != taxon.Id) continue;

                String nodeName = taxon.Abbreviation + sequenceId;

                writer.print("\t<circle id=\"" + nodeName);
                writer.print("\" cx=\"" + node.getX());
                writer.print("\" cy=\"" + node.getY());
                writer.print("\" r=\"5\" onmouseover=\"highlightNode(evt)\"");
                writer.println(" onmouseout=\"unhighlightNode(evt)\"/>");
            }
            writer.println("</g>");
        }

        // draw the footer
        int controlX = width - 300;
        String footer = svgTemplate[2].replace("$$ControlLeft$$",
                Integer.toString(controlX));
        footer = footer.replace("$$TextControlLeft$$",
                Integer.toString(controlX + 10));
        footer = footer.replace("$$SpanLeft$$", Integer.toString(controlX + 20));
        writer.println(footer.replace("$$RectangleWidth$$",
                Integer.toString(controlX)));
        writer.flush();
    }

    private void writeNodes(String[] svgTemplate, Map<Integer, Taxon> taxons,
            Map<Integer, Sequence> sequences,
            Map<OrthomclEdge, OrthomclEdge> edges, PrintWriter writer) {
        Iterator<?> itVertex = graph.getGraphNodeSet().iterator();
        while (itVertex.hasNext()) {
            GraphNode vertex = (GraphNode) itVertex.next();
            int sequenceId = Integer.parseInt(vertex.getNodeName());
            Sequence sequence = sequences.get(sequenceId);
            Taxon taxon = taxons.get(sequence.TaxonId);
            String nodeName = taxon.Abbreviation + sequenceId;

            writer.print("nodeArray.push(new node('" + nodeName + "','");
            writer.print(sequence.SourceId + "','");
            writer.print((sequence.Description != null ? 
                    sequence.Description : sequence.SourceId) + "','");
            writer.print(taxon.Name + "',['");

            // output all sequences in the same taxon
            boolean isFirstNode = true;
            for (Sequence seq : sequences.values()) {
                if (seq.TaxonId != taxon.Id) continue;
                if (isFirstNode) isFirstNode = false;
                else writer.print("', '");
                String nName = taxons.get(seq.TaxonId).Abbreviation
                        + seq.SequenceId;
                writer.print(nName);
            }
            writer.print("'], [");

            // output all indices of edges linked to the node
            boolean isFirstEdge = true;
            for (OrthomclEdge edge : edges.keySet()) {
                if (edge.QueryId == sequenceId || edge.SubjectId == sequenceId) {
                    if (isFirstEdge) isFirstEdge = false;
                    else writer.print(", ");
                    writer.print(edge.EdgeId);
                }
            }
            writer.println("]));");
        }
    }

    private void writeEdges(Map<OrthomclEdge, OrthomclEdge> edges,
            PrintWriter writer) {
        for (Object obj : graph.getGraphEdges()) {
            GraphEdge gEdge = (GraphEdge) obj;
            GraphNode firstNode = gEdge.getNodeFirst();
            GraphNode secondNode = gEdge.getNodeSecond();

            int queryId = Integer.parseInt(firstNode.getNodeName());
            int subjectId = Integer.parseInt(secondNode.getNodeName());
            OrthomclEdge oEdge = edges.get(new OrthomclEdge(queryId, subjectId));

            if (oEdge.Type == EdgeType.Ortholog || oEdge.Type == EdgeType.Coortholog) {
                writer.print("edgeRbesthArray.push(");
            } else if (oEdge.Type == EdgeType.Inparalog) {
                writer.print("edgeRbetterhArray.push(");
            } else {
                writer.print("edgeGeneralArray.push(");
            }
            writer.print("new edge(" + oEdge.EdgeId);
            writer.print(", [" + firstNode.getX());
            writer.print(", " + firstNode.getY());
            writer.print(", " + secondNode.getX());
            writer.print(", " + secondNode.getY() + "], '");
            if (oEdge.PValueMant == 0) {
                writer.print(oEdge.PValueMant);
            } else {
                writer.print(oEdge.PValueMant + "e" + oEdge.PValueExp);
            }
            writer.println("'));");
        }
    }

    private void scaleGraph() {
        // get minimal rect that covers all nodes
        Iterator<?> it = graph.getGraphNodeSet().iterator();
        if (!it.hasNext()) return;
        GraphNode node = (GraphNode) it.next();
        double minx = node.getX();
        double maxx = node.getX();
        double miny = node.getY();
        double maxy = node.getY();
        while (it.hasNext()) {
            node = (GraphNode) it.next();
            double x = node.getX();
            double y = node.getY();
            if (minx > x) minx = x;
            else if (maxx < x) maxx = x;
            if (miny > y) miny = y;
            else if (maxy < y) maxy = y;
        }

        // compute the scale factor
        final double margin = 20;
        final double topMargin = 90;
        double scalex = (graph.getBackPanel().getWidth() - 2 * margin)
                / (maxx - minx);
        double scaley = (graph.getBackPanel().getHeight() - topMargin - margin)
                / (maxy - miny);

        // scale the nodes
        for (Object obj : graph.getGraphNodeSet()) {
            node = (GraphNode) obj;
            double x = (node.getX() - minx) * scalex + margin;
            double y = (node.getY() - miny) * scaley + topMargin;
            node.setLocation((int) Math.round(x), (int) Math.round(y));
            node.m_nodeSize = 10D;
        }
    }

    private void saveImage(OutputStream stream) throws IOException {
        int width = graph.getBackPanel().getWidth();
        int height = graph.getBackPanel().getHeight();
        BufferedImage image = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        graph.updateBackImage(image.getGraphics());
        ImageIO.write(image, "PNG", stream);
    }
}
