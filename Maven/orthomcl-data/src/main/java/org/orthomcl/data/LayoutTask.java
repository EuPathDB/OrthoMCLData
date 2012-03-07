package org.orthomcl.data;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.zip.Deflater;

import net.lliira.common.graphics.GraphicsException;
import net.lliira.common.graphics.layout.Graph;
import net.lliira.common.graphics.layout.LayoutObserver;
import net.lliira.common.graphics.layout.SpringLayout;
import net.lliira.common.graphics.layout.Vector;

import org.apache.log4j.Logger;
import org.orthomcl.common.BlastScore;
import org.orthomcl.common.EdgeType;
import org.orthomcl.common.Gene;
import org.orthomcl.common.Group;

public class LayoutTask implements Runnable, LayoutObserver {

    private static final Logger logger = Logger.getLogger(LayoutTask.class);

    private static long count = 0;

    private final EdgeFactory edgeFactory;
    private final Queue<Group> groups;
    private final Random random;
    private final int maxWeight;

    private Group group;
    private boolean stop;

    // private long start;

    public LayoutTask(Queue<Group> groups, Connection connection, int maxWeight)
            throws SQLException {
        this.edgeFactory = new EdgeFactory(connection);
        this.groups = groups;
        this.random = new Random();
        this.maxWeight = maxWeight;
    }

    public void stop() {
        stop = true;
    }

    @Override
    public void run() {
        stop = false;
        try {
            while (!stop) {
                group = groups.poll();
                if (group == null) {
                    try {
                        Thread.sleep(100 + random.nextInt(100));
                    } catch (InterruptedException ex) {}
                } else {
                    processGroup();
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                edgeFactory.close();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void processGroup() throws SQLException, GraphicsException,
            IOException {
        // load group edges
        // start = System.currentTimeMillis();
        edgeFactory.loadBlastScores(group, maxWeight);
        edgeFactory.loadEdgeTypes(group, EdgeType.Ortholog);
        edgeFactory.loadEdgeTypes(group, EdgeType.Coortholog);
        edgeFactory.loadEdgeTypes(group, EdgeType.Inparalog);
        // logger.debug("Group#" + group.getId() + " loaded in "
        // + ((System.currentTimeMillis() - start) / 1000D) + " seconds");

        // start = System.currentTimeMillis();
        if (group.getGenes().size() == 2) {
            // if there is only 2 nodes, no need to run layout.
            double weight = maxWeight;
            if (group.getScores().size() > 0)
                weight = group.getScores().values().iterator().next().getWeight();
            Iterator<Gene> genes = group.getGenes().values().iterator();
            double x = random.nextBoolean() ? 0 : weight;
            double y = random.nextBoolean() ? 0 : weight;
            genes.next().getPoint().setLocation(Math.abs(weight - x), y);
            genes.next().getPoint().setLocation(x, Math.abs(weight - y));
            saveGroup();
        } else {// more than 2 nodes, compute layout
            // initialize the locations of the nodes
            initializeGroup();
            SpringLayout layout = new SpringLayout(group);
            layout.process(this);
        }
    }

    private void initializeGroup() {
        for (Gene gene : group.getGenes().values()) {
            double x = random.nextDouble() * maxWeight;
            double y = random.nextDouble() * maxWeight;
            gene.getPoint().setLocation(x, y);
        }
    }

    private void saveGroup() throws SQLException, IOException {
        // serialize & compress group
        byte[] data = serializeGroup(group);
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();

        ByteArrayOutputStream output = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!compressor.finished()) {
            int count = compressor.deflate(buffer);
            output.write(buffer, 0, count);
        }
        output.close();

        edgeFactory.saveGroup(group, output.toByteArray());
        count++;
        if (count % 100 == 0) logger.debug(count + " groups saved.");
    }

    private byte[] serializeGroup(Group group) throws IOException {
        int size = 8 + group.getGenes().size() * 12 + group.getScores().size()
                * 19;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(size);
        DataOutputStream output = new DataOutputStream(buffer);
        // export node count & edge count,
        output.writeInt(group.getGenes().size()); // 0x0000, 4B
        output.writeInt(group.getEdges().size()); // 0x0004, 4B

        // export genes - 0x0008+, 12B/gene
        for (Gene gene : group.getGenes().values()) {
            Vector point = gene.getPoint();
            output.writeInt(gene.getId()); // +0x0000, 4B
            output.writeFloat((float) point.x); // +0x0004, 4B
            output.writeFloat((float) point.y); // +0x0008, 4B
        }

        // export blast scores - 19B/edge
        for (BlastScore score : group.getScores().values()) {
            output.writeInt(score.getQueryId()); // +0x0000, 4B
            output.writeInt(score.getSubjectId()); // +0x0004, 4B
            output.writeByte(score.getType().ordinal()); // +0x0008, 1B
            output.writeFloat(score.getEvalueMant()); // +0x0009, 4B
            output.writeShort(score.getEvalueExp()); // +0x000D 2B
            output.writeFloat((float)score.getWeight());// +0x000F, 4B
        }
        output.flush();
        output.close();
        return buffer.toByteArray();
    }

    @Override
    public void finish(Graph arg0, int arg1, double arg2, Vector arg3) {
        // logger.debug("Group#" + group.getId() + " processed in "
        // + ((System.currentTimeMillis() - start) / 1000D) + " seconds");
        // start = System.currentTimeMillis();
        try {
            saveGroup();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        // logger.debug("Group#" + group.getId() + " saved in "
        // + ((System.currentTimeMillis() - start) / 1000D) + " seconds");
    }

    @Override
    public void step(Graph arg0, int arg1, double arg2, Vector arg3) {}
}
